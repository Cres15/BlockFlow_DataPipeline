"""
BlockFlow ETL - Transform stage (Stage 2: Data Transformation)

Takes the three DataFrames handed over by an APPROVED extraction run and turns them
into clean, correctly typed, load-ready tables:

  1. text cleanup        4. impossible values removed
  2. type conversion     5. duplicates dropped
  3. null handling       then: total_amount = quantity x unit_price (Sales)

It then runs the nine transformation checks, decides APPROVED / MANUAL REVIEW /
REJECTED, writes the run log, the audit summary and the rejected rows, and hands the
result to the Load stage.

Run through the whole pipeline:   python run_pipeline.py
Approve an extraction held for review:   python transform.py --staged run_20261026_000000

Note for the Load stage: DECIMAL columns hold Python Decimal values (exact 2-decimal
money) and DATETIME columns hold 'YYYY-MM-DD HH:MM:SS' text. Register an adapter before
inserting, e.g.  sqlite3.register_adapter(Decimal, str)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

import pandas as pd

os.environ.setdefault("PREFECT_LOGGING_EXTRA_LOGGERS", "blockflow")

from prefect import flow, task  # noqa: E402
from prefect.cache_policies import NO_CACHE  # noqa: E402

from extract import (  # noqa: E402
    AUDIT_DIR,
    FAIL,
    LOGS_DIR,
    MAX_RETRIES,
    PASS,
    RETRY_DELAY_SECONDS,
    ROW_REJECTED_DIR,
    STAGING_DIR,
    TIMEOUT_SECONDS,
    WARNING,
)

# --------------------------------------------------------------------------- #
# Configuration (Section 3, 4, 6 and 7 of the Stage 2 documentation)
# --------------------------------------------------------------------------- #
REJECTION_RATE_LIMIT = 0.05  # check 8: WARNING above 5% of a table's input rows
TWO_PLACES = Decimal("0.01")
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
TEXT_KINDS = {"code_upper", "code_lower", "title", "phone", "text"}
NULL_TEXT = {"", "nan", "none", "null"}

# columns: target kind per business column.  required: a null removes the row.
# fill: a null is replaced by a placeholder.  positive: must be above zero.
# order: output column order (the primary key, when the source carries one, goes first).
TABLES: dict[str, dict] = {
    "stocks": {
        "pk": "stock_id",
        "columns": {"size": "code_upper", "quantity": "int", "unit": "code_lower",
                    "unit_price": "dec", "updated_at": "dt", "recorded_by": "int"},
        "required": ["size", "quantity", "unit", "unit_price", "updated_at", "recorded_by"],
        "fill": {},
        "positive": ["quantity", "unit_price"],
        "order": ["size", "quantity", "unit", "unit_price", "updated_at", "recorded_by"],
    },
    "sales": {
        "pk": "sale_id",
        "columns": {"customer_name": "title", "phone_number": "phone", "shop_name": "title",
                    "quantity": "int", "unit_price": "dec", "sale_date": "dt",
                    "recorded_by": "int", "stock_id": "int"},
        "required": ["quantity", "unit_price", "sale_date", "recorded_by", "stock_id"],
        "fill": {"customer_name": "Unknown", "shop_name": "Unknown", "phone_number": "Unknown"},
        "positive": ["quantity", "unit_price"],
        "order": ["customer_name", "phone_number", "shop_name", "quantity", "unit_price",
                  "total_amount", "sale_date", "recorded_by", "stock_id"],
    },
    "expenses": {
        "pk": "expense_id",
        "columns": {"category": "title", "description": "text", "amount": "dec",
                    "expense_date": "dt", "recorded_by": "int"},
        "required": ["category", "amount", "expense_date", "recorded_by"],
        "fill": {"description": "No description"},
        "positive": ["amount"],
        "order": ["category", "description", "amount", "expense_date", "recorded_by"],
    },
}

log = logging.getLogger("blockflow.transform")


# --------------------------------------------------------------------------- #
# Value converters (Section 4)
# --------------------------------------------------------------------------- #
def _clean_text(value, collapse: bool = True) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) and pd.isna(value):
        return None
    text = str(value).strip()
    if collapse:
        text = re.sub(r"\s+", " ", text)
    return None if text.lower() in NULL_TEXT else text


def _text(value, kind: str) -> str | None:
    text = _clean_text(value, collapse=(kind != "text"))
    if text is None:
        return None
    if kind == "code_upper":
        return text.upper()
    if kind == "code_lower":
        return text.lower()
    if kind == "title":
        return text.title()
    if kind == "phone":
        return re.sub(r"\D", "", text) or None
    return text


def _to_int(text: str) -> int | None:
    try:
        number = Decimal(text.replace(",", "").replace(" ", ""))
    except InvalidOperation:
        return None
    if not number.is_finite() or number != number.to_integral_value():
        return None
    return int(number)


_CURRENCY_RE = re.compile(r"(?i)php|₱|pesos?")


def _to_decimal(text: str) -> Decimal | None:
    cleaned = _CURRENCY_RE.sub("", text).replace(",", "").replace(" ", "")
    try:
        number = Decimal(cleaned)
        if not number.is_finite():
            return None
        return number.quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return None


def _to_datetime_str(text: str) -> str | None:
    try:
        stamp = pd.to_datetime(text, errors="coerce")
    except (ValueError, TypeError, OverflowError):
        return None
    if pd.isna(stamp):
        return None
    if getattr(stamp, "tzinfo", None) is not None:
        stamp = stamp.tz_localize(None)
    return stamp.strftime(DATETIME_FORMAT)


CONVERTERS = {"int": _to_int, "dec": _to_decimal, "dt": _to_datetime_str}


# --------------------------------------------------------------------------- #
# Cleaning one table (Section 3, 4 and 5)
# --------------------------------------------------------------------------- #
def clean_table(domain: str, df: pd.DataFrame) -> dict:
    """Clean one table. Returns the output frame, the removed rows and the counts."""
    spec = TABLES[domain]
    pk = spec["pk"]
    has_pk = pk in df.columns
    kinds = dict(spec["columns"])
    if has_pk:
        kinds = {pk: "int", **kinds}
    work_cols = list(kinds)
    original = df.reset_index(drop=True)
    n_in = len(original)
    dropped_columns = [c for c in original.columns if c not in work_cols]

    # Steps 1 and 2: text cleanup and type conversion
    data: dict[str, list] = {}
    blank: dict[str, list[bool]] = {}
    for col in work_cols:
        kind = kinds[col]
        raw = original[col].tolist()
        if kind in TEXT_KINDS:
            values = [_text(v, kind) for v in raw]
            blank[col] = [v is None for v in values]
        else:
            cleaned = [_clean_text(v) for v in raw]
            blank[col] = [v is None for v in cleaned]
            convert = CONVERTERS[kind]
            values = [None if v is None else convert(v) for v in cleaned]
        data[col] = values

    reasons: list[list[str]] = [[] for _ in range(n_in)]

    # Step 3: null handling (remove the row, or fill with a placeholder)
    required = list(spec["required"]) + ([pk] if has_pk else [])
    for col in required:
        for i, value in enumerate(data[col]):
            if value is None:
                reasons[i].append(f"{'null' if blank[col][i] else 'invalid'} {col}")
    filled_per_row = [0] * n_in
    for col, placeholder in spec["fill"].items():
        for i, value in enumerate(data[col]):
            if value is None:
                data[col][i] = placeholder
                filled_per_row[i] += 1

    # Step 4: impossible values
    for col in spec["positive"]:
        for i, value in enumerate(data[col]):
            if value is not None and value <= 0:
                reasons[i].append(f"{col} at or below 0")

    # Step 5: duplicates on every business column, keeping the first occurrence
    business = list(spec["columns"])
    seen: set = set()
    duplicates: set[int] = set()
    for i in range(n_in):
        if reasons[i]:
            continue
        key = tuple(data[col][i] for col in business)
        if key in seen:
            duplicates.add(i)
            reasons[i].append("duplicate of an earlier record")
        else:
            seen.add(key)

    keep = [i for i in range(n_in) if not reasons[i]]
    removed_idx = [i for i in range(n_in) if reasons[i]]

    # Output frame (contract order, correct types)
    out: dict[str, pd.Series] = {}
    for col in work_cols:
        picked = [data[col][i] for i in keep]
        if kinds[col] == "int":
            out[col] = pd.Series(pd.array(picked, dtype="Int64"))
        else:
            out[col] = pd.Series(picked, dtype="object")
    # Business rule (Section 5): total_amount = quantity x unit_price
    if domain == "sales":
        out["total_amount"] = pd.Series(
            [(Decimal(data["quantity"][i]) * data["unit_price"][i]).quantize(
                TWO_PLACES, rounding=ROUND_HALF_UP) for i in keep],
            dtype="object",
        )
    order = ([pk] if has_pk else []) + spec["order"]
    clean = pd.DataFrame(out)[order].reset_index(drop=True)

    # Removed rows keep their original values plus the reason
    removed = original.iloc[removed_idx].copy()
    removed["rejection_reason"] = ["; ".join(reasons[i]) for i in removed_idx]

    return {
        "df": clean,
        "removed": removed,
        "has_pk": has_pk,
        "stats": {
            "rows_in": n_in,
            "rows_out": len(keep),
            "removed": len(removed_idx) - len(duplicates),
            "duplicates_dropped": len(duplicates),
            "values_filled": sum(filled_per_row[i] for i in keep),
            "columns_dropped": dropped_columns,
        },
    }


@task(name="transform-tables", cache_policy=NO_CACHE, retries=MAX_RETRIES,
      retry_delay_seconds=RETRY_DELAY_SECONDS, timeout_seconds=TIMEOUT_SECONDS)
def transform_tables(frames: dict) -> dict:
    """Prefect task: clean every table. Data problems never raise, only real faults do."""
    return {domain: clean_table(domain, frames[domain]) for domain in TABLES}


# --------------------------------------------------------------------------- #
# Validation checks (Section 7)
# --------------------------------------------------------------------------- #
def check(name: str, status: str, detail: str = "", domain: str | None = None) -> dict:
    result = {"check": name, "domain": domain, "status": status, "detail": detail}
    level = {PASS: logging.INFO, WARNING: logging.WARNING, FAIL: logging.ERROR}[status]
    log.log(level, "[%s] %s%s %s", status, name, f" ({domain})" if domain else "", detail)
    return result


def input_problems(extraction: dict, approved_after_review: bool) -> list[str]:
    """Check 1: Input Handover."""
    problems = []
    status = extraction.get("status")
    if not (status == "APPROVED" or (approved_after_review and status == "MANUAL REVIEW")):
        problems.append(f"extraction status is {status!r}, not APPROVED")
    frames = extraction.get("dataframes") or {}
    for domain, spec in TABLES.items():
        frame = frames.get(domain)
        if not isinstance(frame, pd.DataFrame):
            problems.append(f"{domain} DataFrame missing")
            continue
        missing = [c for c in spec["columns"] if c not in frame.columns]
        if missing:
            problems.append(f"{domain} missing columns {missing}")
    metadata = extraction.get("metadata")
    if not isinstance(metadata, dict):
        problems.append("run metadata missing")
    elif not metadata.get("validation_results"):
        problems.append("validation summary missing")
    return problems


def schema_problems(domain: str, df: pd.DataFrame, has_pk: bool) -> list[str]:
    """Check 2: Schema Match (columns and types)."""
    spec = TABLES[domain]
    expected = ([spec["pk"]] if has_pk else []) + spec["order"]
    problems = []
    missing = [c for c in expected if c not in df.columns]
    extra = [c for c in df.columns if c not in expected]
    if missing:
        problems.append(f"missing columns {missing}")
    if extra:
        problems.append(f"extra columns {extra}")
    kinds = {**spec["columns"], "total_amount": "dec", spec["pk"]: "int"}
    for col in df.columns:
        if col not in kinds:
            continue
        kind, values = kinds[col], df[col].tolist()
        if kind == "int":
            ok = str(df[col].dtype) == "Int64"
        elif kind == "dec":
            ok = all(isinstance(v, Decimal) and v.as_tuple().exponent == -2 for v in values)
        elif kind == "dt":
            ok = all(isinstance(v, str) and DATETIME_RE.match(v) for v in values)
        else:
            ok = all(isinstance(v, str) for v in values)
        if not ok:
            problems.append(f"{col} is not a valid {kind}")
    return problems


def validate_table(domain: str, result: dict) -> list[dict]:
    """Checks 2 to 8 for one transformed table."""
    df, stats, has_pk = result["df"], result["stats"], result["has_pk"]
    spec = TABLES[domain]
    checks = []

    problems = schema_problems(domain, df, has_pk)
    checks.append(check("Schema Match", FAIL if problems else PASS, "; ".join(problems), domain))

    nulls = int(df.isna().sum().sum())
    checks.append(check("Required Values Present", FAIL if nulls else PASS,
                        f"{nulls} null values" if nulls else "", domain))

    dup_rows = int(df.duplicated(subset=list(spec["columns"])).sum())
    dup_keys = int(df[spec["pk"]].duplicated().sum()) if has_pk else 0
    checks.append(check("Duplicate Records", FAIL if (dup_rows or dup_keys) else PASS,
                        f"{dup_rows} duplicate records, {dup_keys} repeated keys"
                        if (dup_rows or dup_keys) else "", domain))

    if domain == "sales":
        wrong = sum(
            1 for q, p, t in zip(df["quantity"].tolist(), df["unit_price"].tolist(),
                                 df["total_amount"].tolist())
            if t != (Decimal(q) * p).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
        )
        checks.append(check("Total Amount Accuracy", FAIL if wrong else PASS,
                            f"{wrong} rows differ from quantity x unit_price" if wrong else "",
                            domain))

    accounted = stats["rows_out"] + stats["removed"] + stats["duplicates_dropped"]
    checks.append(check("Record Reconciliation", PASS if accounted == stats["rows_in"] else FAIL,
                        f"in={stats['rows_in']} out={stats['rows_out']} "
                        f"removed={stats['removed']} duplicates={stats['duplicates_dropped']}",
                        domain))

    checks.append(check("Domain Completeness", PASS if stats["rows_out"] > 0 else FAIL,
                        f"{stats['rows_out']} rows after cleaning", domain))

    lost = stats["removed"] + stats["duplicates_dropped"]
    rate = lost / stats["rows_in"] if stats["rows_in"] else 0.0
    checks.append(check("Rejection Rate", WARNING if rate > REJECTION_RATE_LIMIT else PASS,
                        f"{lost} of {stats['rows_in']} rows removed ({rate:.1%})", domain))
    return checks


# --------------------------------------------------------------------------- #
# Run one transformation (called inside the Prefect flow)
# --------------------------------------------------------------------------- #
def run_transform(extraction: dict, approved_after_review: bool = False) -> dict:
    started = datetime.now()
    t0 = time.monotonic()
    run_id = f"transform_{started:%Y%m%d_%H%M%S}"
    metadata_in = extraction.get("metadata") if isinstance(extraction.get("metadata"), dict) else {}

    for directory in (LOGS_DIR, AUDIT_DIR):
        directory.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(LOGS_DIR / f"{run_id}.log", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.setLevel(logging.INFO)
    log.addHandler(handler)
    log.info("Starting %s for extraction run %s", run_id, metadata_in.get("pipeline_run_id"))

    checks: list[dict] = []
    tables: dict[str, dict] = {}
    error_message = "NONE"

    # Check 1: Input Handover
    problems = input_problems(extraction, approved_after_review)
    if problems:
        checks.append(check("Input Handover", FAIL, "; ".join(problems)))
        error_message = "INPUT_HANDOVER_FAILED"
    else:
        checks.append(check("Input Handover", PASS, "3 DataFrames, run metadata, validation summary"))
        try:
            tables = transform_tables(extraction["dataframes"])
        except Exception as exc:  # task failed after its retries, or timed out
            checks.append(check("Transformation Task", FAIL, str(exc)))
            error_message = f"TASK_FAILED:{type(exc).__name__}"
        else:
            for domain, result in tables.items():
                dropped = result["stats"]["columns_dropped"]
                if dropped:
                    log.info("Dropped columns outside the data contract from %s: %s", domain, dropped)
                checks.extend(validate_table(domain, result))

    # Check 9: Execution Completion within Time Limit
    elapsed = time.monotonic() - t0
    if elapsed > TIMEOUT_SECONDS:
        checks.append(check("Execution Completion", FAIL, f"{elapsed:.0f}s > {TIMEOUT_SECONDS}s"))
    else:
        checks.append(check("Execution Completion", PASS, f"{elapsed:.1f}s"))

    # Acceptance decision (Section 10)
    statuses = {c["status"] for c in checks}
    if FAIL in statuses:
        status = "REJECTED"
        if error_message == "NONE":
            first = next(c for c in checks if c["status"] == FAIL)
            error_message = first["check"].upper().replace(" ", "_")
    elif WARNING in statuses:
        status = "MANUAL REVIEW"
        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        for domain, result in tables.items():
            result["df"].to_csv(STAGING_DIR / f"{run_id}_{domain}.csv", index=False, encoding="utf-8")
        log.warning("Held for Owner review. Transformed data staged in %s", STAGING_DIR)
    else:
        status = "APPROVED"

    # Rejected rows (kept with their reason, whatever the decision)
    for domain, result in tables.items():
        if len(result["removed"]):
            ROW_REJECTED_DIR.mkdir(parents=True, exist_ok=True)
            path = ROW_REJECTED_DIR / f"{run_id}_{domain}_rejected.csv"
            result["removed"].to_csv(path, index=False, encoding="utf-8")
            log.warning("Wrote %d rejected %s rows to %s", len(result["removed"]), domain, path)

    summary = {}
    for domain in TABLES:
        domain_status = {c["status"] for c in checks if c["domain"] == domain}
        summary[f"{domain}_table_validation"] = (
            FAIL if FAIL in domain_status else WARNING if WARNING in domain_status
            else PASS if domain_status else "NOT RUN")
    summary["users_table_validation"] = "Not applicable (excluded from the pipeline)"
    summary["overall_transformation"] = status

    def per_table(key: str) -> dict:
        return {d: r["stats"][key] for d, r in tables.items()}

    finished = datetime.now()
    audit = {
        "transform_run_id": run_id,
        "pipeline_run_id": metadata_in.get("pipeline_run_id"),
        "extraction_window": metadata_in.get("extraction_window"),
        "approved_after_review": approved_after_review,
        "transformation_start": started.isoformat(sep=" ", timespec="seconds"),
        "transformation_end": finished.isoformat(sep=" ", timespec="seconds"),
        "status": status,
        "records_received": per_table("rows_in"),
        "records_transformed": per_table("rows_out"),
        "records_removed": per_table("removed"),
        "duplicates_dropped": per_table("duplicates_dropped"),
        "values_filled": per_table("values_filled"),
        "validation_results": checks,
        "validation_summary": summary,
        "error_message": error_message if status == "REJECTED" else "NONE",
        "transformation_duration_seconds": round(elapsed, 1),
    }
    (AUDIT_DIR / f"{run_id}.json").write_text(
        json.dumps(audit, indent=2, default=str), encoding="utf-8")
    log.info("Run %s finished: %s in %.1fs", run_id, status, elapsed)
    log.removeHandler(handler)
    handler.close()

    # Handover to Load: only an APPROVED run passes its data on
    return {
        "status": status,
        "dataframes": {d: r["df"] for d, r in tables.items()} if status == "APPROVED" else {},
        "metadata": audit,
    }
    # Next stage (Load issue):  load(result["dataframes"], result["metadata"])


# --------------------------------------------------------------------------- #
# Approve an extraction that was held for MANUAL REVIEW
# --------------------------------------------------------------------------- #
def load_staged(extraction_run_id: str) -> dict:
    audit_path = AUDIT_DIR / f"{extraction_run_id}.json"
    if not audit_path.is_file():
        raise FileNotFoundError(f"No audit summary found: {audit_path}")
    metadata = json.loads(audit_path.read_text(encoding="utf-8"))
    if metadata.get("status") != "MANUAL REVIEW":
        raise ValueError(f"{extraction_run_id} is {metadata.get('status')}, not MANUAL REVIEW")
    frames = {}
    for domain in TABLES:
        path = STAGING_DIR / f"{extraction_run_id}_{domain}.csv"
        if not path.is_file():
            raise FileNotFoundError(f"Staged file missing: {path}")
        frames[domain] = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8")
    return {"status": metadata["status"], "dataframes": frames, "metadata": metadata}


@flow(name="blockflow-transform-staged", timeout_seconds=TIMEOUT_SECONDS)
def transform_staged_flow(extraction_run_id: str) -> dict:
    """Owner approval path: transform the data of an extraction held for MANUAL REVIEW."""
    return run_transform(load_staged(extraction_run_id), approved_after_review=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BlockFlow transformation")
    parser.add_argument("--staged", metavar="EXTRACTION_RUN_ID",
                        help="approve and transform an extraction held for MANUAL REVIEW")
    args = parser.parse_args()
    if not args.staged:
        parser.error("use run_pipeline.py for a normal run, or --staged RUN_ID to approve a held extraction")
    try:
        outcome = transform_staged_flow(args.staged)
    except (FileNotFoundError, ValueError) as exc:
        raise SystemExit(f"Cannot approve {args.staged}: {exc}")
    print(f"Transformation status: {outcome['status']}")
