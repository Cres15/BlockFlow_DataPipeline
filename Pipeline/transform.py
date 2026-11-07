"""
BlockFlow ETL - Transform stage (Stage 2: Data Transformation), version 1.1.0

Takes the three DataFrames handed over by an APPROVED extraction run and turns them
into clean, correctly typed, load-ready tables. Every change is done by a numbered
rule (C = cleaning, S = standardization, B = business) so it can be counted, logged
and traced back to the documentation:

  1. text cleanup (C-01, C-02)      4. impossible values and orphans (C-06, C-08)
  2. type conversion (S-01..S-06)   5. duplicates (C-07)
  3. null handling (C-03..C-05)     then: total_amount = quantity x unit_price (B-01)

It then runs the validation checks (V-01..V-10), decides APPROVED / MANUAL REVIEW /
REJECTED, writes the run log, the audit summary and the quarantine (rejected rows),
and hands the result to the Load stage.

Run through the whole pipeline:        python run_pipeline.py
Approve an extraction held for review: python transform.py --staged run_20261026_000000
Accept Transform warnings (re-run):    python run_pipeline.py --date 2026-10-20 --accept-warnings

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
import sqlite3
import time
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path

import pandas as pd

os.environ.setdefault("PREFECT_LOGGING_EXTRA_LOGGERS", "blockflow")

from prefect import flow, task  # noqa: E402
from prefect.cache_policies import NO_CACHE  # noqa: E402

from extract import (  # noqa: E402
    AUDIT_DIR,
    BASE_DIR,
    FAIL,
    LOG_FORMAT,
    LOG_LEVEL,
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
# Configuration
# --------------------------------------------------------------------------- #
TRANSFORM_VERSION = "1.1.0"
SKIPPED = "SKIPPED"
# Rejection-rate threshold, fixed in advance (not tuned after seeing results).
REJECTION_RATE_LIMIT = 0.05
# Read-only lookup of parent keys (stocks.stock_id, users.user_id). Optional.
DB_PATH = Path(os.environ.get("BLOCKFLOW_DB", BASE_DIR / "blockflow.db"))

TWO_PLACES = Decimal("0.01")
MONEY_LIMIT = Decimal("1e10")  # DECIMAL(12,2): at most 10 digits before the point
DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
TEXT_KINDS = {"code_upper", "code_lower", "title", "phone", "text"}
NULL_TEXT = {"", "nan", "none", "null"}
S_RULE = {"dt": "S-01", "int": "S-02", "dec": "S-03"}
TEXT_RULE = {"code_upper": "S-04", "code_lower": "S-04", "title": "S-05", "phone": "S-06"}

RULES = {
    "C-01": "Trim and collapse whitespace",
    "C-02": "Treat blank or null-like text as missing",
    "C-03": "Fill missing sales contact fields with 'Unknown'",
    "C-04": "Fill missing expense description with 'No description'",
    "C-05": "Reject rows with a missing required value",
    "C-06": "Reject rows with quantity, price, or amount at or below 0",
    "C-07": "Reject duplicate business records (keep the first)",
    "C-08": "Reject orphan foreign keys",
    "S-01": "Standardize date-times to YYYY-MM-DD HH:MM:SS",
    "S-02": "Standardize whole numbers and identifiers",
    "S-03": "Standardize money to 2 decimal places",
    "S-04": "Standardize code casing (size upper, unit lower)",
    "S-05": "Standardize name casing (Title Case)",
    "S-06": "Standardize phone numbers to digits only",
    "B-01": "Derive total_amount = quantity x unit_price",
}

PROFILE_LABELS = {
    ("C-01", "fixed"): "Extra whitespace",
    ("C-02", "fixed"): "Missing value",
    ("S-01", "fixed"): "Non-standard date format",
    ("S-01", "rejected"): "Invalid date",
    ("S-02", "fixed"): "Non-standard number format",
    ("S-02", "rejected"): "Invalid whole number",
    ("S-03", "fixed"): "Non-standard money format",
    ("S-03", "rejected"): "Invalid amount",
    ("S-04", "fixed"): "Inconsistent code casing",
    ("S-05", "fixed"): "Inconsistent name casing",
    ("S-06", "fixed"): "Non-digit characters in phone number",
    ("C-06", "rejected"): "Value at or below 0",
    ("C-07", "rejected"): "Duplicate record",
    ("C-08", "rejected"): "Orphan foreign key",
}

# columns: target kind per business column.  required: a null rejects the row.
# fill: a null is replaced by (placeholder, rule).  positive: must be above zero.
# fks: column -> parent key set.  measure: column used for the aggregate check.
TABLES: dict[str, dict] = {
    "stocks": {
        "pk": "stock_id",
        "columns": {"size": "code_upper", "quantity": "int", "unit": "code_lower",
                    "unit_price": "dec", "updated_at": "dt", "recorded_by": "int"},
        "required": ["size", "quantity", "unit", "unit_price", "updated_at", "recorded_by"],
        "fill": {},
        "positive": ["quantity", "unit_price"],
        "fks": {"recorded_by": "user_ids"},
        "measure": "quantity",
        "order": ["size", "quantity", "unit", "unit_price", "updated_at", "recorded_by"],
    },
    "sales": {
        "pk": "sale_id",
        "columns": {"customer_name": "title", "phone_number": "phone", "shop_name": "title",
                    "quantity": "int", "unit_price": "dec", "sale_date": "dt",
                    "recorded_by": "int", "stock_id": "int"},
        "required": ["quantity", "unit_price", "sale_date", "recorded_by", "stock_id"],
        "fill": {"customer_name": ("Unknown", "C-03"), "shop_name": ("Unknown", "C-03"),
                 "phone_number": ("Unknown", "C-03")},
        "positive": ["quantity", "unit_price"],
        "fks": {"recorded_by": "user_ids", "stock_id": "stock_ids"},
        "measure": "quantity",
        "order": ["customer_name", "phone_number", "shop_name", "quantity", "unit_price",
                  "total_amount", "sale_date", "recorded_by", "stock_id"],
    },
    "expenses": {
        "pk": "expense_id",
        "columns": {"category": "title", "description": "text", "amount": "dec",
                    "expense_date": "dt", "recorded_by": "int"},
        "required": ["category", "amount", "expense_date", "recorded_by"],
        "fill": {"description": ("No description", "C-04")},
        "positive": ["amount"],
        "fks": {"recorded_by": "user_ids"},
        "measure": "amount",
        "order": ["category", "description", "amount", "expense_date", "recorded_by"],
    },
}

CHECK_IDS = {
    "Input Handover": "V-01",
    "Record Reconciliation": "V-02",
    "Required Values Not Null": "V-03",
    "Key Uniqueness": "V-04",
    "Data Type and Format Conformance": "V-05",
    "Referential Integrity": "V-06",
    "Business Rules": "V-07",
    "Aggregate Reconciliation": "V-08",
    "Output Completeness": "V-09",
    "Rejection Rate": "V-10",
}

log = logging.getLogger("blockflow.transform")


# --------------------------------------------------------------------------- #
# Value converters (Section 4)
# --------------------------------------------------------------------------- #
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
        if not number.is_finite() or abs(number) >= MONEY_LIMIT:
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
        stamp = stamp.tz_localize(None)  # keep the local wall-clock time
    return stamp.strftime("%Y-%m-%d %H:%M:%S")


CONVERTERS = {"int": _to_int, "dec": _to_decimal, "dt": _to_datetime_str}


def _text_value(text: str, kind: str) -> str | None:
    if kind == "code_upper":
        return text.upper()
    if kind == "code_lower":
        return text.lower()
    if kind == "title":
        return text.title()
    if kind == "phone":
        return re.sub(r"\D", "", text) or None
    return text


def _representation(kind: str, value) -> str:
    return format(value, "f") if kind == "dec" else str(value)


def _mask(value) -> str:
    """Quarantine files keep only the last 4 characters of a phone number."""
    text = "" if value is None else str(value)
    return text if len(text) <= 4 else "*" * (len(text) - 4) + text[-4:]


# --------------------------------------------------------------------------- #
# Lookup of parent keys (referential integrity)
# --------------------------------------------------------------------------- #
def load_lookups() -> dict:
    """Read stocks.stock_id and users.user_id from blockflow.db if it exists."""
    parents: dict = {"stock_ids": None, "user_ids": None, "source": "unavailable"}
    if not DB_PATH.is_file():
        return parents
    try:
        con = sqlite3.connect(f"{DB_PATH.as_uri()}?mode=ro", uri=True)
    except sqlite3.Error:
        return parents
    try:
        for key, sql in (("stock_ids", "SELECT stock_id FROM stocks"),
                         ("user_ids", "SELECT user_id FROM users")):
            try:
                parents[key] = {int(r[0]) for r in con.execute(sql) if r[0] is not None}
            except (sqlite3.Error, ValueError, TypeError):
                parents[key] = None
        if parents["stock_ids"] is not None or parents["user_ids"] is not None:
            parents["source"] = DB_PATH.name
    finally:
        con.close()
    return parents


# --------------------------------------------------------------------------- #
# Cleaning one table (Sections 3, 4 and 5)
# --------------------------------------------------------------------------- #
def clean_table(domain: str, df: pd.DataFrame, parents: dict, batch_id: str) -> dict:
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

    touched: dict[tuple[str, str, str], set[int]] = {}

    def mark(rule: str, col: str, kind: str, i: int) -> None:
        touched.setdefault((rule, col, kind), set()).add(i)

    data: dict[str, list] = {}
    blank: dict[str, list[bool]] = {}
    invalid: dict[str, list[bool]] = {}

    # Steps 1 and 2: text cleanup (C-01, C-02) and standardization (S-01 to S-06)
    for col in work_cols:
        kind = kinds[col]
        values, blanks, invalids = [], [], []
        for i, raw in enumerate(original[col].tolist()):
            if raw is None or (not isinstance(raw, str) and pd.isna(raw)):
                values.append(None); blanks.append(True); invalids.append(False)
                continue
            text = str(raw)
            stripped = text.strip()
            cleaned = stripped if kind == "text" else re.sub(r"\s+", " ", stripped)
            if cleaned != text:
                mark("C-01", col, "fixed", i)
            if cleaned.lower() in NULL_TEXT:
                mark("C-02", col, "fixed", i)
                values.append(None); blanks.append(True); invalids.append(False)
                continue
            blanks.append(False)
            if kind in TEXT_KINDS:
                value = _text_value(cleaned, kind)
                if kind != "text" and value != cleaned:
                    mark(TEXT_RULE[kind], col, "fixed", i)
                values.append(value); invalids.append(False)
            else:
                value = CONVERTERS[kind](cleaned)
                if value is None:
                    values.append(None); invalids.append(True)
                else:
                    if _representation(kind, value) != cleaned:
                        mark(S_RULE[kind], col, "fixed", i)
                    values.append(value); invalids.append(False)
        data[col], blank[col], invalid[col] = values, blanks, invalids

    reasons: list[list[tuple[str, str]]] = [[] for _ in range(n_in)]

    def reject(i: int, rule: str, col: str, text: str) -> None:
        reasons[i].append((rule, text))
        mark(rule, col, "rejected", i)

    # Step 3: missing values (C-03, C-04 fill; C-05 reject) and failed conversions
    required = list(spec["required"]) + ([pk] if has_pk else [])
    for col in required:
        rule_for_invalid = S_RULE.get(kinds[col], "C-05")
        for i, value in enumerate(data[col]):
            if value is None:
                if invalid[col][i]:
                    reject(i, rule_for_invalid, col, f"invalid {col}")
                else:
                    reject(i, "C-05", col, f"null {col}")
    filled_per_row = [0] * n_in
    for col, (placeholder, rule) in spec["fill"].items():
        for i, value in enumerate(data[col]):
            if value is None:
                data[col][i] = placeholder
                filled_per_row[i] += 1
                mark(rule, col, "filled", i)

    # Step 4: impossible values (C-06) and orphan foreign keys (C-08)
    for col in spec["positive"]:
        for i, value in enumerate(data[col]):
            if value is not None and value <= 0:
                reject(i, "C-06", col, f"{col} at or below 0")
    for col, parent_key in spec["fks"].items():
        parent = parents.get(parent_key)
        if parent is None:
            continue
        for i, value in enumerate(data[col]):
            if value is not None and value not in parent:
                reject(i, "C-08", col, f"orphan {col} {value}")

    # Step 5: duplicates on every business column, keeping the first occurrence (C-07)
    business = list(spec["columns"])
    seen: set = set()
    duplicates: set[int] = set()
    for i in range(n_in):
        if reasons[i]:
            continue
        key = tuple(data[col][i] for col in business)
        if key in seen:
            duplicates.add(i)
            reject(i, "C-07", "*", "duplicate of an earlier record")
        else:
            seen.add(key)

    keep = [i for i in range(n_in) if not reasons[i]]
    keep_set = set(keep)
    removed_idx = [i for i in range(n_in) if reasons[i]]

    # Output frame in contract order and types; B-01 derives total_amount
    out: dict[str, pd.Series] = {}
    for col in work_cols:
        picked = [data[col][i] for i in keep]
        if kinds[col] == "int":
            out[col] = pd.Series(pd.array(picked, dtype="Int64"))
        else:
            out[col] = pd.Series(picked, dtype="object")
    if domain == "sales":
        out["total_amount"] = pd.Series(
            [(Decimal(data["quantity"][i]) * data["unit_price"][i]).quantize(
                TWO_PLACES, rounding=ROUND_HALF_UP) for i in keep], dtype="object")
        for i in keep:
            mark("B-01", "total_amount", "fixed", i)
    order = ([pk] if has_pk else []) + spec["order"]
    clean = pd.DataFrame(out)[order].reset_index(drop=True)

    # Quarantine rows: original values plus batch ID, rule ID(s), and reason(s)
    removed = original.iloc[removed_idx].copy()
    if "phone_number" in removed.columns:
        removed["phone_number"] = removed["phone_number"].map(_mask)
    removed["batch_id"] = batch_id
    removed["rule_id"] = ["; ".join(r for r, _ in reasons[i]) for i in removed_idx]
    removed["rejection_reason"] = ["; ".join(t for _, t in reasons[i]) for i in removed_idx]

    # Counts: rules applied (retained rows modified, or rows rejected) and the profile
    rule_rows: dict[str, set[int]] = {}
    modified_rows: set[int] = set()
    for (rule, col, kind), rows in touched.items():
        counted = rows if kind == "rejected" else rows & keep_set
        rule_rows.setdefault(rule, set()).update(counted)
        if kind in ("fixed", "filled") and rule != "B-01":
            modified_rows |= counted
    rules_applied = {r: len(rows) for r, rows in sorted(rule_rows.items()) if rows}

    profile = []
    for (rule, col, kind), rows in sorted(touched.items()):
        label = PROFILE_LABELS.get((rule, kind))
        if label is None or not rows:
            continue
        if rule == "C-02":
            if col in spec["fill"]:
                planned = spec["fill"][col][1]
            elif col in required:
                planned = "C-05"
            else:
                planned = "-"
        else:
            planned = rule
        profile.append({"table": domain, "column": col, "issue": label,
                        "records": len(rows),
                        "percent": round(100 * len(rows) / n_in, 1) if n_in else 0.0,
                        "planned_rule": planned})

    measure = spec["measure"]
    zero = Decimal(0)
    sum_in = sum((v for v in data[measure] if v is not None), zero)
    sum_rejected = sum((data[measure][i] for i in removed_idx if data[measure][i] is not None), zero)

    return {
        "df": clean,
        "removed": removed,
        "has_pk": has_pk,
        "profile": profile,
        "stats": {
            "rows_in": n_in,
            "rows_out": len(keep),
            "rejected": len(removed_idx),
            "removed": len(removed_idx) - len(duplicates),
            "duplicates_dropped": len(duplicates),
            "records_modified": len(modified_rows & keep_set),
            "values_filled": sum(filled_per_row[i] for i in keep),
            "rules_applied": rules_applied,
            "sum_in": sum_in,
            "sum_rejected": sum_rejected,
            "columns_dropped": dropped_columns,
        },
    }


@task(name="transform-tables", cache_policy=NO_CACHE, retries=MAX_RETRIES,
      retry_delay_seconds=RETRY_DELAY_SECONDS, timeout_seconds=TIMEOUT_SECONDS)
def transform_tables(frames: dict, batch_id: str) -> dict:
    """Prefect task. Tables are processed stocks, then sales, then expenses."""
    parents = load_lookups()
    results = {}
    for domain in TABLES:
        results[domain] = clean_table(domain, frames[domain], parents, batch_id)
        if domain == "stocks" and parents["stock_ids"] is not None and results[domain]["has_pk"]:
            batch_keys = {int(v) for v in results[domain]["df"]["stock_id"].dropna()}
            parents["stock_ids"] = parents["stock_ids"] | batch_keys
    return {"tables": results, "parents": parents}


# --------------------------------------------------------------------------- #
# Validation checks (Section 7)
# --------------------------------------------------------------------------- #
def check(name: str, status: str, detail: str = "", domain: str | None = None) -> dict:
    result = {"id": CHECK_IDS.get(name, "-"), "check": name, "domain": domain,
              "status": status, "detail": detail}
    level = logging.WARNING if status in (WARNING, FAIL) else logging.INFO
    if status == FAIL:
        level = logging.ERROR
    log.log(level, "[%s] %s %s%s %s", status, result["id"], name,
            f" ({domain})" if domain else "", detail)
    return result


def input_problems(extraction: dict, approved_after_review: bool) -> list[str]:
    """V-01 Input Handover: the package must be what Transform needs, nothing more."""
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
            ok = all(isinstance(v, Decimal) and v.as_tuple().exponent == -2 and abs(v) < MONEY_LIMIT
                     for v in values)
        elif kind == "dt":
            ok = all(isinstance(v, str) and DATETIME_RE.match(v) for v in values)
        else:
            ok = all(isinstance(v, str) for v in values)
        if not ok:
            problems.append(f"{col} is not a valid {kind}")
    return problems


def validate_table(domain: str, result: dict, parents: dict) -> list[dict]:
    """V-02 to V-10 for one transformed table."""
    df, stats, has_pk = result["df"], result["stats"], result["has_pk"]
    spec = TABLES[domain]
    checks = []

    accounted = stats["rows_out"] + stats["rejected"]
    checks.append(check("Record Reconciliation", PASS if accounted == stats["rows_in"] else FAIL,
                        f"in={stats['rows_in']} out={stats['rows_out']} rejected={stats['rejected']}",
                        domain))

    nulls = int(df.isna().sum().sum())
    checks.append(check("Required Values Not Null", FAIL if nulls else PASS,
                        f"{nulls} null values" if nulls else "", domain))

    dup_rows = int(df.duplicated(subset=list(spec["columns"])).sum())
    dup_keys = int(df[spec["pk"]].duplicated().sum()) if has_pk else 0
    checks.append(check("Key Uniqueness", FAIL if (dup_rows or dup_keys) else PASS,
                        f"{dup_rows} duplicate records, {dup_keys} repeated keys"
                        if (dup_rows or dup_keys) else "", domain))

    problems = schema_problems(domain, df, has_pk)
    checks.append(check("Data Type and Format Conformance", FAIL if problems else PASS,
                        "; ".join(problems), domain))

    available = {col: parents.get(key) for col, key in spec["fks"].items()
                 if parents.get(key) is not None}
    if not available:
        checks.append(check("Referential Integrity", SKIPPED,
                            "parent lookup unavailable (blockflow.db not readable)", domain))
    else:
        orphans = sum(1 for col, parent in available.items()
                      for v in df[col].tolist() if pd.isna(v) or int(v) not in parent)
        checks.append(check("Referential Integrity", FAIL if orphans else PASS,
                            f"{orphans} orphan keys" if orphans else
                            f"checked {', '.join(available)}", domain))

    bad = 0
    for col in spec["positive"]:
        bad += sum(1 for v in df[col].tolist() if pd.isna(v) or v <= 0)
    if domain == "sales":
        bad += sum(
            1 for q, p, t in zip(df["quantity"].tolist(), df["unit_price"].tolist(),
                                 df["total_amount"].tolist())
            if t < 0 or t != (Decimal(q) * p).quantize(TWO_PLACES, rounding=ROUND_HALF_UP))
    checks.append(check("Business Rules", FAIL if bad else PASS,
                        f"{bad} rule violations" if bad else "", domain))

    measure = spec["measure"]
    sum_out = sum((v for v in df[measure].tolist() if not pd.isna(v)), Decimal(0))
    balanced = stats["sum_in"] == sum_out + stats["sum_rejected"]
    checks.append(check("Aggregate Reconciliation", PASS if balanced else FAIL,
                        f"sum({measure}) in={stats['sum_in']} out={sum_out} "
                        f"rejected={stats['sum_rejected']}", domain))

    checks.append(check("Output Completeness", PASS if stats["rows_out"] > 0 else FAIL,
                        f"{stats['rows_out']} rows after cleaning", domain))

    rate = stats["rejected"] / stats["rows_in"] if stats["rows_in"] else 0.0
    checks.append(check("Rejection Rate", WARNING if rate > REJECTION_RATE_LIMIT else PASS,
                        f"{stats['rejected']} of {stats['rows_in']} rows rejected ({rate:.1%}), "
                        f"threshold {REJECTION_RATE_LIMIT:.0%}", domain))
    return checks


# --------------------------------------------------------------------------- #
# Run one transformation (called inside the Prefect flow)
# --------------------------------------------------------------------------- #
def run_transform(extraction: dict, approved_after_review: bool = False,
                  accept_warnings: bool = False) -> dict:
    started = datetime.now()
    t0 = time.monotonic()
    run_id = f"transform_{started:%Y%m%d_%H%M%S}"
    metadata_in = extraction.get("metadata") if isinstance(extraction.get("metadata"), dict) else {}
    parent_run_id = metadata_in.get("pipeline_run_id")
    window = metadata_in.get("extraction_window")
    batch_id = f"BATCH_{str(window).replace('-', '')}" if window else "BATCH_UNKNOWN"

    for directory in (LOGS_DIR, AUDIT_DIR):
        directory.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(LOGS_DIR / f"{run_id}.log", encoding="utf-8")
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    log.setLevel(LOG_LEVEL)
    log.addHandler(handler)
    log.info("Starting %s (v%s) for %s, extraction run %s", run_id, TRANSFORM_VERSION,
             batch_id, parent_run_id)

    checks: list[dict] = []
    tables: dict[str, dict] = {}
    parents: dict = {"source": "unavailable"}
    error_message = "NONE"

    problems = input_problems(extraction, approved_after_review)
    if problems:
        checks.append(check("Input Handover", FAIL, "; ".join(problems)))
        error_message = "INPUT_HANDOVER_FAILED"
    else:
        checks.append(check("Input Handover", PASS, "3 DataFrames, run metadata, validation summary"))
        try:
            outcome = transform_tables(extraction["dataframes"], batch_id)
        except Exception as exc:  # task failed after its retries, or timed out
            log.error("Transformation of %s (extraction run %s) failed: %s",
                      batch_id, parent_run_id, exc, exc_info=True)
            checks.append(check("Transformation Task", FAIL, str(exc)))
            error_message = f"TASK_FAILED:{type(exc).__name__}"
        else:
            tables, parents = outcome["tables"], outcome["parents"]
            log.info("Parent lookup source: %s", parents["source"])
            for domain, result in tables.items():
                log.debug("%s: rules applied %s", domain, result["stats"]["rules_applied"])
                if result["stats"]["columns_dropped"]:
                    log.info("Dropped columns outside the data contract from %s: %s",
                             domain, result["stats"]["columns_dropped"])
                checks.extend(validate_table(domain, result, parents))

    elapsed = time.monotonic() - t0

    # Acceptance decision (Section 10)
    statuses = {c["status"] for c in checks}
    warnings_accepted = False
    if FAIL in statuses:
        status = "REJECTED"
        if error_message == "NONE":
            first = next(c for c in checks if c["status"] == FAIL)
            error_message = first["check"].upper().replace(" ", "_")
    elif WARNING in statuses and not accept_warnings:
        status = "MANUAL REVIEW"
        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        for domain, result in tables.items():
            result["df"].to_csv(STAGING_DIR / f"{run_id}_{domain}.csv", index=False, encoding="utf-8")
        log.warning("Held for Owner review. Transformed data staged in %s", STAGING_DIR)
    else:
        status = "APPROVED"
        warnings_accepted = WARNING in statuses
        if warnings_accepted:
            log.warning("Warnings accepted by the Owner; handing over to Load")

    # Quarantine (kept whatever the decision)
    for domain, result in tables.items():
        if len(result["removed"]):
            ROW_REJECTED_DIR.mkdir(parents=True, exist_ok=True)
            path = ROW_REJECTED_DIR / f"{run_id}_{domain}_rejected.csv"
            result["removed"].to_csv(path, index=False, encoding="utf-8")
            log.warning("Wrote %d quarantined %s rows to %s", len(result["removed"]), domain, path)

    summary = {}
    table_status = {}
    for domain in TABLES:
        found = {c["status"] for c in checks if c["domain"] == domain}
        table_status[domain] = (FAIL if FAIL in found else WARNING if WARNING in found
                                else PASS if found else "NOT RUN")
        summary[f"{domain}_transformation"] = table_status[domain]
    summary["overall_transformation"] = status

    tables_audit = {}
    for domain, result in tables.items():
        s = result["stats"]
        tables_audit[domain] = {
            "records_read": s["rows_in"],
            "records_transformed": s["rows_out"],
            "records_rejected": s["rejected"],
            "records_removed": s["removed"],
            "duplicates_dropped": s["duplicates_dropped"],
            "records_modified": s["records_modified"],
            "values_filled": s["values_filled"],
            "rules_applied": s["rules_applied"],
            "status": table_status[domain],
        }

    finished = datetime.now()
    audit = {
        "run_id": run_id,
        "parent_run_id": parent_run_id,
        "batch_id": batch_id,
        "batch_window": window,
        "transform_version": TRANSFORM_VERSION,
        "status": status,
        "approved_after_review": approved_after_review,
        "warnings_accepted": warnings_accepted,
        "lookup_source": parents.get("source", "unavailable"),
        "transformation_start": started.isoformat(sep=" ", timespec="seconds"),
        "transformation_end": finished.isoformat(sep=" ", timespec="seconds"),
        "transformation_duration_seconds": round(elapsed, 1),
        "tables": tables_audit,
        "profile": [p for r in tables.values() for p in r["profile"]],
        "validation_results": checks,
        "validation_summary": summary,
        "error_message": error_message if status == "REJECTED" else "NONE",
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
def transform_staged_flow(extraction_run_id: str, accept_warnings: bool = False) -> dict:
    """Owner approval path: transform and load the data of an extraction held for MANUAL REVIEW."""
    from load import run_load             # imported here: neither module imports this one
    from pipeline_audit import PipelineAudit

    held = load_staged(extraction_run_id)
    audit = PipelineAudit(trigger="owner_approval", window=held["metadata"].get("extraction_window"))
    try:
        audit.record_stage("extraction", held, status="APPROVED (after review)", outcome="ok",
                           note="held for MANUAL REVIEW, then approved by the Owner")
        transformed = audit.run_stage("transform", run_transform, held, approved_after_review=True,
                                      accept_warnings=accept_warnings)
        if transformed["status"] == "APPROVED":
            rows = sum(len(df) for df in transformed["dataframes"].values())
            transformed["load"] = audit.run_stage("load", run_load, transformed, records_in=rows)["status"]
    except BaseException:
        audit.finish()
        raise
    summary = audit.finish()
    transformed["pipeline"], transformed["pipeline_run_id"] = summary["status"], summary["run_id"]
    return transformed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BlockFlow transformation")
    parser.add_argument("--staged", metavar="EXTRACTION_RUN_ID",
                        help="approve and transform an extraction held for MANUAL REVIEW")
    parser.add_argument("--accept-warnings", action="store_true",
                        help="Owner accepts non-critical warnings and hands over to Load")
    args = parser.parse_args()
    if not args.staged:
        parser.error("use run_pipeline.py for a normal run, or --staged RUN_ID to approve a held extraction")
    try:
        outcome = transform_staged_flow(args.staged, args.accept_warnings)
    except (FileNotFoundError, ValueError) as exc:
        raise SystemExit(f"Cannot approve {args.staged}: {exc}")
    print(f"Pipeline: {outcome['pipeline']}  |  Transformation: {outcome['status']}  |  Load: {outcome.get('load')}")
    print(f"Audit record: audit/{outcome['pipeline_run_id']}.json")
