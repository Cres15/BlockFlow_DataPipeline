"""
BlockFlow ETL - Extract stage (Issue #2: Implement CSV Data Extraction)

Scans the entry directory for the previous calendar day's raw CSV batch files
(stocks, sales, expenses), validates them, reads them into pandas DataFrames
and decides APPROVED / MANUAL REVIEW / REJECTED before handing over to Transform.

Run once now:      python extract.py
Run on schedule:   python extract.py --serve      (12:00 AM daily, Asia/Manila)

Requires: pip install pandas prefect
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import shutil
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

os.environ.setdefault("PREFECT_LOGGING_EXTRA_LOGGERS", "blockflow")

from prefect import flow, task  # noqa: E402

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
BASE_DIR = Path(__file__).resolve().parent
ENTRY_DIR = BASE_DIR / "entry"          # raw daily CSV batch files land here
REJECTED_DIR = ENTRY_DIR / "rejected"   # quarantined files
STAGING_DIR = BASE_DIR / "staging"      # MANUAL REVIEW holding area
LOGS_DIR = BASE_DIR / "logs"            # one plain-text log per run
AUDIT_DIR = BASE_DIR / "audit"          # one JSON summary per run

TIMEOUT_SECONDS = 30 * 60               # 30-minute execution timeout
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 60
LOW_COUNT_RATIO = 0.5                   # WARNING if rows < 50% of recent average
HISTORY_RUNS = 7                        # how many past audits to compare against

# ASSUMPTION: batch files are named <domain>_<YYYY-MM-DD>.csv
# e.g. entry/sales_2026-10-20.csv. Change this if your export names differ.
FILE_PATTERN = "{domain}_{day}.csv"

# Data contract (Section 6). total_amount is derived, so it is never required.
DOMAINS: dict[str, dict] = {
    "stocks": {
        "required": ["size", "quantity", "unit", "unit_price", "updated_at", "recorded_by"],
        "date_col": "updated_at",
        "id_cols": ["recorded_by"],
        "key": ["stock_id"],
    },
    "sales": {
        "required": ["customer_name", "phone_number", "shop_name", "quantity",
                     "unit_price", "sale_date", "recorded_by", "stock_id"],
        "date_col": "sale_date",
        "id_cols": ["recorded_by", "stock_id"],
        "key": ["sale_id"],
    },
    "expenses": {
        "required": ["category", "description", "amount", "expense_date", "recorded_by"],
        "date_col": "expense_date",
        "id_cols": ["recorded_by"],
        "key": ["expense_id"],
    },
}

# Documented source -> standardized column mapping (Section 6.3).
# Anything not listed here is NOT silently coerced.
HEADER_MAP = {
    "Customer Name": "customer_name",
    "Phone Number": "phone_number",
    "Shop Name": "shop_name",
    "Quantity": "quantity",
    "Unit Price": "unit_price",
    "Total Amount": "total_amount",
    "Sale Date": "sale_date",
    "Recorded by": "recorded_by",
    "Updated At": "updated_at",
    "Category": "category",
    "Description": "description",
    "Amount": "amount",
    "Expense Date": "expense_date",
    # Spelling variants that appear in the documentation itself
    "sales_date": "sale_date",
    "expenses_date": "expense_date",
    "stocks_id": "stock_id",
    "sales_id": "sale_id",
    "expenses_id": "expense_id",
}

PASS, WARNING, FAIL = "PASS", "WARNING", "FAIL"
log = logging.getLogger("blockflow.extract")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def check(name: str, status: str, detail: str = "", domain: str | None = None) -> dict:
    result = {"check": name, "domain": domain, "status": status, "detail": detail}
    level = {PASS: logging.INFO, WARNING: logging.WARNING, FAIL: logging.ERROR}[status]
    log.log(level, "[%s] %s%s %s", status, name, f" ({domain})" if domain else "", detail)
    return result


def count_raw_records(path: Path) -> int:
    """Data rows in the raw file, header excluded, blank lines ignored."""
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        next(reader, None)
        return sum(1 for row in reader if any(cell.strip() for cell in row))


def recent_average_rows(domain: str) -> float | None:
    """Average extracted rows for this domain over recent APPROVED runs."""
    counts = []
    for audit_file in sorted(AUDIT_DIR.glob("run_*.json"), reverse=True):
        try:
            audit = json.loads(audit_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if audit.get("status") == "APPROVED" and domain in audit.get("records_extracted", {}):
            counts.append(audit["records_extracted"][domain])
        if len(counts) >= HISTORY_RUNS:
            break
    return sum(counts) / len(counts) if counts else None


def quarantine(path: Path) -> None:
    """Move a failing file to rejected/ so the next run does not reprocess it."""
    if not path.exists():
        return
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)
    target = REJECTED_DIR / path.name
    if target.exists():
        target = REJECTED_DIR / f"{path.stem}_{datetime.now():%H%M%S}{path.suffix}"
    shutil.move(str(path), str(target))
    log.error("Quarantined %s -> %s", path.name, target)


# --------------------------------------------------------------------------- #
# Prefect tasks
# --------------------------------------------------------------------------- #
@task(retries=MAX_RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS,
      timeout_seconds=TIMEOUT_SECONDS, name="locate-batch-file")
def locate_batch_file(domain: str, day: date) -> Path:
    """Missing file -> raise so Prefect retries (up to 3), then the flow records a FAIL."""
    path = ENTRY_DIR / FILE_PATTERN.format(domain=domain, day=day.isoformat())
    if not path.is_file():
        raise FileNotFoundError(f"Missing batch file: {path.name}")
    return path


@task(timeout_seconds=TIMEOUT_SECONDS, name="extract-domain")
def extract_domain(domain: str, path: Path, day: date) -> dict:
    """Validate one batch file and read it into a DataFrame."""
    spec = DOMAINS[domain]
    checks: list[dict] = []
    out = {"domain": domain, "file": path.name, "df": None, "checks": checks,
           "read": 0, "extracted": 0, "rejected": 0}

    # ---- File Accessibility -------------------------------------------------
    try:
        if path.stat().st_size == 0:
            raise ValueError("file is empty")
        raw_count = count_raw_records(path)
        df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    except Exception as exc:  # unreadable, empty, malformed
        checks.append(check("File Accessibility", FAIL, f"{path.name}: {exc}", domain))
        return out
    checks.append(check("File Accessibility", PASS, path.name, domain))
    out["read"] = raw_count

    # ---- Header standardization + Columns Exist -----------------------------
    df = df.rename(columns={c: HEADER_MAP.get(c.strip(), c.strip()) for c in df.columns})
    if "total_amount" in df.columns:  # derived field, never accepted from source
        df = df.drop(columns=["total_amount"])
        log.info("Dropped source total_amount for %s (derived as quantity x unit_price)", domain)

    missing = [c for c in spec["required"] if c not in df.columns]
    if missing:
        checks.append(check("Columns Exist", FAIL, f"missing/renamed: {missing}", domain))
        return out
    checks.append(check("Columns Exist", PASS, "", domain))

    # ---- Record Count Integrity ---------------------------------------------
    if len(df) == 0 or len(df) != raw_count:
        checks.append(check("Record Count Integrity", FAIL,
                            f"{path.name}: DataFrame={len(df)} raw_file={raw_count}", domain))
        return out
    checks.append(check("Record Count Integrity", PASS, f"{len(df)} rows", domain))

    # ---- Present Identifiers --------------------------------------------------
    bad_ids = {}
    for col in spec["id_cols"]:
        n = int(df[col].astype(str).str.strip().isin(["", "nan", "None", "NULL"]).sum())
        if n:
            bad_ids[col] = n
    if bad_ids:
        checks.append(check("Present Identifiers", FAIL, f"null identifiers: {bad_ids}", domain))
        return out
    checks.append(check("Present Identifiers", PASS, "", domain))

    # ---- Primary Key Uniqueness -----------------------------------------------
    key_cols = [c for c in spec["key"] if c in df.columns] or list(df.columns)
    dup = int(df.duplicated(subset=key_cols).sum())
    if dup:
        checks.append(check("Primary Key Uniqueness", FAIL,
                            f"{dup} duplicate rows on {key_cols}", domain))
        return out
    checks.append(check("Primary Key Uniqueness", PASS, f"key={key_cols}", domain))

    # ---- Incremental window: keep only the target calendar day ---------------
    parsed = pd.to_datetime(df[spec["date_col"]], errors="coerce")
    unparseable = int(parsed.isna().sum())
    in_window = parsed.dt.date == day
    df = df[in_window].reset_index(drop=True)
    out["rejected"] = raw_count - len(df)
    out["extracted"] = len(df)
    if unparseable:
        checks.append(check("Date Format", WARNING,
                            f"{unparseable} unparseable {spec['date_col']} values", domain))
    if len(df) == 0:
        checks.append(check("Extraction Window", WARNING,
                            f"no records dated {day} in {path.name}", domain))
    else:
        checks.append(check("Extraction Window", PASS, f"{len(df)} rows for {day}", domain))

    # ---- Unusually low count vs recent runs (non-critical) --------------------
    avg = recent_average_rows(domain)
    if avg and len(df) < avg * LOW_COUNT_RATIO:
        checks.append(check("Record Volume", WARNING,
                            f"{len(df)} rows vs recent average {avg:.0f}", domain))

    out["df"] = df
    return out


# --------------------------------------------------------------------------- #
# Prefect flow
# --------------------------------------------------------------------------- #
@flow(name="blockflow-extract", timeout_seconds=TIMEOUT_SECONDS, retries=0)
def extraction_flow(target_day: date | None = None) -> dict:
    started = datetime.now()
    t0 = time.monotonic()
    day = target_day or (date.today() - timedelta(days=1))  # previous calendar day
    run_id = f"run_{started:%Y%m%d_%H%M%S}"

    for d in (LOGS_DIR, AUDIT_DIR):
        d.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(LOGS_DIR / f"{run_id}.log", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.setLevel(logging.INFO)
    log.addHandler(handler)
    log.info("Starting %s for extraction window %s", run_id, day)

    checks: list[dict] = []
    frames: dict[str, pd.DataFrame] = {}
    files: dict[str, str] = {}
    paths: dict[str, Path] = {}
    read: dict[str, int] = {}
    extracted: dict[str, int] = {}
    rejected: dict[str, int] = {}
    error_message = "NONE"

    # ---- Source Connection ------------------------------------------------
    if ENTRY_DIR.is_dir() and os.access(ENTRY_DIR, os.R_OK):
        checks.append(check("Source Connection", PASS, str(ENTRY_DIR)))
        source_ok = True
    else:
        checks.append(check("Source Connection", FAIL,
                            f"{ENTRY_DIR} missing or not readable"))
        error_message = "SOURCE_CONNECTION_FAILED"
        source_ok = False

    # ---- Per-domain extraction --------------------------------------------
    if source_ok:
        for domain in DOMAINS:
            try:
                path = locate_batch_file(domain, day)  # retried up to 3x by Prefect
            except Exception as exc:
                checks.append(check("File Accessibility", FAIL,
                                    f"{domain}: batch file not found after {MAX_RETRIES} retries ({exc})",
                                    domain))
                error_message = f"MISSING_FILE:{domain}"
                continue
            paths[domain] = path
            files[domain] = path.name
            try:
                result = extract_domain(domain, path, day)
            except Exception as exc:  # timeout or unexpected error
                checks.append(check("Extraction Task", FAIL, f"{domain}: {exc}", domain))
                error_message = f"TASK_FAILED:{domain}"
                continue
            checks.extend(result["checks"])
            read[domain] = result["read"]
            extracted[domain] = result["extracted"]
            rejected[domain] = result["rejected"]
            if result["df"] is not None:
                frames[domain] = result["df"]

        # ---- Domain Completeness ------------------------------------------
        absent = [d for d in DOMAINS if d not in frames]
        if absent:
            checks.append(check("Domain Completeness", FAIL, f"missing domains: {absent}"))
        else:
            checks.append(check("Domain Completeness", PASS, "stocks, sales, expenses"))

    # ---- Execution Completion within Time Limit ----------------------------
    elapsed = time.monotonic() - t0
    if elapsed > TIMEOUT_SECONDS:
        checks.append(check("Execution Completion", FAIL, f"{elapsed:.0f}s > {TIMEOUT_SECONDS}s"))
    else:
        checks.append(check("Execution Completion", PASS, f"{elapsed:.1f}s"))

    # ---- Acceptance decision ----------------------------------------------
    statuses = {c["status"] for c in checks}
    if FAIL in statuses:
        status = "REJECTED"
        for domain, path in paths.items():  # quarantine every file from a failed run
            domain_failed = any(c["domain"] == domain and c["status"] == FAIL for c in checks)
            if domain_failed:
                quarantine(path)
        if error_message == "NONE":
            first_fail = next(c for c in checks if c["status"] == FAIL)
            error_message = f"{first_fail['check'].upper().replace(' ', '_')}"
    elif WARNING in statuses:
        status = "MANUAL REVIEW"
        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        for domain, df in frames.items():
            df.to_csv(STAGING_DIR / f"{run_id}_{domain}.csv", index=False)
        log.warning("Held for Owner review. Staged data in %s", STAGING_DIR)
    else:
        status = "APPROVED"

    finished = datetime.now()
    audit = {
        "pipeline_run_id": run_id,
        "files": files,
        "extraction_start": started.isoformat(sep=" ", timespec="seconds"),
        "extraction_end": finished.isoformat(sep=" ", timespec="seconds"),
        "status": status,
        "records_read": read,
        "records_extracted": extracted,
        "records_rejected": rejected,
        "extraction_window": day.isoformat(),
        "validation_results": checks,
        "error_message": error_message if status == "REJECTED" else "NONE",
        "extraction_duration_seconds": round(elapsed, 1),
    }
    (AUDIT_DIR / f"{run_id}.json").write_text(json.dumps(audit, indent=2), encoding="utf-8")
    log.info("Run %s finished: %s in %.1fs", run_id, status, elapsed)
    log.removeHandler(handler)
    handler.close()

    # ---- Handover ----------------------------------------------------------
    # Only APPROVED runs pass DataFrames + run metadata + validation summary on.
    return {
        "status": status,
        "dataframes": frames if status == "APPROVED" else {},
        "metadata": audit,
    }
    # Next stage (Issue #3):  transform(result["dataframes"], result["metadata"])


# --------------------------------------------------------------------------- #
# Monthly archive (log retention)
# --------------------------------------------------------------------------- #
def archive_previous_month(today: date | None = None) -> Path | None:
    """Zip last month's logs + audit files, verify the zip, then remove the originals."""
    import zipfile

    today = today or date.today()
    prev = (today.replace(day=1) - timedelta(days=1))
    tag = f"{prev:%Y%m}"
    members = [*LOGS_DIR.glob(f"run_{tag}*"), *AUDIT_DIR.glob(f"run_{tag}*")]
    if not members:
        return None
    archive_dir = BASE_DIR / "archive"
    archive_dir.mkdir(exist_ok=True)
    zip_path = archive_dir / f"extraction_{tag}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for m in members:
            z.write(m, arcname=f"{m.parent.name}/{m.name}")
    with zipfile.ZipFile(zip_path) as z:  # verify readable before deleting originals
        if z.testzip() is not None:
            raise RuntimeError(f"Archive verification failed: {zip_path}")
    for m in members:
        m.unlink()
    return zip_path


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BlockFlow CSV extraction")
    parser.add_argument("--serve", action="store_true",
                        help="serve on the 12:00 AM daily schedule (max 1 active run)")
    parser.add_argument("--date", type=date.fromisoformat,
                        help="extract a specific YYYY-MM-DD instead of yesterday (re-run/recovery)")
    parser.add_argument("--archive", action="store_true", help="archive last month's logs/audits")
    args = parser.parse_args()

    if args.archive:
        print(archive_previous_month() or "Nothing to archive")
    elif args.serve:
        extraction_flow.serve(
            name="blockflow-extract-daily",
            cron="0 0 * * *",
            timezone="Asia/Manila",
            limit=1,  # Max Active Runs: 1
        )
    else:
        result = extraction_flow(args.date)
        print(f"Extraction status: {result['status']}")