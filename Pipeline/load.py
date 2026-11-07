"""
BlockFlow ETL - Load stage (Stage 3: SQLite Data Loading), version 1.0.0

Takes the transformed DataFrames handed over by an APPROVED Transform run and saves
them into the SQLite database (blockflow.db).

  1. connect and make sure the tables, constraints and indexes exist
  2. insert / upsert stocks, then sales, then expenses, in ONE transaction
     (stocks first, because sales.stock_id points at stocks)
  3. verify the result inside the transaction (row counts, every row present,
     foreign keys) and only then COMMIT - any error means ROLLBACK, so the
     database never holds half a batch
  4. re-count on a fresh connection after the commit
  5. write the run log (logs/load_<timestamp>.log) and audit summary
     (audit/load_<timestamp>.json)

Re-running the same batch is safe: rows that already exist are not duplicated.
  - A table whose DataFrame carries its primary key (stock_id, sale_id, expense_id)
    is UPSERTed on that key.
  - Otherwise a row is inserted only if no identical row exists, and SQLite assigns
    the new primary key.

Run through the whole pipeline:   python run_pipeline.py
Only create the tables/indexes:   python load.py --init
Show the current row counts:      python load.py --counts
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import time
import traceback
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd

os.environ.setdefault("PREFECT_LOGGING_EXTRA_LOGGERS", "blockflow")

from prefect import task  # noqa: E402
from prefect.cache_policies import NO_CACHE  # noqa: E402

from extract import (  # noqa: E402
    AUDIT_DIR,
    BASE_DIR,
    LOG_FORMAT,
    LOG_LEVEL,
    LOGS_DIR,
    MAX_RETRIES,
    RETRY_DELAY_SECONDS,
    TIMEOUT_SECONDS,
)

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
LOAD_VERSION = "1.0.0"
DB_PATH = Path(os.environ.get("BLOCKFLOW_DB", BASE_DIR / "blockflow.db"))
BUSY_TIMEOUT_MS = 30_000               # wait up to 30 s for a lock before "database is locked"
LOAD_RETRY_DELAY_SECONDS = RETRY_DELAY_SECONDS
LOAD_ORDER = ["stocks", "sales", "expenses"]   # parents before children

# Money is handed over as exact Decimal values; SQLite needs them as text/number.
sqlite3.register_adapter(Decimal, str)

# Created only if missing. An existing blockflow.db keeps its own tables untouched;
# its columns are checked instead (see check_schema).
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
    user_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    username      VARCHAR
);
CREATE TABLE IF NOT EXISTS stocks (
    stock_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    size          VARCHAR       NOT NULL,
    quantity      INTEGER       NOT NULL CHECK (quantity > 0),
    unit          VARCHAR       NOT NULL,
    unit_price    DECIMAL(12,2) NOT NULL CHECK (unit_price > 0),
    updated_at    DATETIME      NOT NULL,
    recorded_by   INTEGER       NOT NULL REFERENCES users (user_id)
);
CREATE TABLE IF NOT EXISTS sales (
    sale_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_name VARCHAR       NOT NULL,
    phone_number  VARCHAR       NOT NULL,
    shop_name     VARCHAR       NOT NULL,
    quantity      INTEGER       NOT NULL CHECK (quantity > 0),
    unit_price    DECIMAL(12,2) NOT NULL CHECK (unit_price > 0),
    total_amount  DECIMAL(12,2) NOT NULL CHECK (total_amount >= 0),
    sale_date     DATETIME      NOT NULL,
    recorded_by   INTEGER       NOT NULL REFERENCES users (user_id),
    stock_id      INTEGER       NOT NULL REFERENCES stocks (stock_id)
);
CREATE TABLE IF NOT EXISTS expenses (
    expense_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    category      VARCHAR       NOT NULL,
    description   TEXT          NOT NULL,
    amount        DECIMAL(12,2) NOT NULL CHECK (amount > 0),
    expense_date  DATETIME      NOT NULL,
    recorded_by   INTEGER       NOT NULL REFERENCES users (user_id)
);
CREATE INDEX IF NOT EXISTS idx_stocks_updated_at   ON stocks   (updated_at);
CREATE INDEX IF NOT EXISTS idx_stocks_recorded_by  ON stocks   (recorded_by);
CREATE INDEX IF NOT EXISTS idx_sales_sale_date     ON sales    (sale_date);
CREATE INDEX IF NOT EXISTS idx_sales_stock_id      ON sales    (stock_id);
CREATE INDEX IF NOT EXISTS idx_sales_recorded_by   ON sales    (recorded_by);
CREATE INDEX IF NOT EXISTS idx_expenses_date       ON expenses (expense_date);
CREATE INDEX IF NOT EXISTS idx_expenses_recorded_by ON expenses (recorded_by);
"""

# pk: primary key. cols: business columns written by the pipeline.
TABLES = {
    "stocks": {"pk": "stock_id",
               "cols": ["size", "quantity", "unit", "unit_price", "updated_at", "recorded_by"]},
    "sales": {"pk": "sale_id",
              "cols": ["customer_name", "phone_number", "shop_name", "quantity", "unit_price",
                       "total_amount", "sale_date", "recorded_by", "stock_id"]},
    "expenses": {"pk": "expense_id",
                 "cols": ["category", "description", "amount", "expense_date", "recorded_by"]},
}

log = logging.getLogger("blockflow.load")


class LoadError(Exception):
    """A data or verification problem. The transaction is rolled back; not retried."""


# --------------------------------------------------------------------------- #
# Connection and setup
# --------------------------------------------------------------------------- #
def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    # isolation_level=None: we issue BEGIN / COMMIT / ROLLBACK ourselves.
    con = sqlite3.connect(DB_PATH, timeout=BUSY_TIMEOUT_MS / 1000, isolation_level=None)
    con.execute("PRAGMA foreign_keys = ON")
    con.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    return con


def init_schema(con: sqlite3.Connection) -> None:
    """Create any missing table or index. Existing tables are never altered."""
    con.executescript(SCHEMA_SQL)


def check_schema(con: sqlite3.Connection) -> None:
    """An older blockflow.db may differ from the contract: fail early with a clear message."""
    for table, spec in TABLES.items():
        found = {row[1] for row in con.execute(f"PRAGMA table_info({table})")}
        missing = [c for c in spec["cols"] if c not in found]
        if missing:
            raise LoadError(f"Table {table} in {DB_PATH.name} has no column(s): {', '.join(missing)}")


# --------------------------------------------------------------------------- #
# Insert / upsert one table
# --------------------------------------------------------------------------- #
def _py(value):
    """pandas/numpy scalar -> plain Python value that sqlite3 can bind."""
    if value is None or value is pd.NA:
        return None
    if isinstance(value, Decimal):
        return value
    if hasattr(value, "item"):          # numpy int64 / float64 / bool_
        value = value.item()
    if isinstance(value, float) and value != value:   # NaN
        return None
    return value


def _rows(df: pd.DataFrame, columns: list[str]) -> list[tuple]:
    return [tuple(_py(v) for v in row) for row in df[columns].itertuples(index=False, name=None)]


def _match_sql(table: str, columns: list[str]) -> str:
    return " AND ".join(f"{c} = ?" for c in columns)


def count_rows(con: sqlite3.Connection, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def load_table(con: sqlite3.Connection, table: str, df: pd.DataFrame) -> dict:
    spec = TABLES[table]
    pk, cols = spec["pk"], spec["cols"]
    absent = [c for c in cols if c not in df.columns]
    if absent:
        raise LoadError(f"{table}: handed-over data has no column(s): {', '.join(absent)}")
    use_pk = pk in df.columns
    columns = ([pk] if use_pk else []) + cols
    rows = _rows(df, columns)
    before = count_rows(con, table)
    log.debug("%s: %d row(s) before the load, key columns %s", table, before, columns)

    if use_pk:   # UPSERT on the primary key
        updates = ", ".join(f"{c} = excluded.{c}" for c in cols)
        sql = (f"INSERT INTO {table} ({', '.join(columns)}) "
               f"VALUES ({', '.join('?' * len(columns))}) "
               f"ON CONFLICT({pk}) DO UPDATE SET {updates}")
        con.executemany(sql, rows)
        mode = f"upsert on {pk}"
    else:        # insert only when no identical row exists yet
        sql = (f"INSERT INTO {table} ({', '.join(cols)}) "
               f"SELECT {', '.join('?' * len(cols))} "
               f"WHERE NOT EXISTS (SELECT 1 FROM {table} WHERE {_match_sql(table, cols)})")
        con.executemany(sql, [r + r for r in rows])
        mode = "insert if not already present"

    after = count_rows(con, table)
    inserted = after - before
    return {"mode": mode, "rows_received": len(rows), "rows_before": before,
            "rows_after": after, "inserted": inserted,
            "already_present": len(rows) - inserted}


def verify_table(con: sqlite3.Connection, table: str, df: pd.DataFrame, result: dict) -> list[str]:
    """Row-count and presence checks, run before COMMIT. Returns a list of problems."""
    spec = TABLES[table]
    pk, cols = spec["pk"], spec["cols"]
    problems = []
    if result["rows_after"] - result["rows_before"] != result["inserted"]:
        problems.append("row count does not match the inserted count")
    if result["inserted"] < 0 or result["inserted"] > result["rows_received"]:
        problems.append(f"inserted {result['inserted']} of {result['rows_received']} received rows")
    use_pk = pk in df.columns
    columns = [pk] if use_pk else cols
    sql = f"SELECT 1 FROM {table} WHERE {_match_sql(table, columns)} LIMIT 1"
    missing = sum(1 for r in _rows(df, columns) if con.execute(sql, r).fetchone() is None)
    if missing:
        problems.append(f"{missing} of {result['rows_received']} rows not found after the load")
    return problems


# --------------------------------------------------------------------------- #
# One load attempt = one transaction
# --------------------------------------------------------------------------- #
def load_once(frames: dict) -> dict:
    con = connect()
    try:
        init_schema(con)
        check_schema(con)
        con.execute("BEGIN IMMEDIATE")          # take the write lock now (max 1 writer)
        try:
            results = {}
            for table in LOAD_ORDER:
                results[table] = load_table(con, table, frames[table])
                log.info("%s: %s | received %d, inserted %d, already present %d",
                         table, results[table]["mode"], results[table]["rows_received"],
                         results[table]["inserted"], results[table]["already_present"])
            # Verification inside the transaction: a problem means nothing is saved.
            for table in LOAD_ORDER:
                problems = verify_table(con, table, frames[table], results[table])
                if problems:
                    raise LoadError(f"{table} verification failed: {'; '.join(problems)}")
            violations = con.execute("PRAGMA foreign_key_check").fetchall()
            if violations:
                raise LoadError(f"foreign key check found {len(violations)} violation(s), "
                                f"first in table {violations[0][0]}")
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
                log.error("Transaction rolled back; the database was not changed")
            raise
    finally:
        con.close()
    return results


def recount() -> dict:
    """Post-commit check on a fresh connection."""
    con = connect()
    try:
        return {table: count_rows(con, table) for table in LOAD_ORDER}
    finally:
        con.close()


@task(name="load-tables", cache_policy=NO_CACHE, timeout_seconds=TIMEOUT_SECONDS)
def load_tables(frames: dict) -> dict:
    """Load with up to MAX_RETRIES retries, only for a locked/busy database."""
    attempt = 0
    while True:
        attempt += 1
        try:
            return {"tables": load_once(frames), "attempts": attempt}
        except sqlite3.OperationalError as exc:      # database locked / busy / disk I/O
            if attempt > MAX_RETRIES:
                exc.attempts = attempt
                raise
            log.warning("Attempt %d failed (%s); retrying in %ds", attempt, exc,
                        LOAD_RETRY_DELAY_SECONDS)
            time.sleep(LOAD_RETRY_DELAY_SECONDS)


# --------------------------------------------------------------------------- #
# Load run: handover check, log, audit summary
# --------------------------------------------------------------------------- #
def handover_problems(transformed: dict) -> list[str]:
    problems = []
    if transformed.get("status") != "APPROVED":
        problems.append(f"Transform status is {transformed.get('status')}, not APPROVED")
    frames = transformed.get("dataframes") or {}
    for table in LOAD_ORDER:
        df = frames.get(table)
        if not isinstance(df, pd.DataFrame):
            problems.append(f"{table} DataFrame missing")
        elif df.empty:
            problems.append(f"{table} DataFrame is empty")
    if not isinstance(transformed.get("metadata"), dict):
        problems.append("Transform metadata missing")
    return problems


def run_load(transformed: dict) -> dict:
    started = datetime.now()
    t0 = time.monotonic()
    run_id = f"load_{started:%Y%m%d_%H%M%S}"
    meta = transformed.get("metadata") if isinstance(transformed.get("metadata"), dict) else {}

    for directory in (LOGS_DIR, AUDIT_DIR):
        directory.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(LOGS_DIR / f"{run_id}.log", encoding="utf-8")
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    log.setLevel(LOG_LEVEL)
    log.addHandler(handler)
    log.info("Starting %s (v%s) for %s, transform run %s, database %s", run_id, LOAD_VERSION,
             meta.get("batch_id"), meta.get("run_id"), DB_PATH.name)

    status, error_message, error_traceback = "LOADED", "NONE", None
    tables: dict = {}
    attempts = 0
    row_counts: dict = {}

    problems = handover_problems(transformed)
    if problems:
        status, error_message = "FAILED", "INPUT_HANDOVER_FAILED: " + "; ".join(problems)
        log.error(error_message)
    else:
        try:
            outcome = load_tables(transformed["dataframes"])
            tables, attempts = outcome["tables"], outcome["attempts"]
            row_counts = recount()
            for table, counted in row_counts.items():       # post-commit verification
                if counted != tables[table]["rows_after"]:
                    raise LoadError(f"{table}: {counted} rows after commit, "
                                    f"expected {tables[table]['rows_after']}")
        except Exception as exc:    # rolled back (or, for a post-commit mismatch, flagged)
            attempts = getattr(exc, "attempts", attempts or 1)
            status = "FAILED"
            error_message = f"{type(exc).__name__}: {exc}"
            error_traceback = traceback.format_exc()
            log.error("Load of %s (transform run %s) failed: %s",
                      meta.get("batch_id"), meta.get("run_id"), error_message, exc_info=True)

    finished = datetime.now()
    elapsed = time.monotonic() - t0
    audit = {
        "run_id": run_id,
        "transform_run_id": meta.get("run_id"),
        "extraction_run_id": meta.get("parent_run_id"),
        "batch_id": meta.get("batch_id"),
        "batch_window": meta.get("batch_window"),
        "load_version": LOAD_VERSION,
        "database": DB_PATH.name,
        "status": status,
        "attempts": attempts,
        "load_start": started.isoformat(sep=" ", timespec="seconds"),
        "load_end": finished.isoformat(sep=" ", timespec="seconds"),
        "load_duration_seconds": round(elapsed, 1),
        "tables": tables,
        "row_counts_after_commit": row_counts,
        "verification": "PASS" if status == "LOADED" else "FAIL",
        "error_message": error_message,
        "error_traceback": error_traceback,
    }
    (AUDIT_DIR / f"{run_id}.json").write_text(json.dumps(audit, indent=2, default=str),
                                              encoding="utf-8")
    log.info("Run %s finished: %s in %.1fs", run_id, status, elapsed)
    log.removeHandler(handler)
    handler.close()
    return {"status": status, "metadata": audit}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BlockFlow load stage")
    parser.add_argument("--init", action="store_true", help="create missing tables and indexes")
    parser.add_argument("--counts", action="store_true", help="print the row count of each table")
    args = parser.parse_args()
    if not (args.init or args.counts):
        parser.error("use run_pipeline.py for a normal run, or --init / --counts")
    con = connect()
    try:
        if args.init:
            init_schema(con)
            print(f"Tables and indexes ready in {DB_PATH}")
        if args.counts:
            for name in ["users", *LOAD_ORDER]:
                try:
                    print(f"{name:10}{count_rows(con, name)}")
                except sqlite3.Error:
                    print(f"{name:10}(table not found)")
    finally:
        con.close()
