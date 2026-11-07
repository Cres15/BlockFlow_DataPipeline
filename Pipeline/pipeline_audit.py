"""
BlockFlow ETL - Pipeline logging and audit records (Issue 4), version 1.0.0

One PipelineAudit object follows a whole run (Extraction -> Transformation -> Load):

  Structured logging   Every stage logs with the same line format and levels
                       (DEBUG, INFO, WARNING, ERROR; set BLOCKFLOW_LOG_LEVEL).
                       This module also writes ALL of a run's log records, from every
                       stage, as JSON lines:  logs/pipeline_<timestamp>.jsonl
  Execution audit      start and end time, total duration and the status of the run:
                       SUCCESS, PARTIAL SUCCESS or FAILURE.
  Batch and record     per stage: records in, passed, rejected, validation checks.
  metrics
  Error traceability   error message, stack trace and the batch/file identifiers of
                       any exception or failed check.
  Audit storage        audit/pipeline_<timestamp>.json   (one file per run)
                       and two tracking tables in blockflow.db:
                       pipeline_runs and pipeline_stage_runs (history and reports).

Status of a run
  FAILURE          a stage raised an exception, or a stage ended REJECTED / FAILED.
  PARTIAL SUCCESS  nothing failed, but the run is not clean: the batch is held for
                   MANUAL REVIEW (nothing loaded yet), records were rejected, or the
                   Owner accepted warnings.
  SUCCESS          every stage finished, no record was rejected, no warning accepted.

See the history of past runs:   python pipeline_audit.py --history 10
See the stages of one run:      python pipeline_audit.py --run pipeline_20261108_000000
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import time
import traceback
from collections import Counter
from datetime import date, datetime, timedelta

from extract import AUDIT_DIR, LOG_LEVEL, LOGS_DIR  # first: sets up Prefect log capture
from load import DB_PATH

AUDIT_VERSION = "1.0.0"
SUCCESS, PARTIAL, FAILURE = "SUCCESS", "PARTIAL SUCCESS", "FAILURE"
STAGES = ["extraction", "transform", "load"]

# stage result status -> ok / held for review / failed
OUTCOME = {"APPROVED": "ok", "LOADED": "ok", "MANUAL REVIEW": "held",
           "REJECTED": "failed", "FAILED": "failed"}
# metadata keys holding each stage's own start, end and duration
TIME_KEYS = {"extraction": ("extraction_start", "extraction_end", "extraction_duration_seconds"),
             "transform": ("transformation_start", "transformation_end", "transformation_duration_seconds"),
             "load": ("load_start", "load_end", "load_duration_seconds")}

log = logging.getLogger("blockflow.pipeline")

TRACKING_SQL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id              TEXT PRIMARY KEY,
    trigger             TEXT,
    batch_id            TEXT,
    batch_window        TEXT,
    started_at          DATETIME NOT NULL,
    finished_at         DATETIME NOT NULL,
    duration_seconds    REAL,
    status              TEXT NOT NULL,
    status_detail       TEXT,
    records_extracted   INTEGER,
    records_rejected    INTEGER,
    records_transformed INTEGER,
    records_loaded      INTEGER,
    error_message       TEXT,
    log_file            TEXT,
    audit_version       TEXT
);
CREATE TABLE IF NOT EXISTS pipeline_stage_runs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id              TEXT NOT NULL REFERENCES pipeline_runs (run_id),
    stage               TEXT NOT NULL,
    stage_run_id        TEXT,
    status              TEXT NOT NULL,
    started_at          DATETIME,
    finished_at         DATETIME,
    duration_seconds    REAL,
    records_in          INTEGER,
    records_passed      INTEGER,
    records_rejected    INTEGER,
    checks_passed       INTEGER,
    checks_warning      INTEGER,
    checks_failed       INTEGER,
    error_message       TEXT
);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started ON pipeline_runs (started_at);
CREATE INDEX IF NOT EXISTS idx_pipeline_stage_runs_run ON pipeline_stage_runs (run_id);
"""


# --------------------------------------------------------------------------- #
# Structured (JSON lines) log
# --------------------------------------------------------------------------- #
class JsonLinesFormatter(logging.Formatter):
    """One JSON object per log record, with the stack trace when there is one."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(sep=" ", timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "stage": record.name.rsplit(".", 1)[-1] if record.name.startswith("blockflow.") else None,
            "pipeline_run_id": getattr(record, "pipeline_run_id", None),
            "batch_id": getattr(record, "batch_id", None),
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            entry["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": "".join(traceback.format_exception(*record.exc_info)),
            }
        return json.dumps(entry, ensure_ascii=False, default=str)


class _RunContext(logging.Filter):
    """Adds the pipeline run ID and batch ID to every record of this run."""

    def __init__(self, audit: "PipelineAudit") -> None:
        super().__init__()
        self.audit = audit

    def filter(self, record: logging.LogRecord) -> bool:
        record.pipeline_run_id = self.audit.run_id
        record.batch_id = self.audit.batch_id
        return True


# --------------------------------------------------------------------------- #
# Reading a stage result
# --------------------------------------------------------------------------- #
def _sum(values) -> int:
    return int(sum(values.values())) if isinstance(values, dict) else 0


def _check_counts(results) -> dict:
    counts = Counter(r.get("status") for r in (results or []))
    return {"total": sum(counts.values()), "passed": counts["PASS"], "warnings": counts["WARNING"],
            "failed": counts["FAIL"], "skipped": counts["SKIPPED"]}


def _failed_checks(stage: str, meta: dict, files: dict | None = None) -> list[dict]:
    errors = []
    for c in meta.get("validation_results") or []:
        if c.get("status") == "FAIL":
            domain = c.get("domain")
            entry = {"type": "VALIDATION_FAIL", "stage": stage, "check": c.get("check"),
                     "domain": domain, "message": c.get("detail"),
                     "batch_id": meta.get("batch_id")}
            if files and domain in files:
                entry["file"] = files[domain]
            errors.append(entry)
    return errors


def summarize_stage(stage: str, result: dict, records_in: int | None = None) -> dict:
    """Turn what a stage returned into one audit record (metrics, checks, errors)."""
    meta = result.get("metadata") if isinstance(result.get("metadata"), dict) else {}
    status = result.get("status", "UNKNOWN")
    tables: dict = {}
    extra: dict = {}

    if stage == "extraction":
        stage_run_id = meta.get("pipeline_run_id")
        files = meta.get("files") or {}
        read, ok, rej = (meta.get(k) or {} for k in ("records_read", "records_extracted", "records_rejected"))
        for d in sorted(set(read) | set(ok) | set(rej)):
            tables[d] = {"records_in": read.get(d, 0), "records_passed": ok.get(d, 0),
                         "records_rejected": rej.get(d, 0)}
        total_in, passed, rejected = _sum(read), _sum(ok), _sum(rej)
        errors = _failed_checks(stage, meta, files)
        extra["files"] = files
    elif stage == "transform":
        stage_run_id = meta.get("run_id")
        for d, t in (meta.get("tables") or {}).items():
            tables[d] = {"records_in": t.get("records_read", 0), "records_passed": t.get("records_transformed", 0),
                         "records_rejected": t.get("records_rejected", 0)}
        total_in = sum(t["records_in"] for t in tables.values())
        passed = sum(t["records_passed"] for t in tables.values())
        rejected = sum(t["records_rejected"] for t in tables.values())
        errors = _failed_checks(stage, meta)
        extra["warnings_accepted"] = bool(meta.get("warnings_accepted"))
    else:  # load
        stage_run_id = meta.get("run_id")
        for d, t in (meta.get("tables") or {}).items():
            tables[d] = {"records_in": t.get("rows_received", 0),
                         "records_passed": t.get("inserted", 0) + t.get("already_present", 0),
                         "records_rejected": 0, "records_inserted": t.get("inserted", 0),
                         "records_already_present": t.get("already_present", 0)}
        total_in = sum(t["records_in"] for t in tables.values()) or (records_in or 0)
        passed = sum(t["records_passed"] for t in tables.values())
        rejected = 0 if tables else total_in          # a failed load saved nothing
        errors = []
        if meta.get("error_message") not in (None, "NONE"):
            errors.append({"type": "LOAD_ERROR", "stage": stage, "message": meta["error_message"],
                           "batch_id": meta.get("batch_id"), "traceback": meta.get("error_traceback")})
        extra["records_inserted"] = sum(t["records_inserted"] for t in tables.values())
        extra["records_already_present"] = sum(t["records_already_present"] for t in tables.values())

    if OUTCOME.get(status) == "failed" and not errors and meta.get("error_message") not in (None, "NONE"):
        errors.append({"type": "STAGE_ERROR", "stage": stage, "message": meta["error_message"],
                       "batch_id": meta.get("batch_id")})

    start_key, end_key, dur_key = TIME_KEYS[stage]
    return {
        "stage": stage, "stage_run_id": stage_run_id, "status": status,
        "outcome": OUTCOME.get(status, "failed"),
        "start": meta.get(start_key), "end": meta.get(end_key), "duration_seconds": meta.get(dur_key),
        "records_in": total_in, "records_passed": passed, "records_rejected": rejected,
        "tables": tables, "checks": _check_counts(meta.get("validation_results")),
        "errors": errors, **extra,
    }


# --------------------------------------------------------------------------- #
# The run
# --------------------------------------------------------------------------- #
class PipelineAudit:
    def __init__(self, target_day: date | None = None, trigger: str = "pipeline",
                 window: str | None = None) -> None:
        self.started = datetime.now()
        self._t0 = time.monotonic()
        self.run_id = f"pipeline_{self.started:%Y%m%d_%H%M%S}"
        self.trigger = trigger
        self.window = window or (target_day or date.today() - timedelta(days=1)).isoformat()
        self.stages: list[dict] = []
        self.finished = False
        self.summary: dict | None = None

        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        AUDIT_DIR.mkdir(parents=True, exist_ok=True)
        self.log_path = LOGS_DIR / f"{self.run_id}.jsonl"
        self._handler = logging.FileHandler(self.log_path, encoding="utf-8")
        self._handler.setLevel(logging.DEBUG)
        self._handler.setFormatter(JsonLinesFormatter())
        self._handler.addFilter(_RunContext(self))
        root = logging.getLogger("blockflow")
        root.setLevel(LOG_LEVEL)
        log.setLevel(LOG_LEVEL)
        root.addHandler(self._handler)
        log.info("Pipeline run %s started (%s) for %s", self.run_id, trigger, self.batch_id)

    @property
    def batch_id(self) -> str:
        return "BATCH_" + str(self.window).replace("-", "")

    # ---- recording stages -------------------------------------------------- #
    def record_stage(self, stage: str, result: dict, records_in: int | None = None,
                     status: str | None = None, outcome: str | None = None,
                     note: str | None = None) -> dict:
        record = summarize_stage(stage, result, records_in)
        for error in record["errors"]:
            error["batch_id"] = error.get("batch_id") or self.batch_id
        if status:
            record["status"] = status
        if outcome:
            record["outcome"] = outcome
        if note:
            record["note"] = note
        self.stages.append(record)
        log.info("Stage %s: %s | in %d, passed %d, rejected %d | checks %s", stage, record["status"],
                 record["records_in"], record["records_passed"], record["records_rejected"],
                 record["checks"])
        return record

    def record_exception(self, stage: str, started: datetime, exc: BaseException,
                         records_in: int | None = None) -> dict:
        ended = datetime.now()
        record = {
            "stage": stage, "stage_run_id": None, "status": "EXCEPTION", "outcome": "failed",
            "start": started.isoformat(sep=" ", timespec="seconds"),
            "end": ended.isoformat(sep=" ", timespec="seconds"),
            "duration_seconds": round((ended - started).total_seconds(), 1),
            "records_in": records_in or 0, "records_passed": 0, "records_rejected": records_in or 0,
            "tables": {}, "checks": _check_counts([]),
            "errors": [{"type": "EXCEPTION", "stage": stage, "exception": type(exc).__name__,
                        "message": str(exc), "batch_id": self.batch_id,
                        "traceback": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))}],
        }
        self.stages.append(record)
        log.error("Stage %s raised %s for %s: %s", stage, type(exc).__name__, self.batch_id, exc,
                  exc_info=(type(exc), exc, exc.__traceback__))
        return record

    def run_stage(self, stage: str, fn, *args, records_in: int | None = None, **kwargs) -> dict:
        """Run one stage, record its metrics, and record (then re-raise) any exception."""
        started = datetime.now()
        log.info("Stage %s starting", stage)
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:
            self.record_exception(stage, started, exc, records_in)
            raise
        self.record_stage(stage, result, records_in)
        return result

    # ---- finishing ----------------------------------------------------------- #
    def finish(self) -> dict:
        """Work out the run status, write the audit records, close the log. Safe to call twice."""
        if self.finished and self.summary is not None:
            return self.summary
        self.finished = True
        ended = datetime.now()
        duration = round(time.monotonic() - self._t0, 1)

        done = {s["stage"] for s in self.stages}
        for stage in STAGES:                       # stages that never ran are still recorded
            if stage not in done:
                self.stages.append({"stage": stage, "stage_run_id": None, "status": "NOT RUN",
                                    "outcome": "skipped", "start": None, "end": None,
                                    "duration_seconds": None, "records_in": 0, "records_passed": 0,
                                    "records_rejected": 0, "tables": {}, "checks": _check_counts([]),
                                    "errors": []})
        self.stages.sort(key=lambda s: STAGES.index(s["stage"]))
        by = {s["stage"]: s for s in self.stages}

        rejected_total = sum(s["records_rejected"] for s in self.stages if s["stage"] != "load")
        failed = [s for s in self.stages if s["outcome"] == "failed"]
        held = [s for s in self.stages if s["outcome"] == "held"]
        warned = any(s.get("warnings_accepted") for s in self.stages)
        if failed or not done:
            status, detail = FAILURE, f"{(failed[0]['stage'] if failed else 'pipeline').upper()}_{(failed[0]['status'] if failed else 'NO_STAGE_RAN').replace(' ', '_')}"
        elif held:
            status, detail = PARTIAL, f"HELD_FOR_REVIEW:{held[0]['stage']}"
        elif rejected_total or warned:
            status = PARTIAL
            detail = f"RECORDS_REJECTED:{rejected_total}" if rejected_total else "WARNINGS_ACCEPTED"
        else:
            status, detail = SUCCESS, "NONE"

        errors = [e for s in self.stages for e in s["errors"]]
        summary = {
            "run_id": self.run_id, "audit_version": AUDIT_VERSION, "trigger": self.trigger,
            "batch_id": self.batch_id, "batch_window": self.window,
            "pipeline_start": self.started.isoformat(sep=" ", timespec="seconds"),
            "pipeline_end": ended.isoformat(sep=" ", timespec="seconds"),
            "duration_seconds": duration, "status": status, "status_detail": detail,
            "totals": {
                "records_extracted": by["extraction"]["records_passed"],
                "records_rejected": rejected_total,
                "records_transformed": by["transform"]["records_passed"],
                "records_loaded": by["load"].get("records_inserted", 0),
                "records_already_present": by["load"].get("records_already_present", 0),
            },
            "stages": self.stages,
            "error_message": errors[0]["message"] if failed and errors else "NONE",
            "errors": errors,
            "log_file": self.log_path.name,
        }
        summary["audit_storage"] = {"json": f"{self.run_id}.json", "sqlite": self._write_sqlite(summary)}
        log.log(logging.INFO if status == SUCCESS else logging.WARNING if status == PARTIAL else logging.ERROR,
                "Pipeline run %s finished: %s (%s) in %.1fs | audit storage: sqlite %s",
                self.run_id, status, detail, duration, summary["audit_storage"]["sqlite"])
        (AUDIT_DIR / f"{self.run_id}.json").write_text(
            json.dumps(summary, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        logging.getLogger("blockflow").removeHandler(self._handler)
        self._handler.close()
        self.summary = summary
        return summary

    def _write_sqlite(self, s: dict) -> str:
        """Save the run in the tracking tables. A problem here never stops the pipeline."""
        try:
            DB_PATH.parent.mkdir(parents=True, exist_ok=True)
            con = sqlite3.connect(DB_PATH, timeout=30)
            try:
                con.executescript(TRACKING_SQL)
                t = s["totals"]
                with con:   # one transaction: commit, or roll back on error
                    con.execute("DELETE FROM pipeline_stage_runs WHERE run_id = ?", (s["run_id"],))
                    con.execute(
                        "INSERT OR REPLACE INTO pipeline_runs (run_id, trigger, batch_id, batch_window, "
                        "started_at, finished_at, duration_seconds, status, status_detail, "
                        "records_extracted, records_rejected, records_transformed, records_loaded, "
                        "error_message, log_file, audit_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (s["run_id"], s["trigger"], s["batch_id"], s["batch_window"], s["pipeline_start"],
                         s["pipeline_end"], s["duration_seconds"], s["status"], s["status_detail"],
                         t["records_extracted"], t["records_rejected"], t["records_transformed"],
                         t["records_loaded"], s["error_message"], s["log_file"], s["audit_version"]))
                    con.executemany(
                        "INSERT INTO pipeline_stage_runs (run_id, stage, stage_run_id, status, started_at, "
                        "finished_at, duration_seconds, records_in, records_passed, records_rejected, "
                        "checks_passed, checks_warning, checks_failed, error_message) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        [(s["run_id"], st["stage"], st["stage_run_id"], st["status"], st["start"], st["end"],
                          st["duration_seconds"], st["records_in"], st["records_passed"],
                          st["records_rejected"], st["checks"]["passed"], st["checks"]["warnings"],
                          st["checks"]["failed"], st["errors"][0]["message"] if st["errors"] else None)
                         for st in s["stages"]])
            finally:
                con.close()
            return "OK"
        except Exception as exc:                      # noqa: BLE001 - audit must not break the run
            log.error("Could not save the audit record to %s: %s", DB_PATH.name, exc, exc_info=True)
            return f"FAILED: {exc}"


# --------------------------------------------------------------------------- #
# History and reports
# --------------------------------------------------------------------------- #
def show_history(limit: int) -> None:
    if not DB_PATH.is_file():
        raise SystemExit(f"No database yet: {DB_PATH}")
    con = sqlite3.connect(f"{DB_PATH.as_uri()}?mode=ro", uri=True)
    try:
        rows = con.execute(
            "SELECT run_id, batch_id, status, duration_seconds, records_extracted, records_rejected, "
            "records_loaded FROM pipeline_runs ORDER BY started_at DESC, run_id DESC LIMIT ?", (limit,)).fetchall()
    except sqlite3.Error:
        raise SystemExit("No pipeline runs recorded yet.")
    finally:
        con.close()
    print(f"{'run_id':<26}{'batch':<16}{'status':<17}{'secs':>6}{'extracted':>11}{'rejected':>10}{'loaded':>8}")
    for r in rows:
        print(f"{r[0]:<26}{r[1] or '':<16}{r[2]:<17}{r[3] or 0:>6}{r[4] or 0:>11}{r[5] or 0:>10}{r[6] or 0:>8}")


def show_run(run_id: str) -> None:
    if not DB_PATH.is_file():
        raise SystemExit(f"No database yet: {DB_PATH}")
    con = sqlite3.connect(f"{DB_PATH.as_uri()}?mode=ro", uri=True)
    try:
        rows = con.execute(
            "SELECT stage, status, duration_seconds, records_in, records_passed, records_rejected, "
            "checks_passed, checks_warning, checks_failed, error_message FROM pipeline_stage_runs "
            "WHERE run_id = ? ORDER BY id", (run_id,)).fetchall()
    except sqlite3.Error:
        raise SystemExit("No pipeline runs recorded yet.")
    finally:
        con.close()
    if not rows:
        raise SystemExit(f"No run found: {run_id}")
    print(f"{'stage':<12}{'status':<24}{'secs':>6}{'in':>6}{'passed':>8}{'rejected':>10}  checks P/W/F  error")
    for r in rows:
        print(f"{r[0]:<12}{r[1]:<24}{r[2] or 0:>6}{r[3] or 0:>6}{r[4] or 0:>8}{r[5] or 0:>10}  "
              f"{r[6]}/{r[7]}/{r[8]}  {r[9] or ''}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BlockFlow pipeline audit history")
    parser.add_argument("--history", type=int, metavar="N", help="show the last N pipeline runs")
    parser.add_argument("--run", metavar="RUN_ID", help="show the stages of one pipeline run")
    args = parser.parse_args()
    if args.history:
        show_history(args.history)
    elif args.run:
        show_run(args.run)
    else:
        parser.error("use --history N or --run RUN_ID")
