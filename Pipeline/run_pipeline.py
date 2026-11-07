"""
BlockFlow ETL - daily pipeline: Extract (Stage 1), Transform (Stage 2), Load (Stage 3).

Run once now:      python run_pipeline.py
Re-run a day:      python run_pipeline.py --date 2026-10-20
Owner accepts the Transform warnings of a re-run:
                   python run_pipeline.py --date 2026-10-20 --accept-warnings
Run on schedule:   python run_pipeline.py --serve      (12:00 AM daily, Asia/Manila)

Transform only runs when extraction is APPROVED, and Load only when Transform is APPROVED.
Re-running the same day is safe: Load never duplicates a row that is already in blockflow.db.

Every run is followed by pipeline_audit.PipelineAudit: one JSON-lines log, one audit file
(audit/pipeline_<timestamp>.json) and rows in the pipeline_runs / pipeline_stage_runs
tables of blockflow.db. Run status: SUCCESS, PARTIAL SUCCESS or FAILURE.
"""
from __future__ import annotations

import argparse
from datetime import date

from prefect import flow
from prefect.client.schemas.objects import ConcurrencyLimitConfig, ConcurrencyLimitStrategy

from extract import MAX_RETRIES, RETRY_DELAY_SECONDS, TIMEOUT_SECONDS, extraction_flow
from load import run_load
from pipeline_audit import FAILURE, PipelineAudit
from transform import run_transform


# Issue 5 settings
SCHEDULE_CRON = "0 0 * * *"           # every day at 12:00 AM
SCHEDULE_TIMEZONE = "Asia/Manila"
MAX_ACTIVE_RUNS = 1                   # one pipeline run at a time (SQLite single writer)
PIPELINE_TIMEOUT_SECONDS = TIMEOUT_SECONDS   # 30-minute limit on the whole flow
# Task-level retries (MAX_RETRIES attempts of retry_delay_seconds each) are set on the tasks:
#   extract.py   locate_batch_file / extract_domain
#   transform.py transform_tables
#   load.py      load_tables (retries when the database is locked/busy)


@flow(name="blockflow-pipeline", timeout_seconds=PIPELINE_TIMEOUT_SECONDS)
def pipeline_flow(target_day: date | None = None, accept_warnings: bool = False) -> dict:
    audit = PipelineAudit(target_day)
    outcome = {"extraction": None, "transform": None, "load": None}
    try:
        extraction = audit.run_stage("extraction", extraction_flow, target_day)
        outcome["extraction"] = extraction["status"]
        if extraction["status"] == "APPROVED":
            transformed = audit.run_stage("transform", run_transform, extraction,
                                          accept_warnings=accept_warnings)
            outcome["transform"] = transformed["status"]
            if transformed["status"] == "APPROVED":
                rows = sum(len(df) for df in transformed["dataframes"].values())
                loaded = audit.run_stage("load", run_load, transformed, records_in=rows)
                outcome["load"] = loaded["status"]
        # MANUAL REVIEW holds the pipeline and REJECTED stops it: later stages do not run.
    except BaseException:
        audit.finish()          # the audit record is written even when a stage raised
        raise
    summary = audit.finish()
    return {**outcome, "pipeline": summary["status"], "pipeline_run_id": summary["run_id"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BlockFlow daily pipeline")
    parser.add_argument("--serve", action="store_true",
                        help="serve on the 12:00 AM daily schedule (max 1 active run)")
    parser.add_argument("--date", type=date.fromisoformat,
                        help="run a specific YYYY-MM-DD instead of yesterday")
    parser.add_argument("--accept-warnings", action="store_true",
                        help="Owner accepts non-critical Transform warnings for this run")
    args = parser.parse_args()

    if args.serve:
        pipeline_flow.serve(
            name="blockflow-pipeline-daily",
            cron=SCHEDULE_CRON,
            timezone=SCHEDULE_TIMEZONE,
            limit=MAX_ACTIVE_RUNS,   # at most 1 run executing in this process
            global_limit=ConcurrencyLimitConfig(      # at most 1 active run of this deployment
                limit=MAX_ACTIVE_RUNS,
                collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW,
            ),
        )
    else:
        outcome = pipeline_flow(args.date, args.accept_warnings)
        print(f"Pipeline: {outcome['pipeline']}  |  Extraction: {outcome['extraction']}  |  "
              f"Transform: {outcome['transform']}  |  Load: {outcome['load']}")
        print(f"Audit record: audit/{outcome['pipeline_run_id']}.json")
        if outcome["pipeline"] == FAILURE:
            raise SystemExit(1)   # a scheduler or script can tell the run failed
