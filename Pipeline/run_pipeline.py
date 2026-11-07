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

from extract import TIMEOUT_SECONDS, extraction_flow
from load import run_load
from pipeline_audit import FAILURE, PipelineAudit
from transform import run_transform


@flow(name="blockflow-pipeline", timeout_seconds=2 * TIMEOUT_SECONDS)
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
            cron="0 0 * * *",
            timezone="Asia/Manila",
            limit=1,  # Max Active Runs: 1
        )
    else:
        outcome = pipeline_flow(args.date, args.accept_warnings)
        print(f"Pipeline: {outcome['pipeline']}  |  Extraction: {outcome['extraction']}  |  "
              f"Transform: {outcome['transform']}  |  Load: {outcome['load']}")
        print(f"Audit record: audit/{outcome['pipeline_run_id']}.json")
        if outcome["pipeline"] == FAILURE:
            raise SystemExit(1)   # a scheduler or script can tell the run failed
