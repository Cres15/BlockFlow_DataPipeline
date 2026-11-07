"""
BlockFlow ETL - daily pipeline: Extract (Stage 1), Transform (Stage 2), Load (Stage 3).

Run once now:      python run_pipeline.py
Re-run a day:      python run_pipeline.py --date 2026-10-20
Owner accepts the Transform warnings of a re-run:
                   python run_pipeline.py --date 2026-10-20 --accept-warnings
Run on schedule:   python run_pipeline.py --serve      (12:00 AM daily, Asia/Manila)

Transform only runs when extraction is APPROVED, and Load only when Transform is APPROVED.
Re-running the same day is safe: Load never duplicates a row that is already in blockflow.db.
"""
from __future__ import annotations

import argparse
from datetime import date

from prefect import flow

from extract import TIMEOUT_SECONDS, extraction_flow
from load import run_load
from transform import run_transform


@flow(name="blockflow-pipeline", timeout_seconds=2 * TIMEOUT_SECONDS)
def pipeline_flow(target_day: date | None = None, accept_warnings: bool = False) -> dict:
    extraction = extraction_flow(target_day)
    if extraction["status"] != "APPROVED":
        # MANUAL REVIEW holds the pipeline; REJECTED stops it. Transform does not run.
        return {"extraction": extraction["status"], "transform": None, "load": None}
    transformed = run_transform(extraction, accept_warnings=accept_warnings)
    if transformed["status"] != "APPROVED":
        return {"extraction": "APPROVED", "transform": transformed["status"], "load": None}
    loaded = run_load(transformed)
    return {"extraction": "APPROVED", "transform": "APPROVED", "load": loaded["status"],
            "result": loaded}


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
        print(f"Extraction: {outcome['extraction']}  |  Transform: {outcome['transform']}  |  Load: {outcome['load']}")
