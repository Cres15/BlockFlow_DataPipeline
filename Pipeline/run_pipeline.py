"""
BlockFlow ETL - daily pipeline: Extract (Stage 1) then Transform (Stage 2).

Run once now:      python run_pipeline.py
Re-run a day:      python run_pipeline.py --date 2026-10-20
Run on schedule:   python run_pipeline.py --serve      (12:00 AM daily, Asia/Manila)

Transform only runs when extraction is APPROVED. The Load stage will be added after it.
"""
from __future__ import annotations

import argparse
from datetime import date

from prefect import flow

from extract import TIMEOUT_SECONDS, extraction_flow
from transform import run_transform


@flow(name="blockflow-pipeline", timeout_seconds=2 * TIMEOUT_SECONDS)
def pipeline_flow(target_day: date | None = None) -> dict:
    extraction = extraction_flow(target_day)
    if extraction["status"] != "APPROVED":
        # MANUAL REVIEW holds the pipeline; REJECTED stops it. Transform does not run.
        return {"extraction": extraction["status"], "transform": None}
    transformed = run_transform(extraction)
    return {"extraction": "APPROVED", "transform": transformed["status"], "result": transformed}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BlockFlow daily pipeline")
    parser.add_argument("--serve", action="store_true",
                        help="serve on the 12:00 AM daily schedule (max 1 active run)")
    parser.add_argument("--date", type=date.fromisoformat,
                        help="run a specific YYYY-MM-DD instead of yesterday")
    args = parser.parse_args()

    if args.serve:
        pipeline_flow.serve(
            name="blockflow-pipeline-daily",
            cron="0 0 * * *",
            timezone="Asia/Manila",
            limit=1,  # Max Active Runs: 1
        )
    else:
        outcome = pipeline_flow(args.date)
        print(f"Extraction: {outcome['extraction']}  |  Transform: {outcome['transform']}")
