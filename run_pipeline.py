#!/usr/bin/env python3
"""
Render Cron Job Entrypoint — NSE Daily Data Pipeline

This script is the single entrypoint for Render Cron Jobs.
Render will execute this script on the configured schedule.

Usage:
    python run_pipeline.py                  # Run the daily pipeline
    python run_pipeline.py --backfill 2024-01-01 2024-06-30  # Backfill a date range

Render Cron Job command:
    cd /app/backend && python run_pipeline.py
"""

import sys
import argparse
from datetime import datetime


def main():
    parser = argparse.ArgumentParser(description="NSE Data Pipeline Runner")
    parser.add_argument(
        "--backfill",
        nargs=2,
        metavar=("START_DATE", "END_DATE"),
        help="Backfill data for a date range (YYYY-MM-DD YYYY-MM-DD)",
    )
    args = parser.parse_args()

    print(f"[{datetime.now().isoformat()}] Pipeline runner started")

    try:
        if args.backfill:
            start_date, end_date = args.backfill
            print(f"[{datetime.now().isoformat()}] Running backfill: {start_date} → {end_date}")
            from app.scheduler.jobs import backfill_data
            backfill_data(start_date, end_date)
        else:
            print(f"[{datetime.now().isoformat()}] Running daily pipeline")
            from app.scheduler.jobs import run_daily_pipeline
            run_daily_pipeline()

        print(f"[{datetime.now().isoformat()}] Pipeline completed successfully")
        sys.exit(0)

    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Pipeline FAILED: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
