#!/usr/bin/env python3
"""
Render Cron Job Entrypoint for the NSE data pipeline.

Usage:
    python run_pipeline.py
    python run_pipeline.py --backfill 2024-01-01 2024-06-30
    python run_pipeline.py --recompute
    python run_pipeline.py --recompute 2026-06-28
"""

import argparse
import sys
from datetime import datetime


def main():
    parser = argparse.ArgumentParser(description="NSE Data Pipeline Runner")
    parser.add_argument(
        "--backfill",
        nargs=2,
        metavar=("START_DATE", "END_DATE"),
        help="Backfill data for a date range (YYYY-MM-DD YYYY-MM-DD)",
    )
    parser.add_argument(
        "--recompute",
        nargs="?",
        const="today",
        metavar="TRADE_DATE",
        help=(
            "Recompute analytics and daily recommendations from existing DB data. "
            "Optionally pass YYYY-MM-DD to control the recommendation date."
        ),
    )
    args = parser.parse_args()

    from app.config import get_settings
    from app.core.logging import setup_logging

    settings = get_settings()
    setup_logging(debug=settings.app_debug)

    print(f"[{datetime.now().isoformat()}] Pipeline runner started")

    try:
        if args.backfill:
            start_date, end_date = args.backfill
            print(
                f"[{datetime.now().isoformat()}] Running backfill: "
                f"{start_date} -> {end_date}"
            )
            from app.scheduler.jobs import backfill_data

            backfill_data(start_date, end_date)
        elif args.recompute is not None:
            trade_date = None if args.recompute == "today" else args.recompute
            print(
                f"[{datetime.now().isoformat()}] Running analytics-only recompute"
                f"{f' for {trade_date}' if trade_date else ''}"
            )
            from app.scheduler.jobs import recompute_analytics

            recompute_analytics(trade_date)
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
