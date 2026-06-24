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

    # Log database connection target (excluding credentials) for debugging
    try:
        import os
        from urllib.parse import urlparse
        print(f"[{datetime.now().isoformat()}] Available env keys: {sorted(list(os.environ.keys()))}")
        
        # Check raw os.environ value before loading any config/dotenv
        raw_before = os.environ.get("DATABASE_URL", "NOT_SET")
        if raw_before != "NOT_SET":
            parsed_before = urlparse(raw_before)
            print(f"[{datetime.now().isoformat()}] Raw env DATABASE_URL host (BEFORE config load): {parsed_before.hostname}:{parsed_before.port or 5432}")
        else:
            print(f"[{datetime.now().isoformat()}] Raw env DATABASE_URL (BEFORE config load): NOT_SET")

        # Test socket-level DNS resolution
        import socket
        for test_host in ["google.com", "aws-0-us-west-2.pooler.supabase.com", "db.gmmykegxgdtheqjvaufu.supabase.co"]:
            try:
                addr_info = socket.getaddrinfo(test_host, 80)
                resolved_ips = list(set([info[4][0] for info in addr_info]))
                print(f"[{datetime.now().isoformat()}] DNS Resolve {test_host} -> {resolved_ips}")
            except Exception as se:
                print(f"[{datetime.now().isoformat()}] DNS Resolve FAILED for {test_host}: {se}")

        from app.config import get_settings
        settings = get_settings()
        parsed = urlparse(settings.database_url)
        print(f"[{datetime.now().isoformat()}] Database host target (AFTER config load): {parsed.hostname}:{parsed.port or 5432}")
        
        # Check raw os.environ value after config/dotenv load
        raw_after = os.environ.get("DATABASE_URL", "NOT_SET")
        if raw_after != "NOT_SET":
            parsed_after = urlparse(raw_after)
            print(f"[{datetime.now().isoformat()}] Raw env DATABASE_URL host (AFTER config load): {parsed_after.hostname}:{parsed_after.port or 5432}")
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Error during database URL check: {e}")

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
