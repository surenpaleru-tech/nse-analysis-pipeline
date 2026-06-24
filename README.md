# NSE Analysis Pipeline

Data ingestion and analytics engine for the NSE Options Intelligence Platform.

This project runs as a **Render Cron Job** to:
1. Scrape NSE bhavcopy data (equity + F&O) daily after market close
2. Collect India VIX data
3. Compute P&L analytics for CE/PE option strategies
4. Generate optimal selling bands and daily recommendations
5. Store everything in Supabase PostgreSQL

## Architecture

```
Render Cron Job (Mon-Fri 7 PM IST)
    └── run_pipeline.py
        └── app/scheduler/jobs.py
            ├── app/ingestion/   (NSE data scraping)
            ├── app/analytics/   (P&L computation)
            └── app/models/      (SQLAlchemy ORM → Supabase)
```

## Setup

1. Copy `.env.example` to `.env` and fill in your `DATABASE_URL`
2. Install dependencies: `pip install -e .`
3. Run the pipeline: `python run_pipeline.py`

### Backfill historical data
```bash
python run_pipeline.py --backfill 2024-01-01 2024-06-30
```

## Render Deployment

1. Create a new **Cron Job** on Render
2. Connect this GitHub repo
3. Set environment to **Docker**
4. Set command: `python run_pipeline.py`
5. Set schedule: `30 13 * * 1-5` (7:00 PM IST, Mon-Fri)
6. Add `DATABASE_URL` environment variable
