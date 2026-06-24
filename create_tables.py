#!/usr/bin/env python3
"""
Database Setup & Seeding Script — NSE Analysis Pipeline

This script:
1. Connects to your database (via the DATABASE_URL environment variable).
2. Creates all tables defined in the SQLAlchemy models.
3. Seeds the initial active F&O stocks and indices into the 'fno_universe' table.
"""

import asyncio
import sys
from datetime import date
from sqlalchemy import select

# Import database engine and metadata base
from app.core.database import engine, Base, async_session_factory
from app.models import FnOUniverse

# Import all models to ensure they are registered with Base.metadata
import app.models  # noqa: F401


# Standard NSE indices and major F&O stocks to seed the universe
INITIAL_SEED_DATA = [
    # Indices
    {"symbol": "NIFTY", "instrument_type": "index", "lot_size": 25, "is_active": True},
    {"symbol": "BANKNIFTY", "instrument_type": "index", "lot_size": 15, "is_active": True},
    {"symbol": "FINNIFTY", "instrument_type": "index", "lot_size": 40, "is_active": True},
    {"symbol": "MIDCPNIFTY", "instrument_type": "index", "lot_size": 75, "is_active": True},
    {"symbol": "NIFTYNXT50", "instrument_type": "index", "lot_size": 10, "is_active": True},
    # Highly active F&O Stocks (Large Caps)
    {"symbol": "RELIANCE", "instrument_type": "stock", "lot_size": 250, "is_active": True},
    {"symbol": "TCS", "instrument_type": "stock", "lot_size": 175, "is_active": True},
    {"symbol": "INFY", "instrument_type": "stock", "lot_size": 400, "is_active": True},
    {"symbol": "HDFCBANK", "instrument_type": "stock", "lot_size": 550, "is_active": True},
    {"symbol": "ICICIBANK", "instrument_type": "stock", "lot_size": 700, "is_active": True},
]


async def main():
    print("Connecting to database and creating tables...")
    try:
        # Step 1: Create all tables defined in SQLAlchemy metadata
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        print("Schema and tables created successfully!")

        # Step 2: Seed the initial F&O universe
        print("\nSeeding initial active F&O stock and index universe...")
        async with async_session_factory() as db:
            for item in INITIAL_SEED_DATA:
                # Check if symbol already exists
                stmt = select(FnOUniverse).where(FnOUniverse.symbol == item["symbol"])
                result = await db.execute(stmt)
                existing = result.scalars().first()

                if not existing:
                    new_symbol = FnOUniverse(
                        symbol=item["symbol"],
                        instrument_type=item["instrument_type"],
                        lot_size=item["lot_size"],
                        is_active=item["is_active"],
                        added_date=date.today()
                    )
                    db.add(new_symbol)
                    print(f" -> Added {item['symbol']} ({item['instrument_type']}) to F&O universe")
                else:
                    print(f" -> {item['symbol']} already exists, skipping")
            
            await db.commit()
        print("\nDatabase initialization complete! You are ready to run the pipeline.")

    except Exception as e:
        print(f"\nDatabase initialization FAILED: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    # Ensure correct loop policy on Windows if needed, then run
    asyncio.run(main())
