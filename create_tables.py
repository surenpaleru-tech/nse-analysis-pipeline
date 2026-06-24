#!/usr/bin/env python3
"""
Database Setup & Seeding Script — NSE Analysis Pipeline

This script:
1. Connects to your database (via the DATABASE_URL environment variable).
2. Creates all tables defined in the SQLAlchemy models.
3. Seeds/updates the F&O stocks and indices dynamically in 'fno_universe' table.
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


async def main():
    print("Connecting to database and creating tables...")
    try:
        # Step 1: Create all tables defined in SQLAlchemy metadata
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        print("Schema and tables created successfully!")

        # Step 2: Fetch and sync F&O universe dynamically
        print("\nFetching current F&O universe (indices and stocks) from NSE...")
        from app.ingestion.nse_scraper import NSEScraper
        scraper = NSEScraper()
        
        try:
            fno_symbols = await scraper.fetch_fno_symbols()
            if fno_symbols:
                print(f"Found {len(fno_symbols)} F&O symbols in NSE market lots. Syncing database...")
                async with async_session_factory() as db:
                    added_count = 0
                    updated_count = 0
                    deactivated_count = 0
                    
                    # Create a set of fetched symbols to check which ones to deactivate
                    fetched_symbols = {item["symbol"] for item in fno_symbols}
                    
                    # Process each fetched symbol
                    for item in fno_symbols:
                        symbol = item["symbol"]
                        stmt = select(FnOUniverse).where(FnOUniverse.symbol == symbol)
                        result = await db.execute(stmt)
                        existing = result.scalars().first()
                        
                        if not existing:
                            new_symbol = FnOUniverse(
                                symbol=symbol,
                                instrument_type=item["instrument_type"],
                                lot_size=item["lot_size"],
                                is_active=True,
                                added_date=date.today()
                            )
                            db.add(new_symbol)
                            added_count += 1
                        else:
                            # Update if lot size, activity, or type changed
                            changed = False
                            if existing.lot_size != item["lot_size"]:
                                print(f" -> {symbol}: Lot size changed {existing.lot_size} -> {item['lot_size']}")
                                existing.lot_size = item["lot_size"]
                                changed = True
                            if not existing.is_active:
                                print(f" -> {symbol}: Re-activating symbol")
                                existing.is_active = True
                                existing.removed_date = None
                                changed = True
                            if existing.instrument_type != item["instrument_type"]:
                                print(f" -> {symbol}: Instrument type changed {existing.instrument_type} -> {item['instrument_type']}")
                                existing.instrument_type = item["instrument_type"]
                                changed = True
                                
                            if changed:
                                updated_count += 1
                                
                    # Deactivate active symbols in the DB that are not in the fetched list
                    stmt_active = select(FnOUniverse).where(FnOUniverse.is_active == True)
                    result_active = await db.execute(stmt_active)
                    db_active_symbols = result_active.scalars().all()
                    
                    for db_item in db_active_symbols:
                        if db_item.symbol not in fetched_symbols:
                            print(f" -> {db_item.symbol}: Deactivating (no longer in F&O universe)")
                            db_item.is_active = False
                            db_item.removed_date = date.today()
                            deactivated_count += 1
                            
                    await db.commit()
                print(f" -> Sync complete: Added {added_count}, Updated {updated_count}, Deactivated {deactivated_count} symbols.")
            else:
                print(" -> No symbols retrieved from NSE. Skipping database sync to protect existing data.")
        except Exception as e:
            print(f" -> Error during F&O universe sync: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
        finally:
            await scraper.close()

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
