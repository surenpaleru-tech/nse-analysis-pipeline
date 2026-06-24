"""
Scheduled job definitions — daily data pipeline and analytics tasks.

These functions are called directly by run_pipeline.py (triggered via Render Cron Jobs)
instead of Celery. The async functions remain unchanged from the original pipeline logic.
"""

import asyncio
from datetime import date, timedelta

from app.core.logging import get_logger

logger = get_logger(__name__)


def run_daily_pipeline():
    """
    Daily data pipeline — runs after market close.

    Sequence:
    1. Download latest NSE data (bhavcopy + VIX)
    2. Validate data quality
    3. Store in PostgreSQL (deduplication handled by upserts)
    4. Compute analytics for new expiries
    5. Update optimal bands
    6. Generate daily recommendations
    """
    logger.info("Starting daily data pipeline")

    try:
        asyncio.run(_async_daily_pipeline())
        logger.info("Daily pipeline completed successfully")
    except Exception as e:
        logger.error(f"Daily pipeline failed: {e}")
        raise


async def sync_fno_universe(db):
    """Sync the F&O stock and index universe and lot sizes dynamically from NSE."""
    from app.ingestion.nse_scraper import NSEScraper
    from app.models import FnOUniverse
    from datetime import date
    from sqlalchemy import select
    
    scraper = NSEScraper()
    try:
        fno_symbols = await scraper.fetch_fno_symbols()
        if fno_symbols:
            logger.info(f"Syncing F&O universe with {len(fno_symbols)} symbols from NSE...")
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
                        logger.info(f"Updating lot size for {symbol}: {existing.lot_size} -> {item['lot_size']}")
                        existing.lot_size = item["lot_size"]
                        changed = True
                    if not existing.is_active:
                        logger.info(f"Re-activating F&O symbol: {symbol}")
                        existing.is_active = True
                        existing.removed_date = None
                        changed = True
                    if existing.instrument_type != item["instrument_type"]:
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
                    logger.info(f"Deactivating F&O symbol: {db_item.symbol} (no longer in NSE active list)")
                    db_item.is_active = False
                    db_item.removed_date = date.today()
                    deactivated_count += 1
                    
            logger.info(f"F&O universe sync complete: added {added_count}, updated {updated_count}, deactivated {deactivated_count}")
        else:
            logger.warning("No F&O symbols retrieved from NSE. Skipping database sync to protect existing data.")
    except Exception as e:
        logger.error(f"Error during F&O universe sync: {e}")
        raise
    finally:
        await scraper.close()


async def _async_daily_pipeline():
    """Async implementation of the daily pipeline."""
    from app.core.database import async_session_factory
    from app.ingestion.nse_scraper import NSEScraper
    from app.ingestion.option_collector import OptionCollector
    from app.ingestion.spot_collector import SpotCollector
    from app.ingestion.vix_collector import VIXCollector
    from app.ingestion.validator import DataValidator
    from app.models import FnOUniverse
    from sqlalchemy import select

    today = date.today()
    scraper = NSEScraper()
    validator = DataValidator()

    try:
        async with async_session_factory() as db:
            # 1. Sync F&O universe first
            try:
                await sync_fno_universe(db)
                await db.commit()
            except Exception as e:
                logger.error(f"Failed to sync F&O universe before daily run: {e}")
                await db.rollback()

            # 2. Get F&O universe
            fno_query = select(FnOUniverse).where(FnOUniverse.is_active == True)
            fno_result = await db.execute(fno_query)
            fno_symbols = {r.symbol for r in fno_result.scalars().all()}

            logger.info(f"F&O universe: {len(fno_symbols)} symbols")

            # 2. Download equity bhavcopy for spot prices
            eq_df = await scraper.fetch_equity_bhavcopy(today)
            spot_collector = SpotCollector(db)
            spot_prices = {}

            if eq_df is not None:
                valid, issues = validator.validate_bhavcopy(eq_df, today)
                if valid or len(issues) < 3:
                    spot_prices = await spot_collector.process_equity_bhavcopy(
                        eq_df, today, fno_symbols
                    )

            # 3. Download F&O bhavcopy
            fo_df = await scraper.fetch_derivatives_bhavcopy(today)
            if fo_df is not None:
                valid, issues = validator.validate_bhavcopy(fo_df, today)
                if valid or len(issues) < 3:
                    option_collector = OptionCollector(db)
                    await option_collector.process_bhavcopy(fo_df, today, spot_prices)

            # 4. Download VIX
            vix_collector = VIXCollector(db, scraper)
            await vix_collector.collect(today)

            await db.commit()
            logger.info("Data ingestion phase complete")

            # 5. Compute analytics for recently expired contracts
            from app.analytics.pnl_calculator import PnLCalculator
            from app.analytics.band_optimizer import BandOptimizer

            pnl_calc = PnLCalculator(db)
            optimizer = BandOptimizer(db)

            for symbol in fno_symbols:
                instrument_type = "index" if symbol in {
                    "NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"
                } else "stock"

                expiry_types = ["monthly"]
                if instrument_type == "index":
                    expiry_types.append("weekly")

                for expiry_type in expiry_types:
                    try:
                        await pnl_calc.compute_for_symbol(symbol, expiry_type)
                        await optimizer.optimize_for_symbol(
                            symbol, instrument_type, expiry_type
                        )
                    except Exception as e:
                        logger.error(
                            f"Analytics failed for {symbol} ({expiry_type}): {e}"
                        )

            await db.commit()
            logger.info("Analytics computation complete")

            # 6. Generate daily recommendations
            from app.analytics.recommendation_pipeline import DailyRecommendationPipeline
            rec_pipeline = DailyRecommendationPipeline(db)
            rec_count = await rec_pipeline.run(trade_date=today)
            logger.info(f"Generated {rec_count} daily recommendations")

            await db.commit()

    finally:
        await scraper.close()
        from app.core.database import engine
        await engine.dispose()
        logger.info("Database engine connections disposed")


def backfill_data(start_date_str: str, end_date_str: str):
    """Backfill historical data for a date range."""
    logger.info(f"Backfilling data from {start_date_str} to {end_date_str}")
    asyncio.run(_async_backfill(start_date_str, end_date_str))


async def _async_backfill(start_str: str, end_str: str):
    """Async backfill implementation."""
    from app.core.database import async_session_factory
    from app.ingestion.nse_scraper import NSEScraper
    from app.ingestion.option_collector import OptionCollector
    from app.ingestion.spot_collector import SpotCollector
    from app.ingestion.vix_collector import VIXCollector
    from app.ingestion.validator import DataValidator
    from app.models import FnOUniverse
    from sqlalchemy import select

    start = date.fromisoformat(start_str)
    end = date.fromisoformat(end_str)

    scraper = NSEScraper()
    validator = DataValidator()

    try:
        async with async_session_factory() as db:
            fno_query = select(FnOUniverse).where(FnOUniverse.is_active == True)
            fno_result = await db.execute(fno_query)
            fno_symbols = {r.symbol for r in fno_result.scalars().all()}

            current = start
            while current <= end:
                # Skip weekends
                if current.weekday() >= 5:
                    current += timedelta(days=1)
                    continue

                logger.info(f"Backfilling {current}")

                try:
                    # Equity bhavcopy
                    eq_df = await scraper.fetch_equity_bhavcopy(current)
                    spot_collector = SpotCollector(db)
                    spot_prices = {}

                    if eq_df is not None:
                        spot_prices = await spot_collector.process_equity_bhavcopy(
                            eq_df, current, fno_symbols
                        )

                    # F&O bhavcopy
                    fo_df = await scraper.fetch_derivatives_bhavcopy(current)
                    if fo_df is not None:
                        option_collector = OptionCollector(db)
                        await option_collector.process_bhavcopy(fo_df, current, spot_prices)

                    # VIX
                    vix_collector = VIXCollector(db, scraper)
                    await vix_collector.collect(current)

                    await db.commit()

                except Exception as e:
                    logger.warning(f"Error backfilling {current}: {e}")
                    await db.rollback()

                current += timedelta(days=1)

                # Rate limiting between days
                await asyncio.sleep(1)

    finally:
        await scraper.close()
        from app.core.database import engine
        await engine.dispose()
        logger.info("Database engine connections disposed")
