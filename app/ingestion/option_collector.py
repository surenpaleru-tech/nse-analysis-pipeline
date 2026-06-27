"""
Option chain data collector and normalizer.
Processes raw bhavcopy data into normalized option_chain records.
"""

from datetime import date, datetime
from typing import Optional

import polars as pl
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from app.models import OptionChain, SpotPrice, Expiry
from app.core.logging import get_logger

logger = get_logger(__name__)


class OptionCollector:
    """Processes and stores option chain data from bhavcopy."""

    # Columns expected in F&O bhavcopy
    BHAVCOPY_COLUMNS = {
        "INSTRUMENT": str,
        "SYMBOL": str,
        "EXPIRY_DT": str,
        "STRIKE_PR": float,
        "OPTION_TYP": str,
        "OPEN": float,
        "HIGH": float,
        "LOW": float,
        "CLOSE": float,
        "SETTLE_PR": float,
        "CONTRACTS": int,
        "VAL_INLAKH": float,
        "OPEN_INT": int,
        "CHG_IN_OI": int,
        "TIMESTAMP": str,
    }

    # Index symbols
    INDEX_SYMBOLS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}

    def __init__(self, db: AsyncSession):
        self.db = db

    async def process_bhavcopy(
        self,
        df: pl.DataFrame,
        trade_date: date,
        spot_prices: dict[str, float],
    ) -> int:
        """
        Process a raw F&O bhavcopy DataFrame into option_chain records.

        Args:
            df: Raw bhavcopy DataFrame from NSE
            trade_date: The trading date
            spot_prices: Dict mapping symbol -> spot close price

        Returns:
            Number of records inserted/updated
        """
        logger.info(f"Processing bhavcopy for {trade_date}", rows=len(df))

        # Filter for option instruments only (OPTIDX and OPTSTK)
        options_df = df.filter(
            pl.col("INSTRUMENT").is_in(["OPTIDX", "OPTSTK"])
        )

        if len(options_df) == 0:
            logger.warning(f"No options data found in bhavcopy for {trade_date}")
            return 0

        # Clean and normalize data
        options_df = self._normalize_columns(options_df)

        # Classify expiry type
        records = []
        for row in options_df.iter_rows(named=True):
            symbol = row["symbol"].strip()
            expiry_date = self._parse_date(row["expiry_dt"])
            if expiry_date is None:
                continue

            expiry_type = self._classify_expiry_type(symbol, expiry_date)
            underlying_price = spot_prices.get(symbol)
            if not underlying_price:
                raw_und_val = row.get("underlying_val") or row.get("underlying_price") or row.get("underlying")
                underlying_price = self._safe_float(raw_und_val)
                if symbol in self.INDEX_SYMBOLS and underlying_price and symbol not in spot_prices:
                    from app.ingestion.spot_collector import SpotCollector
                    spot_collector = SpotCollector(self.db)
                    await spot_collector.store_index_spot(symbol, trade_date, underlying_price)
                    spot_prices[symbol] = underlying_price

            # For index options, skip weekly for stocks, monthly for non-monthly index
            instrument = row.get("instrument", "")
            if instrument == "OPTSTK" and expiry_type == "weekly":
                continue  # Stocks only have monthly options

            records.append({
                "trade_date": trade_date,
                "symbol": symbol,
                "expiry": expiry_date,
                "expiry_type": expiry_type,
                "strike": float(row["strike_pr"]),
                "option_type": row["option_typ"].strip(),
                "open": self._safe_float(row.get("open")),
                "high": self._safe_float(row.get("high")),
                "low": self._safe_float(row.get("low")),
                "close": self._safe_float(row.get("close")),
                "volume": int(row.get("contracts", 0) or 0),
                "oi": int(row.get("open_int", 0) or 0),
                "change_oi": int(row.get("chg_in_oi", 0) or 0),
                "implied_volatility": None,  # Not in bhavcopy
                "underlying_price": underlying_price,
            })

        if not records:
            logger.warning(f"No valid option records to insert for {trade_date}")
            return 0

        # Upsert records using PostgreSQL ON CONFLICT
        inserted = await self._upsert_records(records)
        logger.info(
            f"Processed option chain data",
            date=str(trade_date),
            records=inserted,
        )
        return inserted

    def _normalize_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize column names to lowercase."""
        return df.rename({col: col.strip().lower() for col in df.columns})

    def _parse_date(self, date_str: str) -> Optional[date]:
        """Parse date string from bhavcopy format."""
        if not date_str or not date_str.strip():
            return None
        s = date_str.strip()
        for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        logger.warning(f"Cannot parse date: {date_str}")
        return None

    def _classify_expiry_type(self, symbol: str, expiry_date: date) -> str:
        """
        Classify expiry as weekly or monthly.
        Monthly expiries are the last Thursday of the month.
        """
        if symbol not in self.INDEX_SYMBOLS:
            return "monthly"  # Stocks only have monthly options

        # Check if this is the last Thursday of the month
        import calendar
        year = expiry_date.year
        month = expiry_date.month

        # Find last Thursday of the month
        last_day = calendar.monthrange(year, month)[1]
        last_thursday = None
        for day in range(last_day, last_day - 7, -1):
            if date(year, month, day).weekday() == 3:  # Thursday
                last_thursday = date(year, month, day)
                break

        if last_thursday and expiry_date == last_thursday:
            return "monthly"
        return "weekly"

    def _safe_float(self, value) -> Optional[float]:
        """Safely convert to float."""
        if value is None:
            return None
        try:
            f = float(value)
            return f if f >= 0 else None
        except (ValueError, TypeError):
            return None

    async def _upsert_records(self, records: list[dict]) -> int:
        """Upsert option chain records using PostgreSQL ON CONFLICT in chunks."""
        if not records:
            return 0

        chunk_size = 1000
        inserted_count = 0
        
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            stmt = insert(OptionChain).values(chunk)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_oc_trade_symbol_expiry_strike_type",
                set_={
                    "open": stmt.excluded.open,
                    "high": stmt.excluded.high,
                    "low": stmt.excluded.low,
                    "close": stmt.excluded.close,
                    "volume": stmt.excluded.volume,
                    "oi": stmt.excluded.oi,
                    "change_oi": stmt.excluded.change_oi,
                    "implied_volatility": stmt.excluded.implied_volatility,
                    "underlying_price": stmt.excluded.underlying_price,
                },
            )
            await self.db.execute(stmt)
            inserted_count += len(chunk)

        return inserted_count
