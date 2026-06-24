"""
Spot price collector — extracts OHLCV from equity bhavcopy.
"""

from datetime import date
from typing import Optional

import polars as pl
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from app.models import SpotPrice
from app.core.logging import get_logger

logger = get_logger(__name__)


class SpotCollector:
    """Collects and stores spot prices from equity bhavcopy."""

    # Indices to track (these come from index-specific endpoints)
    INDEX_SYMBOLS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}

    def __init__(self, db: AsyncSession):
        self.db = db

    async def process_equity_bhavcopy(
        self,
        df: pl.DataFrame,
        trade_date: date,
        fno_symbols: set[str],
    ) -> dict[str, float]:
        """
        Process equity bhavcopy to extract spot prices for F&O symbols.

        Args:
            df: Raw equity bhavcopy DataFrame
            trade_date: Trading date
            fno_symbols: Set of F&O symbols to collect

        Returns:
            Dict mapping symbol -> closing price
        """
        logger.info(f"Processing equity bhavcopy for {trade_date}")

        # Normalize columns
        df = df.rename({col: col.strip().upper() for col in df.columns})

        # Filter for EQ series (regular equity)
        if "SERIES" in df.columns:
            df = df.filter(pl.col("SERIES").str.strip_chars() == "EQ")

        spot_prices = {}
        records = []

        for row in df.iter_rows(named=True):
            symbol = row.get("SYMBOL", "").strip()
            if not symbol or symbol not in fno_symbols:
                continue

            close_price = self._safe_float(row.get("CLOSE"))
            if close_price is None or close_price <= 0:
                continue

            spot_prices[symbol] = close_price

            records.append({
                "date": trade_date,
                "symbol": symbol,
                "open": self._safe_float(row.get("OPEN")),
                "high": self._safe_float(row.get("HIGH")),
                "low": self._safe_float(row.get("LOW")),
                "close": close_price,
                "volume": int(row.get("TOTTRDQTY", 0) or row.get("VOLUME", 0) or 0),
            })

        if records:
            await self._upsert_records(records)
            logger.info(
                f"Stored spot prices",
                date=str(trade_date),
                count=len(records),
            )

        return spot_prices

    async def store_index_spot(
        self,
        symbol: str,
        trade_date: date,
        close_price: float,
        open_price: Optional[float] = None,
        high_price: Optional[float] = None,
        low_price: Optional[float] = None,
    ) -> None:
        """Store index spot price (indices aren't in equity bhavcopy)."""
        record = {
            "date": trade_date,
            "symbol": symbol,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": None,
        }
        await self._upsert_records([record])

    def _safe_float(self, value) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    async def _upsert_records(self, records: list[dict]) -> int:
        if not records:
            return 0

        stmt = insert(SpotPrice).values(records)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_spot_date_symbol",
            set_={
                "open": stmt.excluded.open,
                "high": stmt.excluded.high,
                "low": stmt.excluded.low,
                "close": stmt.excluded.close,
                "volume": stmt.excluded.volume,
            },
        )
        await self.db.execute(stmt)
        return len(records)
