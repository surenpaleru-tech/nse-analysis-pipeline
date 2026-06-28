"""Futures chain collector and normalizer."""

from datetime import date, datetime
from typing import Optional

import polars as pl
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.models import FuturesChain

logger = get_logger(__name__)


class FuturesCollector:
    """Processes raw derivatives bhavcopy data into futures_chain rows."""

    INDEX_SYMBOLS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}

    def __init__(self, db: AsyncSession):
        self.db = db

    async def process_bhavcopy(
        self,
        df: pl.DataFrame,
        trade_date: date,
        spot_prices: dict[str, float],
    ) -> int:
        """Normalize and upsert futures rows from a raw bhavcopy DataFrame."""
        futures_df = df.filter(pl.col("INSTRUMENT").is_in(["FUTIDX", "FUTSTK"]))
        if len(futures_df) == 0:
            logger.info(f"No futures data found in bhavcopy for {trade_date}")
            return 0

        futures_df = self._normalize_columns(futures_df)

        records = []
        for row in futures_df.iter_rows(named=True):
            symbol = row["symbol"].strip()
            expiry_date = self._parse_date(row["expiry_dt"])
            if expiry_date is None:
                continue

            underlying_price = spot_prices.get(symbol)
            if not underlying_price:
                raw_underlying = row.get("underlying_val") or row.get("underlying_price")
                underlying_price = self._safe_float(raw_underlying)

            instrument = row.get("instrument", "").strip()
            records.append(
                {
                    "trade_date": trade_date,
                    "symbol": symbol,
                    "instrument": instrument,
                    "instrument_type": "index" if symbol in self.INDEX_SYMBOLS else "stock",
                    "expiry": expiry_date,
                    "expiry_type": "monthly",
                    "open": self._safe_float(row.get("open")),
                    "high": self._safe_float(row.get("high")),
                    "low": self._safe_float(row.get("low")),
                    "close": self._safe_float(row.get("close")),
                    "settle_price": self._safe_float(row.get("settle_pr")),
                    "volume": int(row.get("contracts", 0) or 0),
                    "turnover_lakh": self._safe_float(row.get("val_inlakh")),
                    "oi": int(row.get("open_int", 0) or 0),
                    "change_oi": int(row.get("chg_in_oi", 0) or 0),
                    "underlying_price": underlying_price,
                }
            )

        if not records:
            return 0

        inserted = await self._upsert_records(records)
        logger.info("Processed futures chain data", date=str(trade_date), records=inserted)
        return inserted

    def _normalize_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.rename({col: col.strip().lower() for col in df.columns})

    def _parse_date(self, date_str: str) -> Optional[date]:
        if not date_str or not str(date_str).strip():
            return None
        value = str(date_str).strip()
        for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
        logger.warning(f"Cannot parse futures expiry date: {date_str}")
        return None

    def _safe_float(self, value) -> Optional[float]:
        if value is None:
            return None
        try:
            parsed = float(value)
            return parsed if parsed >= 0 else None
        except (ValueError, TypeError):
            return None

    async def _upsert_records(self, records: list[dict]) -> int:
        chunk_size = 1000
        inserted_count = 0

        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            stmt = insert(FuturesChain).values(chunk)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_fc_trade_symbol_expiry_instrument",
                set_={
                    "instrument_type": stmt.excluded.instrument_type,
                    "expiry_type": stmt.excluded.expiry_type,
                    "open": stmt.excluded.open,
                    "high": stmt.excluded.high,
                    "low": stmt.excluded.low,
                    "close": stmt.excluded.close,
                    "settle_price": stmt.excluded.settle_price,
                    "volume": stmt.excluded.volume,
                    "turnover_lakh": stmt.excluded.turnover_lakh,
                    "oi": stmt.excluded.oi,
                    "change_oi": stmt.excluded.change_oi,
                    "underlying_price": stmt.excluded.underlying_price,
                },
            )
            await self.db.execute(stmt)
            inserted_count += len(chunk)

        return inserted_count
