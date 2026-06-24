"""
VIX collector — fetches and stores India VIX data.
"""

from datetime import date
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from app.models import IndiaVIX
from app.ingestion.base import DataSource
from app.core.logging import get_logger

logger = get_logger(__name__)


class VIXCollector:
    """Collects and stores India VIX data."""

    def __init__(self, db: AsyncSession, data_source: DataSource):
        self.db = db
        self.data_source = data_source

    async def collect(self, trade_date: date) -> Optional[float]:
        """
        Fetch and store VIX for a given date.
        Returns the closing VIX value.
        """
        vix_data = await self.data_source.fetch_india_vix(trade_date)

        if not vix_data:
            logger.info(f"No VIX data for {trade_date}")
            return None

        record = {
            "date": trade_date,
            "open": vix_data.get("open"),
            "high": vix_data.get("high"),
            "low": vix_data.get("low"),
            "close": vix_data["close"],
        }

        stmt = insert(IndiaVIX).values(record)
        stmt = stmt.on_conflict_do_update(
            index_elements=["date"],
            set_={
                "open": stmt.excluded.open,
                "high": stmt.excluded.high,
                "low": stmt.excluded.low,
                "close": stmt.excluded.close,
            },
        )
        await self.db.execute(stmt)

        logger.info(f"Stored VIX data", date=str(trade_date), close=vix_data["close"])
        return vix_data["close"]
