"""
Market Regime Classifier — classifies market conditions into regimes.

Regimes:
- VIX: low (< 15), medium (15-25), high (> 25)
- Market: bull (20d return > +3%), bear (< -3%), sideways (-3% to +3%)
"""

from datetime import date, timedelta
from typing import Optional

import numpy as np
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models import SpotPrice, IndiaVIX
from app.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


class RegimeClassifier:
    """Classifies market conditions into VIX and trend regimes."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.vix_low = settings.vix_low_threshold
        self.vix_high = settings.vix_high_threshold
        self.bull_threshold = settings.market_bull_threshold
        self.bear_threshold = settings.market_bear_threshold

    def classify_vix_regime(self, vix_value: float) -> str:
        """Classify VIX into regime categories."""
        if vix_value < self.vix_low:
            return "low"
        elif vix_value > self.vix_high:
            return "high"
        return "medium"

    async def classify_market_regime(
        self,
        symbol: str,
        as_of_date: date,
        lookback_days: int = 20,
    ) -> str:
        """
        Classify market regime based on trailing return.

        Args:
            symbol: The underlying symbol
            as_of_date: Date to classify
            lookback_days: Number of trading days to look back

        Returns:
            'bull', 'bear', or 'sideways'
        """
        # Get spot prices for lookback period
        start_date = as_of_date - timedelta(days=lookback_days * 2)  # Buffer for weekends

        query = (
            select(SpotPrice.date, SpotPrice.close)
            .where(
                SpotPrice.symbol == symbol,
                SpotPrice.date.between(start_date, as_of_date),
            )
            .order_by(SpotPrice.date)
        )
        result = await self.db.execute(query)
        rows = result.all()

        if len(rows) < lookback_days:
            return "sideways"  # Insufficient data

        # Calculate trailing return
        prices = [float(r.close) for r in rows[-lookback_days:]]
        if prices[0] == 0:
            return "sideways"

        trailing_return = ((prices[-1] / prices[0]) - 1) * 100

        if trailing_return > self.bull_threshold:
            return "bull"
        elif trailing_return < self.bear_threshold:
            return "bear"
        return "sideways"

    async def get_vix_for_date(self, trade_date: date) -> Optional[float]:
        """Get VIX closing value for a date."""
        query = select(IndiaVIX.close).where(IndiaVIX.date == trade_date)
        result = await self.db.execute(query)
        vix = result.scalar_one_or_none()
        return float(vix) if vix else None

    async def build_regime_map(
        self,
        symbol: str,
        dates: list[date],
    ) -> tuple[dict[date, float], dict[date, str]]:
        """
        Build VIX and market regime maps for a list of dates.

        Returns:
            (vix_map, regime_map) — both mapping date -> value
        """
        vix_map = {}
        regime_map = {}

        for d in dates:
            vix = await self.get_vix_for_date(d)
            if vix:
                vix_map[d] = vix

            regime = await self.classify_market_regime(symbol, d)
            regime_map[d] = regime

        return vix_map, regime_map
