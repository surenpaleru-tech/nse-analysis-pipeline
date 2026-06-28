"""
OTM Distance Calculator — computes out-of-the-money distances and maps to strikes.

For each historical expiry and each OTM percentage level (1% to 20%),
finds the nearest available strike and retrieves its option data.
"""

from datetime import date
from typing import Optional

import numpy as np
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, distinct

from app.models import OptionChain, SpotPrice
from app.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


class OTMDistanceCalculator:
    """Calculates OTM distances and maps them to actual strikes."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.otm_percentages = settings.otm_pct_list

    async def get_otm_strikes_for_expiry(
        self,
        symbol: str,
        expiry_date: date,
        entry_date: date,
        spot_price: float,
    ) -> dict:
        """
        For a given expiry, calculate the nearest CE and PE strikes
        at each OTM percentage level.

        Returns:
            {
                "ce": {1: {"strike": 25200, "premium": 45.5}, 2: {...}, ...},
                "pe": {1: {"strike": 24800, "premium": 32.1}, 2: {...}, ...},
            }
        """
        # Get available strikes for this expiry on the entry date
        strikes_query = (
            select(
                OptionChain.strike,
                OptionChain.option_type,
                OptionChain.close,
                OptionChain.volume,
                OptionChain.oi,
            )
            .where(
                OptionChain.symbol == symbol,
                OptionChain.expiry == expiry_date,
                OptionChain.trade_date == entry_date,
            )
        )
        result = await self.db.execute(strikes_query)
        rows = result.all()

        if not rows:
            return {"ce": {}, "pe": {}}

        # Separate CE and PE strikes
        ce_strikes = {}
        pe_strikes = {}
        for row in rows:
            strike, opt_type, close_price, volume, oi = row
            strike = float(strike)
            data = {
                "strike": strike,
                "premium": float(close_price) if close_price else 0.0,
                "volume": int(volume or 0),
                "oi": int(oi or 0),
            }
            if opt_type == "CE":
                ce_strikes[strike] = data
            else:
                pe_strikes[strike] = data

        # Calculate OTM strikes for each percentage
        result_ce = {}
        result_pe = {}

        for pct in self.otm_percentages:
            # CE: strike = spot * (1 + pct/100)
            target_ce_strike = spot_price * (1 + pct / 100)
            nearest_ce = self._find_nearest_strike(target_ce_strike, list(ce_strikes.keys()))
            if nearest_ce is not None:
                result_ce[pct] = ce_strikes[nearest_ce]

            # PE: strike = spot * (1 - pct/100)
            target_pe_strike = spot_price * (1 - pct / 100)
            nearest_pe = self._find_nearest_strike(target_pe_strike, list(pe_strikes.keys()))
            if nearest_pe is not None:
                result_pe[pct] = pe_strikes[nearest_pe]

        return {"ce": result_ce, "pe": result_pe}

    async def get_expiry_premiums(
        self,
        symbol: str,
        expiry_date: date,
        strikes: dict[str, dict],
        exit_date: Optional[date] = None,
    ) -> dict:
        """
        Get the closing premiums on expiry day or exit date for given strikes.

        Args:
            symbol: The underlying symbol
            expiry_date: The expiry date
            strikes: {"ce": {pct: {"strike": ...}}, "pe": {pct: {"strike": ...}}}
            exit_date: Optional exit/LTP date

        Returns:
            {"ce": {pct: expiry_premium}, "pe": {pct: expiry_premium}}
        """
        result = {"ce": {}, "pe": {}}

        for opt_type in ["ce", "pe"]:
            for pct, data in strikes.get(opt_type, {}).items():
                strike = data["strike"]
                query = (
                    select(OptionChain.close)
                    .where(
                        OptionChain.symbol == symbol,
                        OptionChain.expiry == expiry_date,
                        OptionChain.trade_date == (exit_date or expiry_date),
                        OptionChain.strike == strike,
                        OptionChain.option_type == opt_type.upper(),
                    )
                )
                res = await self.db.execute(query)
                expiry_close = res.scalar_one_or_none()

                # If no data on expiry day, it expired worthless or was 0
                result[opt_type][pct] = float(expiry_close) if expiry_close else 0.0

        return result

    async def get_unique_expiries(
        self,
        symbol: str,
        expiry_type: str,
    ) -> list[date]:
        """Get all unique expiry dates for a symbol and expiry type."""
        query = (
            select(distinct(OptionChain.expiry))
            .where(
                OptionChain.symbol == symbol,
                OptionChain.expiry_type == expiry_type,
            )
            .order_by(OptionChain.expiry)
        )
        result = await self.db.execute(query)
        return [row[0] for row in result.all()]

    async def get_entry_date_for_expiry(
        self,
        symbol: str,
        expiry_date: date,
        days_before: int = 0,
    ) -> Optional[date]:
        """
        Get the entry date for a given expiry.
        If days_before=0:
            For weekly: returns the first available trading day for this expiry.
            For monthly: returns the first trading day after the previous monthly expiry,
                         or the first trading day of the month if no previous expiry is found.
        """
        # Determine expiry type
        type_query = select(OptionChain.expiry_type).where(
            OptionChain.symbol == symbol,
            OptionChain.expiry == expiry_date
        ).limit(1)
        type_res = await self.db.execute(type_query)
        expiry_type = type_res.scalar() or "monthly"

        if days_before == 0:
            if expiry_type == "weekly":
                query = (
                    select(func.min(OptionChain.trade_date))
                    .where(
                        OptionChain.symbol == symbol,
                        OptionChain.expiry == expiry_date,
                    )
                )
            else:
                # Find previous monthly expiry
                prev_expiry_query = (
                    select(distinct(OptionChain.expiry))
                    .where(
                        OptionChain.symbol == symbol,
                        OptionChain.expiry_type == "monthly",
                        OptionChain.expiry < expiry_date,
                    )
                    .order_by(OptionChain.expiry.desc())
                    .limit(1)
                )
                prev_res = await self.db.execute(prev_expiry_query)
                prev_expiry = prev_res.scalar()

                if prev_expiry:
                    query = (
                        select(func.min(OptionChain.trade_date))
                        .where(
                            OptionChain.symbol == symbol,
                            OptionChain.expiry == expiry_date,
                            OptionChain.trade_date > prev_expiry,
                        )
                    )
                else:
                    first_of_month = date(expiry_date.year, expiry_date.month, 1)
                    query = (
                        select(func.min(OptionChain.trade_date))
                        .where(
                            OptionChain.symbol == symbol,
                            OptionChain.expiry == expiry_date,
                            OptionChain.trade_date >= first_of_month,
                        )
                    )
        else:
            if expiry_type == "weekly":
                query = (
                    select(OptionChain.trade_date)
                    .where(
                        OptionChain.symbol == symbol,
                        OptionChain.expiry == expiry_date,
                    )
                    .order_by(OptionChain.trade_date)
                    .offset(days_before)
                    .limit(1)
                )
            else:
                # Find previous monthly expiry
                prev_expiry_query = (
                    select(distinct(OptionChain.expiry))
                    .where(
                        OptionChain.symbol == symbol,
                        OptionChain.expiry_type == "monthly",
                        OptionChain.expiry < expiry_date,
                    )
                    .order_by(OptionChain.expiry.desc())
                    .limit(1)
                )
                prev_res = await self.db.execute(prev_expiry_query)
                prev_expiry = prev_res.scalar()

                if prev_expiry:
                    query = (
                        select(OptionChain.trade_date)
                        .where(
                            OptionChain.symbol == symbol,
                            OptionChain.expiry == expiry_date,
                            OptionChain.trade_date > prev_expiry,
                        )
                        .order_by(OptionChain.trade_date)
                        .offset(days_before)
                        .limit(1)
                    )
                else:
                    first_of_month = date(expiry_date.year, expiry_date.month, 1)
                    query = (
                        select(OptionChain.trade_date)
                        .where(
                            OptionChain.symbol == symbol,
                            OptionChain.expiry == expiry_date,
                            OptionChain.trade_date >= first_of_month,
                        )
                        .order_by(OptionChain.trade_date)
                        .offset(days_before)
                        .limit(1)
                    )

        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    def _find_nearest_strike(
        self, target: float, available: list[float]
    ) -> Optional[float]:
        """Find the nearest available strike to the target."""
        if not available:
            return None
        arr = np.array(available)
        idx = np.argmin(np.abs(arr - target))
        return float(arr[idx])
