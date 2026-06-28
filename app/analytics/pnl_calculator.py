"""
P&L Calculator — computes profit/loss for option selling strategies.

For each CE/PE combination at each OTM percentage, calculates:
- Entry premium (selling)
- Expiry premium (buying back or expiring worthless)
- Net P&L
"""

from datetime import date
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select

from app.models import StrategyResult, SpotPrice
from app.analytics.otm_distance import OTMDistanceCalculator
from app.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


class PnLCalculator:
    """Calculates P&L for every CE/PE combination across historical expiries."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.otm_calc = OTMDistanceCalculator(db)
        self.otm_percentages = settings.otm_pct_list

    async def compute_for_symbol(
        self,
        symbol: str,
        expiry_type: str,
        vix_values: dict[date, float] | None = None,
        market_regimes: dict[date, str] | None = None,
    ) -> int:
        """
        Compute P&L for all CE/PE combinations for all historical expiries.

        Args:
            symbol: The underlying symbol
            expiry_type: 'weekly' or 'monthly'
            vix_values: Optional dict mapping dates to VIX values
            market_regimes: Optional dict mapping dates to regime labels

        Returns:
            Total number of strategy results computed
        """
        logger.info(f"Computing P&L for {symbol} ({expiry_type})")

        # Get all unique expiries
        expiries = await self.otm_calc.get_unique_expiries(symbol, expiry_type)
        logger.info(f"Found {len(expiries)} expiries for {symbol}")

        total_results = 0

        for expiry_date in expiries:
            try:
                results = await self._compute_for_expiry(
                    symbol, expiry_date, expiry_type,
                    vix_values, market_regimes,
                )
                total_results += results
            except Exception as e:
                logger.error(
                    f"Error computing P&L for expiry {expiry_date}: {e}",
                    symbol=symbol,
                )
                continue

        logger.info(
            f"Completed P&L computation",
            symbol=symbol,
            expiry_type=expiry_type,
            total_results=total_results,
        )
        return total_results

    async def _compute_for_expiry(
        self,
        symbol: str,
        expiry_date: date,
        expiry_type: str,
        vix_values: Optional[dict] = None,
        market_regimes: Optional[dict] = None,
    ) -> int:
        """Compute P&L for all CE/PE combinations for a single expiry."""
        # Historical strategy results should represent completed expiries only.
        # Ongoing or future contracts are surfaced separately in the app using
        # live option-chain snapshots.
        if expiry_date > date.today():
            logger.info(
                "Skipping ongoing/future expiry for historical P&L storage",
                symbol=symbol,
                expiry=str(expiry_date),
            )
            return 0

        # Get entry date
        entry_date = await self.otm_calc.get_entry_date_for_expiry(symbol, expiry_date)
        if not entry_date:
            return 0

        # Skip if the entry date is in the future (trade hasn't started yet)
        if entry_date > date.today():
            logger.info(
                "Skipping future expiry P&L calculation - entry date is in the future",
                symbol=symbol,
                expiry=str(expiry_date),
                entry_date=str(entry_date),
            )
            return 0

        # Get spot price at entry
        spot_query = select(SpotPrice.close).where(
            SpotPrice.symbol == symbol,
            SpotPrice.date == entry_date,
        )
        spot_result = await self.db.execute(spot_query)
        spot_at_entry = spot_result.scalar_one_or_none()

        if not spot_at_entry:
            # Fallback to OptionChain
            from app.models import OptionChain
            fall_q = select(OptionChain.underlying_price).where(
                OptionChain.symbol == symbol,
                OptionChain.trade_date == entry_date,
            ).limit(1)
            fall_res = await self.db.execute(fall_q)
            spot_at_entry = fall_res.scalar()

        if not spot_at_entry:
            return 0

        spot_at_entry = float(spot_at_entry)

        # Determine exit date and spot at exit
        exit_date = expiry_date
        spot_at_expiry = None

        # Get spot at exit_date
        spot_expiry_query = select(SpotPrice.close).where(
            SpotPrice.symbol == symbol,
            SpotPrice.date == exit_date,
        )
        spot_expiry_result = await self.db.execute(spot_expiry_query)
        spot_at_expiry = spot_expiry_result.scalar_one_or_none()
        
        if not spot_at_expiry:
            # Fallback to OptionChain
            from app.models import OptionChain
            fall_exp_q = select(OptionChain.underlying_price).where(
                OptionChain.symbol == symbol,
                OptionChain.trade_date == exit_date,
            ).limit(1)
            fall_exp_res = await self.db.execute(fall_exp_q)
            spot_at_expiry = fall_exp_res.scalar()

        if not spot_at_expiry:
            logger.warning(
                "Skipping expiry P&L calculation - missing spot price at exit date",
                symbol=symbol,
                expiry=str(expiry_date),
                exit_date=str(exit_date),
            )
            return 0

        spot_at_expiry = float(spot_at_expiry)

        # Get OTM strikes at entry
        otm_strikes = await self.otm_calc.get_otm_strikes_for_expiry(
            symbol, expiry_date, entry_date, spot_at_entry,
        )

        # Get expiry premiums
        expiry_premiums = await self.otm_calc.get_expiry_premiums(
            symbol, expiry_date, otm_strikes, exit_date=exit_date,
        )

        # VIX and regime at entry
        vix_at_entry = vix_values.get(entry_date) if vix_values else None
        regime = market_regimes.get(entry_date) if market_regimes else None

        # Compute P&L for every CE/PE combination
        records = []
        for ce_pct in self.otm_percentages:
            ce_entry = otm_strikes["ce"].get(ce_pct, {})
            ce_entry_premium = ce_entry.get("premium", 0)
            ce_expiry_premium = expiry_premiums["ce"].get(ce_pct, 0)
            ce_strike = ce_entry.get("strike")

            if ce_strike is None:
                continue

            for pe_pct in self.otm_percentages:
                pe_entry = otm_strikes["pe"].get(pe_pct, {})
                pe_entry_premium = pe_entry.get("premium", 0)
                pe_expiry_premium = expiry_premiums["pe"].get(pe_pct, 0)
                pe_strike = pe_entry.get("strike")

                if pe_strike is None:
                    continue

                # P&L for selling options:
                # Profit = entry premium - expiry premium (for seller)
                ce_pnl = ce_entry_premium - ce_expiry_premium
                pe_pnl = pe_entry_premium - pe_expiry_premium
                total_pnl = ce_pnl + pe_pnl

                # Return percentage based on total premium collected
                total_premium = ce_entry_premium + pe_entry_premium
                return_pct = (total_pnl / total_premium * 100) if total_premium > 0 else 0

                records.append({
                    "symbol": symbol,
                    "expiry": expiry_date,
                    "expiry_type": expiry_type,
                    "ce_pct": ce_pct,
                    "pe_pct": pe_pct,
                    "ce_strike": ce_strike,
                    "pe_strike": pe_strike,
                    "spot_at_entry": spot_at_entry,
                    "spot_at_expiry": spot_at_expiry,
                    "ce_entry_premium": ce_entry_premium,
                    "pe_entry_premium": pe_entry_premium,
                    "ce_expiry_premium": ce_expiry_premium,
                    "pe_expiry_premium": pe_expiry_premium,
                    "ce_pnl": ce_pnl,
                    "pe_pnl": pe_pnl,
                    "total_pnl": total_pnl,
                    "ce_expired_worthless": ce_expiry_premium <= 0.05,
                    "pe_expired_worthless": pe_expiry_premium <= 0.05,
                    "return_pct": return_pct,
                    "vix_at_entry": vix_at_entry,
                    "market_regime": regime,
                })

        if records:
            await self._upsert_results(records)

        return len(records)

    async def _upsert_results(self, records: list[dict]) -> None:
        """Upsert strategy results."""
        stmt = insert(StrategyResult).values(records)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_sr_symbol_expiry_pcts",
            set_={
                "ce_strike": stmt.excluded.ce_strike,
                "pe_strike": stmt.excluded.pe_strike,
                "spot_at_entry": stmt.excluded.spot_at_entry,
                "spot_at_expiry": stmt.excluded.spot_at_expiry,
                "ce_entry_premium": stmt.excluded.ce_entry_premium,
                "pe_entry_premium": stmt.excluded.pe_entry_premium,
                "ce_expiry_premium": stmt.excluded.ce_expiry_premium,
                "pe_expiry_premium": stmt.excluded.pe_expiry_premium,
                "ce_pnl": stmt.excluded.ce_pnl,
                "pe_pnl": stmt.excluded.pe_pnl,
                "total_pnl": stmt.excluded.total_pnl,
                "ce_expired_worthless": stmt.excluded.ce_expired_worthless,
                "pe_expired_worthless": stmt.excluded.pe_expired_worthless,
                "return_pct": stmt.excluded.return_pct,
                "vix_at_entry": stmt.excluded.vix_at_entry,
                "market_regime": stmt.excluded.market_regime,
            },
        )
        await self.db.execute(stmt)
