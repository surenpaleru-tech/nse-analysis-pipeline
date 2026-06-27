"""
Band Optimizer — determines optimal CE/PE selling percentages.

Iterates all 169 CE/PE combinations (13x13) and ranks them by
the selected optimization mode: Expected Value, Win Rate, Sharpe, or Min Drawdown.
"""

from datetime import date, timedelta
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from sqlalchemy.dialects.postgresql import insert

from app.models import StrategyResult, OptimalSellingBand
from app.analytics.risk_metrics import calculate_risk_metrics, RiskMetrics
from app.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

# Analysis periods
ANALYSIS_PERIODS = {
    "3m": 90,
    "6m": 180,
    "1y": 365,
    "2y": 730,
    "all": None,
}

# Optimization modes and their sort keys
OPTIMIZATION_MODES = {
    "expected_value": lambda m: m.expected_value,
    "win_rate": lambda m: m.win_rate,
    "sharpe_ratio": lambda m: m.sharpe_ratio,
    "min_drawdown": lambda m: -m.max_drawdown,  # Negative because lower is better
}


class BandOptimizer:
    """Optimizes CE/PE selling bands using historical strategy results."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.otm_percentages = settings.otm_pct_list

    async def optimize_for_symbol(
        self,
        symbol: str,
        instrument_type: str,
        expiry_type: str,
    ) -> int:
        """
        Run full optimization for a symbol across all periods, regimes, and modes.

        Returns total number of optimal band records created.
        """
        logger.info(f"Optimizing bands for {symbol} ({expiry_type})")
        from sqlalchemy import delete
        await self.db.execute(
            delete(OptimalSellingBand).where(
                OptimalSellingBand.symbol == symbol,
                OptimalSellingBand.expiry_type == expiry_type,
            )
        )
        total = 0

        for period_name, period_days in ANALYSIS_PERIODS.items():
            for mode in OPTIMIZATION_MODES:
                # Overall (no regime filter)
                result = await self._optimize_single(
                    symbol, instrument_type, expiry_type,
                    period_name, period_days, mode,
                    vix_regime=None, market_regime=None,
                )
                if result:
                    total += 1

                # By VIX regime
                for vix_regime in ["low", "medium", "high"]:
                    result = await self._optimize_single(
                        symbol, instrument_type, expiry_type,
                        period_name, period_days, mode,
                        vix_regime=vix_regime, market_regime=None,
                    )
                    if result:
                        total += 1

                # By market regime
                for market_regime in ["bull", "bear", "sideways"]:
                    result = await self._optimize_single(
                        symbol, instrument_type, expiry_type,
                        period_name, period_days, mode,
                        vix_regime=None, market_regime=market_regime,
                    )
                    if result:
                        total += 1

        logger.info(f"Band optimization complete", symbol=symbol, records=total)
        return total

    async def _optimize_single(
        self,
        symbol: str,
        instrument_type: str,
        expiry_type: str,
        period_name: str,
        period_days: Optional[int],
        optimization_mode: str,
        vix_regime: Optional[str] = None,
        market_regime: Optional[str] = None,
    ) -> Optional[dict]:
        """Optimize for a specific period/regime/mode combination."""
        # Build query for strategy results
        query = select(StrategyResult).where(
            StrategyResult.symbol == symbol,
            StrategyResult.expiry_type == expiry_type,
        )

        # Period filter
        if period_days is not None:
            cutoff_date = date.today() - timedelta(days=period_days)
            query = query.where(StrategyResult.expiry >= cutoff_date)

        # Regime filters
        if market_regime:
            query = query.where(StrategyResult.market_regime == market_regime)

        # VIX regime — need to check vix_at_entry
        # We'll filter in Python for VIX since it's not a direct column match

        result = await self.db.execute(query)
        all_results = result.scalars().all()

        if not all_results:
            return None

        # Filter by VIX regime if specified
        if vix_regime:
            all_results = [
                r for r in all_results
                if r.vix_at_entry is not None and self._classify_vix(float(r.vix_at_entry)) == vix_regime
            ]

        if len(all_results) < 1:
            return None

        # Group by CE/PE combination
        combo_results = {}
        for r in all_results:
            key = (float(r.ce_pct), float(r.pe_pct))
            if key not in combo_results:
                combo_results[key] = {
                    "pnl": [],
                    "ce_worthless": [],
                    "pe_worthless": [],
                }
            combo_results[key]["pnl"].append(float(r.total_pnl or 0))
            combo_results[key]["ce_worthless"].append(bool(r.ce_expired_worthless))
            combo_results[key]["pe_worthless"].append(bool(r.pe_expired_worthless))

        # Calculate metrics for each combination
        annualization = 52 if expiry_type == "weekly" else 12
        best_combo = None
        best_metrics = None
        best_score = float("-inf")

        sort_fn = OPTIMIZATION_MODES[optimization_mode]

        for (ce_pct, pe_pct), data in combo_results.items():
            metrics = calculate_risk_metrics(
                data["pnl"],
                data["ce_worthless"],
                data["pe_worthless"],
                annualization_factor=annualization,
            )
            if metrics is None:
                continue

            score = sort_fn(metrics)
            if score > best_score:
                best_score = score
                best_combo = (ce_pct, pe_pct)
                best_metrics = metrics

        if best_combo is None or best_metrics is None:
            return None

        # Store optimal band
        band_record = {
            "symbol": symbol,
            "instrument_type": instrument_type,
            "expiry_type": expiry_type,
            "analysis_period": period_name,
            "recommended_ce_pct": best_combo[0],
            "recommended_pe_pct": best_combo[1],
            "ce_win_rate": best_metrics.prob_ce_worthless,
            "pe_win_rate": best_metrics.prob_pe_worthless,
            "combined_win_rate": best_metrics.win_rate,
            "avg_profit": best_metrics.avg_profit,
            "avg_loss": best_metrics.avg_loss,
            "expected_value": best_metrics.expected_value,
            "sharpe_ratio": best_metrics.sharpe_ratio,
            "sortino_ratio": best_metrics.sortino_ratio,
            "calmar_ratio": best_metrics.calmar_ratio,
            "max_drawdown": best_metrics.max_drawdown,
            "profit_factor": best_metrics.profit_factor,
            "kelly_criterion": best_metrics.kelly_criterion,
            "probability_expire_worthless": best_metrics.combined_prob_worthless,
            "vix_regime": vix_regime,
            "market_regime": market_regime,
            "optimization_mode": optimization_mode,
        }

        await self._upsert_band(band_record)
        return band_record

    def _classify_vix(self, vix: float) -> str:
        """Classify VIX value into regime."""
        if vix < settings.vix_low_threshold:
            return "low"
        elif vix > settings.vix_high_threshold:
            return "high"
        return "medium"

    async def _upsert_band(self, record: dict) -> None:
        """Upsert an optimal selling band record."""
        self.db.add(OptimalSellingBand(**record))
