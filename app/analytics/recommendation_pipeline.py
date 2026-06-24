"""
Daily Recommendation Pipeline — generates DailyRecommendation rows from optimal bands.

Runs after band optimization and produces actionable sell recommendations
incorporating current spot prices and today's market regime.
"""

from datetime import date
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert

from app.models import (
    OptimalSellingBand,
    DailyRecommendation,
    FnOUniverse,
    SpotPrice,
    IndiaVIX,
)
from app.analytics.regime_classifier import RegimeClassifier
from app.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

INDEX_SYMBOLS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}


class DailyRecommendationPipeline:
    """Generates daily option selling recommendations from optimal bands."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.regime_clf = RegimeClassifier(db)

    async def run(self, trade_date: Optional[date] = None) -> int:
        """
        Generate today's recommendations for all active F&O symbols.

        Returns:
            Number of recommendations generated.
        """
        if trade_date is None:
            trade_date = date.today()

        logger.info(f"Running daily recommendation pipeline for {trade_date}")

        # Get VIX for today
        vix = await self.regime_clf.get_vix_for_date(trade_date)
        vix_regime = self.regime_clf.classify_vix_regime(vix) if vix else "medium"

        # Get all active F&O symbols
        fno_result = await self.db.execute(
            select(FnOUniverse).where(FnOUniverse.is_active == True)
        )
        symbols = fno_result.scalars().all()

        records = []
        for fno in symbols:
            symbol = fno.symbol
            instrument_type = fno.instrument_type

            # Get current spot price
            spot_result = await self.db.execute(
                select(SpotPrice.close)
                .where(SpotPrice.symbol == symbol, SpotPrice.date == trade_date)
            )
            spot_price = spot_result.scalar_one_or_none()

            if not spot_price:
                # Use most recent available price
                spot_result = await self.db.execute(
                    select(SpotPrice.close)
                    .where(SpotPrice.symbol == symbol)
                    .order_by(SpotPrice.date.desc())
                    .limit(1)
                )
                spot_price = spot_result.scalar_one_or_none()

            if not spot_price:
                continue

            spot_price = float(spot_price)

            # Get market regime
            market_regime = await self.regime_clf.classify_market_regime(symbol, trade_date)

            # Determine expiry types
            expiry_types = ["monthly"]
            if instrument_type == "index":
                expiry_types.append("weekly")

            for expiry_type in expiry_types:
                rec = await self._build_recommendation(
                    symbol=symbol,
                    instrument_type=instrument_type,
                    expiry_type=expiry_type,
                    trade_date=trade_date,
                    spot_price=spot_price,
                    vix=vix,
                    vix_regime=vix_regime,
                    market_regime=market_regime,
                )
                if rec:
                    records.append(rec)

        if records:
            stmt = insert(DailyRecommendation).values(records)
            stmt = stmt.on_conflict_do_update(
                index_elements=["date", "symbol", "expiry_type"],
                set_={
                    "spot_price": stmt.excluded.spot_price,
                    "recommended_ce_pct": stmt.excluded.recommended_ce_pct,
                    "recommended_pe_pct": stmt.excluded.recommended_pe_pct,
                    "recommended_ce_strike": stmt.excluded.recommended_ce_strike,
                    "recommended_pe_strike": stmt.excluded.recommended_pe_strike,
                    "ce_probability": stmt.excluded.ce_probability,
                    "pe_probability": stmt.excluded.pe_probability,
                    "combined_probability": stmt.excluded.combined_probability,
                    "expected_return": stmt.excluded.expected_return,
                    "risk_score": stmt.excluded.risk_score,
                    "vix_at_recommendation": stmt.excluded.vix_at_recommendation,
                    "market_regime": stmt.excluded.market_regime,
                },
            )
            await self.db.execute(stmt)
            await self.db.flush()

        logger.info(
            f"Generated {len(records)} recommendations for {trade_date}",
            vix=vix,
            vix_regime=vix_regime,
        )
        return len(records)

    async def _build_recommendation(
        self,
        symbol: str,
        instrument_type: str,
        expiry_type: str,
        trade_date: date,
        spot_price: float,
        vix: Optional[float],
        vix_regime: str,
        market_regime: str,
    ) -> Optional[dict]:
        """Build a recommendation dict by picking the best optimal band."""
        # Query optimal bands — prefer regime-specific, fall back to overall
        # Try: vix_regime + market_regime → vix_regime only → overall
        band = None
        for (vr, mr) in [
            (vix_regime, market_regime),
            (vix_regime, None),
            (None, market_regime),
            (None, None),
        ]:
            query = (
                select(OptimalSellingBand)
                .where(
                    OptimalSellingBand.symbol == symbol,
                    OptimalSellingBand.expiry_type == expiry_type,
                    OptimalSellingBand.optimization_mode == "expected_value",
                    OptimalSellingBand.analysis_period == "1y",
                )
            )
            if vr is not None:
                query = query.where(OptimalSellingBand.vix_regime == vr)
            else:
                query = query.where(OptimalSellingBand.vix_regime.is_(None))
            if mr is not None:
                query = query.where(OptimalSellingBand.market_regime == mr)
            else:
                query = query.where(OptimalSellingBand.market_regime.is_(None))

            result = await self.db.execute(query)
            band = result.scalar_one_or_none()
            if band:
                break

        if not band:
            return None

        ce_pct = float(band.recommended_ce_pct) if band.recommended_ce_pct else None
        pe_pct = float(band.recommended_pe_pct) if band.recommended_pe_pct else None

        ce_strike = round(spot_price * (1 + ce_pct / 100)) if ce_pct else None
        pe_strike = round(spot_price * (1 - pe_pct / 100)) if pe_pct else None

        # Risk score: lower is better — inverse of Sharpe if available
        sharpe = float(band.sharpe_ratio) if band.sharpe_ratio else 0
        risk_score = max(0, 1 - min(sharpe / 3, 1)) if sharpe > 0 else 0.5

        return {
            "date": trade_date,
            "symbol": symbol,
            "instrument_type": instrument_type,
            "expiry_type": expiry_type,
            "spot_price": spot_price,
            "recommended_ce_pct": ce_pct,
            "recommended_pe_pct": pe_pct,
            "recommended_ce_strike": ce_strike,
            "recommended_pe_strike": pe_strike,
            "ce_probability": float(band.ce_win_rate) if band.ce_win_rate else None,
            "pe_probability": float(band.pe_win_rate) if band.pe_win_rate else None,
            "combined_probability": float(band.combined_win_rate) if band.combined_win_rate else None,
            "expected_return": float(band.expected_value) if band.expected_value else None,
            "risk_score": risk_score,
            "vix_at_recommendation": vix,
            "market_regime": market_regime,
            "alert_generated": False,
        }
