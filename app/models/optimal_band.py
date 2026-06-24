"""OptimalSellingBand ORM model — best CE/PE selling percentages."""

from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class OptimalSellingBand(Base):
    __tablename__ = "optimal_selling_bands"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(50), nullable=False)
    instrument_type: Mapped[str] = mapped_column(String(10), nullable=False)
    expiry_type: Mapped[str] = mapped_column(String(10), nullable=False)
    analysis_period: Mapped[str] = mapped_column(String(20), nullable=False)
    recommended_ce_pct: Mapped[Optional[float]] = mapped_column(Numeric(6, 2))
    recommended_pe_pct: Mapped[Optional[float]] = mapped_column(Numeric(6, 2))
    ce_win_rate: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    pe_win_rate: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    combined_win_rate: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    avg_profit: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    avg_loss: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    expected_value: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    sharpe_ratio: Mapped[Optional[float]] = mapped_column(Numeric(8, 4))
    sortino_ratio: Mapped[Optional[float]] = mapped_column(Numeric(8, 4))
    calmar_ratio: Mapped[Optional[float]] = mapped_column(Numeric(8, 4))
    max_drawdown: Mapped[Optional[float]] = mapped_column(Numeric(8, 4))
    profit_factor: Mapped[Optional[float]] = mapped_column(Numeric(8, 4))
    kelly_criterion: Mapped[Optional[float]] = mapped_column(Numeric(8, 4))
    probability_expire_worthless: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    recommended_ce_strike_offset: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    recommended_pe_strike_offset: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    vix_regime: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    market_regime: Mapped[Optional[str]] = mapped_column(String(15), nullable=True)
    optimization_mode: Mapped[str] = mapped_column(String(20), default="expected_value")
    last_updated: Mapped[datetime] = mapped_column(default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self) -> str:
        return (
            f"<OptimalSellingBand(symbol={self.symbol}, CE={self.recommended_ce_pct}%, "
            f"PE={self.recommended_pe_pct}%, mode={self.optimization_mode})>"
        )
