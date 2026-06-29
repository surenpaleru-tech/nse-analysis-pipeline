"""DailyRecommendation ORM model — daily trading recommendations."""

from datetime import date, datetime
from typing import Optional

from sqlalchemy import BigInteger, Boolean, Date, Index, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class DailyRecommendation(Base):
    __tablename__ = "daily_recommendations"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    date: Mapped[date] = mapped_column(Date, nullable=False)
    symbol: Mapped[str] = mapped_column(String(50), nullable=False)
    instrument_type: Mapped[str] = mapped_column(String(10), nullable=False)
    expiry_type: Mapped[str] = mapped_column(String(10), nullable=False)
    spot_price: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    recommended_ce_pct: Mapped[Optional[float]] = mapped_column(Numeric(6, 2))
    recommended_pe_pct: Mapped[Optional[float]] = mapped_column(Numeric(6, 2))
    recommended_ce_strike: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    recommended_pe_strike: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    ce_probability: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    pe_probability: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    combined_probability: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    expected_return: Mapped[Optional[float]] = mapped_column(Numeric(8, 4))
    risk_score: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    vix_at_recommendation: Mapped[Optional[float]] = mapped_column(Numeric(8, 4))
    market_regime: Mapped[Optional[str]] = mapped_column(String(15), nullable=True)
    alert_generated: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("date", "symbol", "expiry_type", name="uq_dr_date_symbol_expiry"),
        Index("idx_dr_date", "date"),
        Index("idx_dr_symbol", "symbol", "date"),
    )

    def __repr__(self) -> str:
        return (
            f"<DailyRecommendation(symbol={self.symbol}, date={self.date}, "
            f"CE={self.recommended_ce_pct}%, PE={self.recommended_pe_pct}%)>"
        )
