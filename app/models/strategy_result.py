"""StrategyResult ORM model — historical P&L for each CE/PE combination per expiry."""

from datetime import date, datetime
from typing import Optional

from sqlalchemy import BigInteger, Boolean, Date, Index, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class StrategyResult(Base):
    __tablename__ = "strategy_results"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(50), nullable=False)
    expiry: Mapped[date] = mapped_column(Date, nullable=False)
    expiry_type: Mapped[str] = mapped_column(String(10), nullable=False)
    ce_pct: Mapped[float] = mapped_column(Numeric(6, 2), nullable=False)
    pe_pct: Mapped[float] = mapped_column(Numeric(6, 2), nullable=False)
    ce_strike: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    pe_strike: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    spot_at_entry: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    spot_at_expiry: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    ce_entry_premium: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    pe_entry_premium: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    ce_expiry_premium: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    pe_expiry_premium: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    ce_pnl: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    pe_pnl: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    total_pnl: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    ce_expired_worthless: Mapped[Optional[bool]] = mapped_column(Boolean)
    pe_expired_worthless: Mapped[Optional[bool]] = mapped_column(Boolean)
    return_pct: Mapped[Optional[float]] = mapped_column(Numeric(8, 4))
    vix_at_entry: Mapped[Optional[float]] = mapped_column(Numeric(8, 4))
    market_regime: Mapped[Optional[str]] = mapped_column(String(15))
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("symbol", "expiry", "ce_pct", "pe_pct", name="uq_sr_symbol_expiry_pcts"),
        Index("idx_sr_symbol_expiry_type", "symbol", "expiry_type"),
        Index("idx_sr_regime", "market_regime", "symbol"),
    )

    def __repr__(self) -> str:
        return (
            f"<StrategyResult(symbol={self.symbol}, expiry={self.expiry}, "
            f"CE={self.ce_pct}%, PE={self.pe_pct}%, PnL={self.total_pnl})>"
        )
