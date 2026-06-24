"""OptionChain ORM model — complete derivatives data."""

from datetime import date, datetime

from sqlalchemy import BigInteger, Date, Index, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class OptionChain(Base):
    __tablename__ = "option_chain"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    symbol: Mapped[str] = mapped_column(String(50), nullable=False)
    expiry: Mapped[date] = mapped_column(Date, nullable=False)
    expiry_type: Mapped[str] = mapped_column(String(10), nullable=False)  # weekly | monthly
    strike: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    option_type: Mapped[str] = mapped_column(String(2), nullable=False)  # CE | PE
    open: Mapped[float | None] = mapped_column(Numeric(12, 2))
    high: Mapped[float | None] = mapped_column(Numeric(12, 2))
    low: Mapped[float | None] = mapped_column(Numeric(12, 2))
    close: Mapped[float | None] = mapped_column(Numeric(12, 2))
    volume: Mapped[int | None] = mapped_column(BigInteger)
    oi: Mapped[int | None] = mapped_column(BigInteger)
    change_oi: Mapped[int | None] = mapped_column(BigInteger)
    implied_volatility: Mapped[float | None] = mapped_column(Numeric(8, 4))
    underlying_price: Mapped[float | None] = mapped_column(Numeric(12, 2))
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "trade_date", "symbol", "expiry", "strike", "option_type",
            name="uq_oc_trade_symbol_expiry_strike_type",
        ),
        Index("idx_oc_symbol_expiry", "symbol", "expiry", "trade_date"),
        Index("idx_oc_trade_date", "trade_date"),
        Index("idx_oc_symbol_type", "symbol", "option_type", "expiry_type"),
        Index("idx_oc_expiry_type", "expiry_type"),
    )

    def __repr__(self) -> str:
        return (
            f"<OptionChain(symbol={self.symbol}, strike={self.strike}, "
            f"type={self.option_type}, date={self.trade_date})>"
        )
