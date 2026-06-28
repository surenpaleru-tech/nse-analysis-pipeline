"""FuturesChain ORM model - normalized futures bhavcopy rows."""

from datetime import date, datetime
from typing import Optional

from sqlalchemy import BigInteger, Date, Index, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class FuturesChain(Base):
    __tablename__ = "futures_chain"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    symbol: Mapped[str] = mapped_column(String(50), nullable=False)
    instrument: Mapped[str] = mapped_column(String(10), nullable=False)  # FUTIDX | FUTSTK
    instrument_type: Mapped[str] = mapped_column(String(10), nullable=False)  # index | stock
    expiry: Mapped[date] = mapped_column(Date, nullable=False)
    expiry_type: Mapped[str] = mapped_column(String(10), nullable=False, default="monthly")
    open: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    high: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    low: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    close: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    settle_price: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    volume: Mapped[Optional[int]] = mapped_column(BigInteger)
    turnover_lakh: Mapped[Optional[float]] = mapped_column(Numeric(14, 2))
    oi: Mapped[Optional[int]] = mapped_column(BigInteger)
    change_oi: Mapped[Optional[int]] = mapped_column(BigInteger)
    underlying_price: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "trade_date",
            "symbol",
            "expiry",
            "instrument",
            name="uq_fc_trade_symbol_expiry_instrument",
        ),
        Index("idx_fc_symbol_trade_date", "symbol", "trade_date"),
        Index("idx_fc_symbol_expiry", "symbol", "expiry"),
        Index("idx_fc_trade_date", "trade_date"),
    )

    def __repr__(self) -> str:
        return (
            f"<FuturesChain(symbol={self.symbol}, expiry={self.expiry}, "
            f"trade_date={self.trade_date}, close={self.close})>"
        )
