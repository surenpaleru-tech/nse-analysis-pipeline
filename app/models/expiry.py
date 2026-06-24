"""Expiry ORM model — expiry calendar with holiday adjustments."""

from datetime import date, datetime
from typing import Optional

from sqlalchemy import Boolean, Date, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class Expiry(Base):
    __tablename__ = "expiries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(50), nullable=False)
    expiry_date: Mapped[date] = mapped_column(Date, nullable=False)
    expiry_type: Mapped[str] = mapped_column(String(10), nullable=False)  # weekly | monthly
    is_holiday: Mapped[bool] = mapped_column(Boolean, default=False)
    actual_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("symbol", "expiry_date", "expiry_type", name="uq_expiry_symbol_date_type"),
        Index("idx_expiries_symbol", "symbol", "expiry_date"),
    )

    def __repr__(self) -> str:
        return f"<Expiry(symbol={self.symbol}, date={self.expiry_date}, type={self.expiry_type})>"
