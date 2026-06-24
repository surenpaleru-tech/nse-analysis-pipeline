"""FnOUniverse ORM model — F&O stock/index universe tracker."""

from datetime import date, datetime
from typing import Optional

from sqlalchemy import Boolean, Date, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class FnOUniverse(Base):
    __tablename__ = "fno_universe"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(50), nullable=False)
    instrument_type: Mapped[str] = mapped_column(String(10), nullable=False)  # index | stock
    lot_size: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    added_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    removed_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<FnOUniverse(symbol={self.symbol}, type={self.instrument_type}, active={self.is_active})>"
