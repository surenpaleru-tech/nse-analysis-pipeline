"""IndiaVIX ORM model — daily India VIX values."""

from datetime import date, datetime

from sqlalchemy import Date, Integer, Numeric
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class IndiaVIX(Base):
    __tablename__ = "india_vix"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[date] = mapped_column(Date, nullable=False, unique=True)
    open: Mapped[float | None] = mapped_column(Numeric(8, 4))
    high: Mapped[float | None] = mapped_column(Numeric(8, 4))
    low: Mapped[float | None] = mapped_column(Numeric(8, 4))
    close: Mapped[float] = mapped_column(Numeric(8, 4), nullable=False)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<IndiaVIX(date={self.date}, close={self.close})>"
