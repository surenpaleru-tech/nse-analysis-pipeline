"""SpotPrice ORM model — daily OHLCV for indices and stocks."""

from datetime import date, datetime

from sqlalchemy import BigInteger, Date, Index, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class SpotPrice(Base):
    __tablename__ = "spot_prices"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    date: Mapped[date] = mapped_column(Date, nullable=False)
    symbol: Mapped[str] = mapped_column(String(50), nullable=False)
    open: Mapped[float | None] = mapped_column(Numeric(12, 2))
    high: Mapped[float | None] = mapped_column(Numeric(12, 2))
    low: Mapped[float | None] = mapped_column(Numeric(12, 2))
    close: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    volume: Mapped[int | None] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("date", "symbol", name="uq_spot_date_symbol"),
        Index("idx_spot_symbol_date", "symbol", "date"),
    )

    def __repr__(self) -> str:
        return f"<SpotPrice(symbol={self.symbol}, date={self.date}, close={self.close})>"
