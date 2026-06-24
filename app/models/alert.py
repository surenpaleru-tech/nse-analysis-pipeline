"""Alert ORM model — notification tracking."""

from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, Boolean, ForeignKey, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class Alert(Base):
    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    user_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=True
    )
    symbol: Mapped[str] = mapped_column(String(50), nullable=False)
    alert_type: Mapped[str] = mapped_column(String(30), nullable=False)
    title: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    channel: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    is_sent: Mapped[bool] = mapped_column(Boolean, default=False)
    sent_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    __table_args__ = (
        Index("idx_alerts_user", "user_id", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<Alert(symbol={self.symbol}, type={self.alert_type}, sent={self.is_sent})>"
