"""ChatHistory ORM model — conversation logs."""

import uuid
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import BigInteger, ForeignKey, Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class ChatHistory(Base):
    __tablename__ = "chat_history"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    user_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    session_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), default=uuid.uuid4)
    role: Mapped[str] = mapped_column(String(10), nullable=False)  # user | assistant | system
    content: Mapped[str] = mapped_column(Text, nullable=False)
    sql_query: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    chat_metadata: Mapped[Optional[dict[str, Any]]] = mapped_column("metadata", JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    __table_args__ = (
        Index("idx_chat_session", "session_id", "created_at"),
        Index("idx_chat_user", "user_id", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<ChatHistory(session={self.session_id}, role={self.role})>"
