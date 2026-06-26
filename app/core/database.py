"""
Database engine, session factory, and base model for SQLAlchemy 2.0 async.
"""

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from typing import Any
from app.config import get_settings

settings = get_settings()

connect_args: dict[str, Any] = {"statement_cache_size": 0}  # Required for Supabase/PgBouncer

# Enable SSL for remote connections (Supabase/Render Postgres)
db_url_lower = settings.database_url.lower()
if not any(local_host in db_url_lower for local_host in ["localhost", "127.0.0.1", "db", "postgres"]):
    connect_args["ssl"] = "require"

# Async engine
engine = create_async_engine(
    settings.database_url,
    echo=settings.app_debug,
    pool_size=5,
    max_overflow=5,
    pool_pre_ping=True,
    pool_recycle=3600,
    connect_args=connect_args,
)

# Session factory
async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    """Base class for all ORM models."""
    pass


async def get_db():
    """Dependency that yields a database session."""
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
