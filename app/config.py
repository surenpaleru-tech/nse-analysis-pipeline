"""
Pipeline configuration via pydantic-settings.
Simplified for the data pipeline — only database and ingestion settings.
"""

import os
from functools import lru_cache
from typing import Optional
from urllib.parse import urlparse, urlunparse, quote, unquote

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

# Load environment variables into os.environ
load_dotenv()


def _sanitize_db_url(url: str) -> str:
    """URL-encodes special characters in the password of a database connection URI."""
    try:
        parsed = urlparse(url)
        if not parsed.password:
            return url
        # Unquote first to avoid double encoding, then quote the password safely
        password = unquote(parsed.password)
        quoted_password = quote(password, safe="")
        
        # Build netloc: username:password@host:port
        netloc = parsed.username or ""
        if quoted_password:
            netloc += f":{quoted_password}"
        if parsed.hostname:
            netloc += f"@{parsed.hostname}"
            if parsed.port:
                netloc += f":{parsed.port}"
                
        parts = list(parsed)
        parts[1] = netloc
        return urlunparse(parts)
    except Exception:
        return url


class Settings(BaseSettings):
    """Pipeline settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # -------------------------------------------------------------------------
    # Application
    # -------------------------------------------------------------------------
    app_env: str = "development"
    app_debug: bool = True

    # -------------------------------------------------------------------------
    # PostgreSQL / Supabase
    # -------------------------------------------------------------------------
    database_url_override: Optional[str] = None  # Set via DATABASE_URL env var
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "nse_intelligence"
    postgres_user: str = "nse_admin"
    postgres_password: str = "change_me_in_production"

    @property
    def database_url(self) -> str:
        """
        Returns async database URL.
        Priority: DATABASE_URL env var > constructed from individual vars.
        Auto-converts postgresql:// to postgresql+asyncpg:// for Supabase.
        """
        raw_url = self.database_url_override or os.environ.get("DATABASE_URL")
        if raw_url:
            raw_url = _sanitize_db_url(raw_url.strip())
            # Supabase gives postgresql:// — convert to asyncpg driver
            if raw_url.startswith("postgresql://"):
                return raw_url.replace("postgresql://", "postgresql+asyncpg://", 1)
            if raw_url.startswith("postgres://"):
                return raw_url.replace("postgres://", "postgresql+asyncpg://", 1)
            return raw_url
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def database_url_sync(self) -> str:
        """Returns sync database URL."""
        raw_url = self.database_url_override or os.environ.get("DATABASE_URL")
        if raw_url:
            raw_url = _sanitize_db_url(raw_url.strip())
            if raw_url.startswith("postgresql+asyncpg://"):
                return raw_url.replace("postgresql+asyncpg://", "postgresql+psycopg2://", 1)
            if raw_url.startswith("postgresql://"):
                return raw_url.replace("postgresql://", "postgresql+psycopg2://", 1)
            if raw_url.startswith("postgres://"):
                return raw_url.replace("postgres://", "postgresql+psycopg2://", 1)
            return raw_url
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    # -------------------------------------------------------------------------
    # NSE Data Ingestion
    # -------------------------------------------------------------------------
    nse_data_source: str = "nse_scraper"
    ingestion_cron: str = "0 18 * * 1-5"
    nse_rate_limit: int = 3
    backfill_days: int = 365

    # -------------------------------------------------------------------------
    # Analytics
    # -------------------------------------------------------------------------
    vix_low_threshold: float = 15.0
    vix_high_threshold: float = 25.0
    market_bull_threshold: float = 3.0
    market_bear_threshold: float = -3.0
    otm_percentages: str = "1,2,3,4,5,6,7,8,9,10,12,15,20"

    @property
    def otm_pct_list(self) -> list[float]:
        return [float(x.strip()) for x in self.otm_percentages.split(",")]


@lru_cache()
def get_settings() -> Settings:
    """Cached settings instance."""
    return Settings()
