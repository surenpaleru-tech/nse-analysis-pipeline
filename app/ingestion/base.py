"""
Abstract data source interface.
All data collectors must implement this interface to support pluggable backends.
"""

from abc import ABC, abstractmethod
from datetime import date
from typing import Optional

import polars as pl


class DataSource(ABC):
    """Abstract base class for NSE data sources."""

    @abstractmethod
    async def fetch_derivatives_bhavcopy(self, trade_date: date) -> Optional[pl.DataFrame]:
        """
        Fetch F&O bhavcopy for a given date.
        Returns a Polars DataFrame with columns:
            INSTRUMENT, SYMBOL, EXPIRY_DT, STRIKE_PR, OPTION_TYP,
            OPEN, HIGH, LOW, CLOSE, SETTLE_PR, CONTRACTS, VAL_INLAKH,
            OPEN_INT, CHG_IN_OI, TIMESTAMP
        """
        ...

    @abstractmethod
    async def fetch_equity_bhavcopy(self, trade_date: date) -> Optional[pl.DataFrame]:
        """
        Fetch equity bhavcopy for spot prices.
        Returns a Polars DataFrame with OHLCV data.
        """
        ...

    @abstractmethod
    async def fetch_india_vix(self, trade_date: date) -> Optional[dict]:
        """
        Fetch India VIX data for a date.
        Returns dict with keys: date, open, high, low, close
        """
        ...

    @abstractmethod
    async def fetch_fno_symbols(self) -> list[dict]:
        """
        Fetch current F&O stock/index universe.
        Returns list of dicts with keys: symbol, instrument_type, lot_size
        """
        ...

    @abstractmethod
    async def is_available(self) -> bool:
        """Check if the data source is available/reachable."""
        ...
