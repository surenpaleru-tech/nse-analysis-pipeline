"""
Expiry Calendar Manager — determines expiry dates and trading holidays.

Handles:
- Last Thursday of the month = monthly expiry
- Every Thursday = weekly expiry (for indices)
- NSE holiday calendar
- Actual expiry date shifting when Thursday is a holiday
"""

import calendar
from datetime import date, timedelta
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.models import Expiry, FnOUniverse
from app.core.logging import get_logger

logger = get_logger(__name__)

# NSE public holidays (approximate — update annually)
# Format: (month, day)
NSE_HOLIDAYS_2024 = {
    date(2024, 1, 26),  # Republic Day
    date(2024, 3, 25),  # Holi
    date(2024, 3, 29),  # Good Friday
    date(2024, 4, 14),  # Dr. Ambedkar Jayanti
    date(2024, 4, 17),  # Ram Navami
    date(2024, 4, 21),  # Mahavir Jayanti
    date(2024, 5, 23),  # Buddha Purnima
    date(2024, 6, 17),  # Eid ul-Adha
    date(2024, 7, 17),  # Muharram
    date(2024, 8, 15),  # Independence Day
    date(2024, 10, 2),  # Gandhi Jayanti
    date(2024, 10, 14), # Dussehra
    date(2024, 11, 1),  # Diwali (Laxmi Puja)
    date(2024, 11, 15), # Gurunanak Jayanti
    date(2024, 11, 20), # Maharashtra Day
    date(2024, 12, 25), # Christmas
}

NSE_HOLIDAYS_2025 = {
    date(2025, 2, 26),  # Maha Shivratri
    date(2025, 3, 14),  # Holi
    date(2025, 3, 31),  # Id-ul-Fitr (Ramzan Eid)
    date(2025, 4, 10),  # Good Friday (tentative)
    date(2025, 4, 14),  # Dr. Ambedkar Jayanti
    date(2025, 4, 18),  # Ram Navami
    date(2025, 5, 1),   # Maharashtra Day
    date(2025, 5, 12),  # Buddha Purnima
    date(2025, 8, 15),  # Independence Day
    date(2025, 8, 27),  # Ganesh Chaturthi
    date(2025, 10, 2),  # Gandhi Jayanti
    date(2025, 10, 2),  # Dussehra (tentative)
    date(2025, 10, 20), # Diwali (tentative)
    date(2025, 10, 21), # Diwali holiday
    date(2025, 11, 5),  # Gurunanak Jayanti
    date(2025, 12, 25), # Christmas
}

# Combine all holidays
ALL_HOLIDAYS = NSE_HOLIDAYS_2024 | NSE_HOLIDAYS_2025


class ExpiryCalendar:
    """Manages NSE expiry dates and trading calendar."""

    def __init__(self, db: AsyncSession):
        self.db = db

    def is_trading_day(self, d: date) -> bool:
        """Check if a date is a valid NSE trading day."""
        # Skip weekends
        if d.weekday() >= 5:
            return False
        # Skip known holidays
        if d in ALL_HOLIDAYS:
            return False
        return True

    def get_previous_trading_day(self, d: date) -> date:
        """Get the nearest previous trading day (exclusive of d)."""
        d -= timedelta(days=1)
        while not self.is_trading_day(d):
            d -= timedelta(days=1)
        return d

    def get_monthly_expiry(self, year: int, month: int) -> date:
        """
        Get NSE monthly expiry date.
        Rule: Last Thursday of the month. If that's a holiday, go to Wednesday, etc.
        """
        last_day = calendar.monthrange(year, month)[1]
        # Find last Thursday
        for day in range(last_day, last_day - 7, -1):
            candidate = date(year, month, day)
            if candidate.weekday() == 3:  # Thursday
                # If it's a holiday, move back
                while not self.is_trading_day(candidate):
                    candidate -= timedelta(days=1)
                return candidate
        # Fallback (should never happen)
        return date(year, month, last_day)

    def get_weekly_expiries(self, year: int, month: int) -> list[date]:
        """Get all weekly expiry dates (every Thursday) for a given month."""
        expiries = []
        monthly = self.get_monthly_expiry(year, month)

        # All Thursdays in the month
        for day in range(1, calendar.monthrange(year, month)[1] + 1):
            candidate = date(year, month, day)
            if candidate.weekday() == 3:  # Thursday
                # Adjust for holidays
                actual = candidate
                while not self.is_trading_day(actual):
                    actual -= timedelta(days=1)
                if actual not in expiries:
                    expiries.append(actual)

        return sorted(expiries)

    def get_expiry_type(self, expiry_date: date) -> str:
        """Determine if a date is a monthly or weekly expiry."""
        monthly = self.get_monthly_expiry(expiry_date.year, expiry_date.month)
        return "monthly" if expiry_date == monthly else "weekly"

    async def sync_expiries(
        self,
        start_year: int = 2019,
        end_year: Optional[int] = None,
        symbols: Optional[list[str]] = None,
    ) -> int:
        """
        Generate and store expiry calendar for all symbols.
        Returns count of records created.
        """
        if end_year is None:
            end_year = date.today().year + 1

        if symbols is None:
            # Get all active symbols
            result = await self.db.execute(
                select(FnOUniverse).where(FnOUniverse.is_active == True)
            )
            symbols = [r.symbol for r in result.scalars().all()]

        records = []
        index_symbols = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}

        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                monthly_expiry = self.get_monthly_expiry(year, month)
                weekly_expiries = self.get_weekly_expiries(year, month)

                for symbol in symbols:
                    is_index = symbol in index_symbols

                    # Monthly expiry for all symbols
                    records.append({
                        "symbol": symbol,
                        "expiry_date": monthly_expiry,
                        "expiry_type": "monthly",
                        "is_holiday": False,
                        "actual_date": monthly_expiry,
                    })

                    # Weekly expiries only for indices
                    if is_index:
                        for weekly in weekly_expiries:
                            if weekly != monthly_expiry:
                                records.append({
                                    "symbol": symbol,
                                    "expiry_date": weekly,
                                    "expiry_type": "weekly",
                                    "is_holiday": False,
                                    "actual_date": weekly,
                                })

        # Batch upsert
        if records:
            stmt = insert(Expiry).values(records)
            stmt = stmt.on_conflict_do_nothing()
            await self.db.execute(stmt)
            await self.db.flush()

        logger.info(f"Synced {len(records)} expiry records")
        return len(records)
