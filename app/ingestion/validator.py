"""
Data validation for ingested NSE data.
"""

from datetime import date
from typing import Optional

import polars as pl

from app.core.logging import get_logger
from app.core.exceptions import DataValidationError

logger = get_logger(__name__)


class DataValidator:
    """Validates ingested data for quality and consistency."""

    def validate_bhavcopy(self, df: pl.DataFrame, trade_date: date) -> tuple[bool, list[str]]:
        """
        Validate a bhavcopy DataFrame.
        Returns (is_valid, list_of_issues).
        """
        issues = []

        if df is None or len(df) == 0:
            return False, ["Empty DataFrame"]

        # Check required columns exist
        required_cols = {"INSTRUMENT", "SYMBOL", "EXPIRY_DT", "STRIKE_PR", "OPTION_TYP"}
        # Normalize column names for checking
        actual_cols = {c.strip().upper() for c in df.columns}
        missing = required_cols - actual_cols
        if missing:
            issues.append(f"Missing columns: {missing}")

        # Check for reasonable row count
        if len(df) < 100:
            issues.append(f"Suspiciously low row count: {len(df)}")

        # Check for null symbols
        normalized = df.rename({c: c.strip().upper() for c in df.columns})
        if "SYMBOL" in normalized.columns:
            null_symbols = normalized.filter(pl.col("SYMBOL").is_null()).height
            if null_symbols > 0:
                issues.append(f"Found {null_symbols} rows with null SYMBOL")

        # Check for negative prices
        for col in ["OPEN", "HIGH", "LOW", "CLOSE"]:
            if col in normalized.columns:
                negatives = normalized.filter(pl.col(col).cast(pl.Float64, strict=False) < 0).height
                if negatives > 0:
                    issues.append(f"Found {negatives} negative values in {col}")

        is_valid = len(issues) == 0
        if not is_valid:
            logger.warning(
                "Bhavcopy validation issues",
                date=str(trade_date),
                issues=issues,
            )
        else:
            logger.info(f"Bhavcopy validation passed", date=str(trade_date))

        return is_valid, issues

    def validate_spot_prices(self, prices: dict[str, float]) -> tuple[bool, list[str]]:
        """Validate spot price data."""
        issues = []

        if not prices:
            return False, ["No spot prices"]

        for symbol, price in prices.items():
            if price <= 0:
                issues.append(f"{symbol}: non-positive price {price}")
            if price > 500000:  # Sanity check
                issues.append(f"{symbol}: suspiciously high price {price}")

        return len(issues) == 0, issues

    def validate_vix(self, vix_value: Optional[float]) -> tuple[bool, list[str]]:
        """Validate VIX value."""
        if vix_value is None:
            return False, ["VIX value is None"]
        if vix_value <= 0 or vix_value > 100:
            return False, [f"VIX value out of range: {vix_value}"]
        return True, []
