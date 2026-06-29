"""
Backend test suite — unit and integration tests.
"""
import pytest
import asyncio
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch
import polars as pl


# =============================================================================
# Risk Metrics Tests
# =============================================================================

class TestRiskMetrics:
    """Tests for the risk metrics calculator."""

    def test_positive_pnl_series(self):
        from app.analytics.risk_metrics import calculate_risk_metrics
        pnl = [1.5, 2.0, -0.5, 1.8, 3.0, 0.5, -1.0, 2.5, 1.2, 0.8,
               1.9, 2.1, -0.3, 1.6, 2.8, 0.7, -0.8, 1.4, 2.3, 1.1]
        ce = [True] * 15 + [False] * 5
        pe = [True] * 14 + [False] * 6
        metrics = calculate_risk_metrics(pnl, ce, pe)
        assert metrics is not None
        assert metrics.total_trades == 20
        assert 0 <= metrics.win_rate <= 1
        assert metrics.max_drawdown >= 0
        assert isinstance(metrics.sharpe_ratio, float)
        assert isinstance(metrics.sortino_ratio, float)
        assert isinstance(metrics.kelly_criterion, float)

    def test_insufficient_data_returns_none(self):
        from app.analytics.risk_metrics import calculate_risk_metrics
        metrics = calculate_risk_metrics([], [], [])
        assert metrics is None

    def test_all_losses(self):
        from app.analytics.risk_metrics import calculate_risk_metrics
        pnl = [-1.0] * 10
        ce = [False] * 10
        pe = [False] * 10
        metrics = calculate_risk_metrics(pnl, ce, pe)
        assert metrics is not None
        assert metrics.win_rate == 0.0
        assert metrics.expected_value < 0

    def test_all_wins(self):
        from app.analytics.risk_metrics import calculate_risk_metrics
        pnl = [2.0] * 10
        ce = [True] * 10
        pe = [True] * 10
        metrics = calculate_risk_metrics(pnl, ce, pe)
        assert metrics is not None
        assert metrics.win_rate == 1.0
        assert metrics.prob_ce_worthless == 1.0
        assert metrics.combined_prob_worthless == 1.0

    def test_probability_bounds(self):
        from app.analytics.risk_metrics import calculate_risk_metrics
        import random
        random.seed(42)
        pnl = [random.gauss(1, 2) for _ in range(20)]
        ce = [random.random() > 0.2 for _ in range(20)]
        pe = [random.random() > 0.2 for _ in range(20)]
        metrics = calculate_risk_metrics(pnl, ce, pe)
        assert metrics is not None
        assert 0 <= metrics.prob_ce_worthless <= 1
        assert 0 <= metrics.prob_pe_worthless <= 1
        assert 0 <= metrics.combined_prob_worthless <= 1


# =============================================================================
# Expiry Calendar Tests
# =============================================================================

class TestExpiryCalendar:
    """Tests for the expiry calendar manager."""

    def setup_method(self):
        self.mock_db = AsyncMock()
        from app.ingestion.expiry_calendar import ExpiryCalendar
        self.calendar = ExpiryCalendar(self.mock_db)

    def test_last_thursday_of_month(self):
        # November 2024: last Thursday should be Nov 28
        expiry = self.calendar.get_monthly_expiry(2024, 11)
        assert expiry.weekday() == 3  # Thursday
        assert expiry.month == 11
        assert expiry.day == 28

    def test_monthly_expiry_not_a_weekend(self):
        for year in [2024, 2025]:
            for month in range(1, 13):
                expiry = self.calendar.get_monthly_expiry(year, month)
                assert expiry.weekday() < 5, f"{year}-{month} expiry is a weekend: {expiry}"

    def test_weekly_expiries_are_thursdays(self):
        expiries = self.calendar.get_weekly_expiries(2024, 11)
        for e in expiries:
            assert e.weekday() == 3, f"Weekly expiry {e} is not a Thursday"

    def test_trading_day_excludes_weekends(self):
        # Saturday
        assert not self.calendar.is_trading_day(date(2024, 11, 23))
        # Sunday
        assert not self.calendar.is_trading_day(date(2024, 11, 24))

    def test_trading_day_includes_monday(self):
        # Monday Nov 25 2024 (not a holiday)
        assert self.calendar.is_trading_day(date(2024, 11, 25))

    def test_expiry_type_classification(self):
        # Monthly expiry for Nov 2024 is Nov 28
        monthly = self.calendar.get_monthly_expiry(2024, 11)
        assert self.calendar.get_expiry_type(monthly) == "monthly"

        # Any other Thursday in the month should be "weekly"
        weeklies = self.calendar.get_weekly_expiries(2024, 11)
        for w in weeklies:
            if w != monthly:
                assert self.calendar.get_expiry_type(w) == "weekly"

    def test_previous_trading_day(self):
        # Previous trading day before Monday should be Friday
        monday = date(2024, 11, 25)
        prev = self.calendar.get_previous_trading_day(monday)
        assert prev.weekday() == 4  # Friday


# =============================================================================
# OTM Distance Calculator Tests
# =============================================================================

class TestOTMDistanceCalculator:
    """Tests for OTM distance calculation logic."""

    def test_find_nearest_strike(self):
        from app.analytics.otm_distance import OTMDistanceCalculator
        calc = OTMDistanceCalculator(AsyncMock())
        available = [24700, 24750, 24800, 24850, 24900]
        assert calc._find_nearest_strike(24780, available) == 24800
        assert calc._find_nearest_strike(24720, available) == 24700
        assert calc._find_nearest_strike(24850, available) == 24850

    def test_find_nearest_strike_empty_returns_none(self):
        from app.analytics.otm_distance import OTMDistanceCalculator
        calc = OTMDistanceCalculator(AsyncMock())
        assert calc._find_nearest_strike(24800, []) is None


# =============================================================================
# Data Validator Tests
# =============================================================================

class TestDataValidator:
    """Tests for the data validator."""

    def test_valid_vix(self):
        from app.ingestion.validator import DataValidator
        v = DataValidator()
        ok, issues = v.validate_vix(14.5)
        assert ok is True
        assert len(issues) == 0

    def test_invalid_vix_zero(self):
        from app.ingestion.validator import DataValidator
        v = DataValidator()
        ok, issues = v.validate_vix(0)
        assert ok is False

    def test_invalid_vix_none(self):
        from app.ingestion.validator import DataValidator
        v = DataValidator()
        ok, issues = v.validate_vix(None)
        assert ok is False

    def test_valid_spot_prices(self):
        from app.ingestion.validator import DataValidator
        v = DataValidator()
        ok, issues = v.validate_spot_prices({"NIFTY": 24856.5, "RELIANCE": 2945.0})
        assert ok is True

    def test_invalid_spot_price_negative(self):
        from app.ingestion.validator import DataValidator
        v = DataValidator()
        ok, issues = v.validate_spot_prices({"NIFTY": -100})
        assert ok is False

    def test_empty_spot_prices(self):
        from app.ingestion.validator import DataValidator
        v = DataValidator()
        ok, issues = v.validate_spot_prices({})
        assert ok is False


# =============================================================================
# Regime Classifier Tests
# =============================================================================

class TestRegimeClassifier:
    """Tests for the market regime classifier."""

    def setup_method(self):
        from app.analytics.regime_classifier import RegimeClassifier
        from app.config import get_settings
        settings = get_settings()
        self.clf = RegimeClassifier(AsyncMock())

    def test_low_vix(self):
        regime = self.clf.classify_vix_regime(12.0)
        assert regime == "low"

    def test_medium_vix(self):
        regime = self.clf.classify_vix_regime(20.0)
        assert regime == "medium"

    def test_high_vix(self):
        regime = self.clf.classify_vix_regime(28.0)
        assert regime == "high"

    def test_boundary_vix_low(self):
        # At exactly 15.0 it's medium
        regime = self.clf.classify_vix_regime(15.0)
        assert regime == "medium"

    def test_boundary_vix_high(self):
        # At exactly 25.0 it's medium
        regime = self.clf.classify_vix_regime(25.0)
        assert regime == "medium"


# =============================================================================
# NSE Scraper Tests (mocked)
# =============================================================================

class TestNSEScraper:
    """Tests for NSE scraper (mocked HTTP calls)."""

    @pytest.mark.asyncio
    async def test_scraper_returns_none_on_404(self):
        from app.ingestion.nse_scraper import NSEScraper
        scraper = NSEScraper()

        with patch("httpx.AsyncClient.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.status_code = 404
            mock_get.return_value = mock_resp
            result = await scraper.fetch_derivatives_bhavcopy(date(2024, 11, 25))
            # Should gracefully return None on 404
            assert result is None

        await scraper.close()


# =============================================================================
# Futures Collector Tests
# =============================================================================

class TestFuturesCollector:
    """Tests for futures bhavcopy normalization and upsert flow."""

    @pytest.mark.asyncio
    async def test_process_bhavcopy_extracts_futures_rows(self):
        from app.ingestion.futures_collector import FuturesCollector

        mock_db = AsyncMock()
        collector = FuturesCollector(mock_db)
        df = pl.DataFrame(
            {
                "INSTRUMENT": ["FUTIDX", "FUTSTK", "OPTSTK"],
                "SYMBOL": ["NIFTY", "RELIANCE", "RELIANCE"],
                "EXPIRY_DT": ["30-Jun-2026", "30-Jun-2026", "30-Jun-2026"],
                "OPEN": [25010.0, 1490.0, 12.0],
                "HIGH": [25120.0, 1512.0, 15.0],
                "LOW": [24920.0, 1478.0, 8.0],
                "CLOSE": [25055.0, 1504.0, 11.0],
                "SETTLE_PR": [25060.0, 1503.5, 10.8],
                "CONTRACTS": [1000, 450, 900],
                "VAL_INLAKH": [320000.0, 54000.0, 18000.0],
                "OPEN_INT": [120000, 43000, 55000],
                "CHG_IN_OI": [2000, -500, 1100],
            }
        )

        inserted = await collector.process_bhavcopy(
            df,
            date(2026, 6, 28),
            {"NIFTY": 25000.0, "RELIANCE": 1498.0},
        )

        assert inserted == 2
        assert mock_db.execute.await_count == 1

    @pytest.mark.asyncio
    async def test_is_available_false_on_connection_error(self):
        from app.ingestion.nse_scraper import NSEScraper
        scraper = NSEScraper()

        with patch("httpx.AsyncClient.get", side_effect=Exception("Connection error")):
            result = await scraper.is_available()
            assert result is False

        await scraper.close()

    @pytest.mark.asyncio
    async def test_fetch_fno_symbols_success(self):
        from app.ingestion.nse_scraper import NSEScraper
        scraper = NSEScraper()

        csv_content = (
            "UNDERLYING,SYMBOL,JUN-26\n"
            "NIFTY 50,NIFTY,65\n"
            "NIFTY BANK,BANKNIFTY,30\n"
            "Derivatives on Individual Securities,Symbol,JUN-26\n"
            "ABB INDIA LIMITED,ABB,125\n"
        )

        with patch.object(scraper, "_rate_limited_request") as mock_req:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.text = csv_content
            mock_req.return_value = mock_resp
            
            result = await scraper.fetch_fno_symbols()
            assert len(result) == 3
            assert result[0] == {"symbol": "NIFTY", "instrument_type": "index", "lot_size": 65}
            assert result[1] == {"symbol": "BANKNIFTY", "instrument_type": "index", "lot_size": 30}
            assert result[2] == {"symbol": "ABB", "instrument_type": "stock", "lot_size": 125}

        await scraper.close()


# =============================================================================
# Pipeline DB Guard Tests
# =============================================================================

class TestPipelineDatabaseGuards:
    """Tests for early writable-connection validation."""

    def test_describe_database_target_redacts_credentials(self):
        from app.scheduler.jobs import _describe_database_target

        target = _describe_database_target(
            "postgresql+asyncpg://postgres:super-secret@db.example.supabase.co:5432/postgres"
        )

        assert target == "db.example.supabase.co:5432/postgres"

    @pytest.mark.asyncio
    async def test_ensure_writable_connection_accepts_writable_session(self):
        from app.scheduler.jobs import ensure_writable_connection

        mock_connection = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = "off"
        mock_connection.execute.return_value = mock_result

        await ensure_writable_connection(mock_connection, "test run")

    @pytest.mark.asyncio
    async def test_ensure_writable_connection_rejects_read_only_session(self):
        from app.scheduler.jobs import ensure_writable_connection

        mock_connection = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = "on"
        mock_connection.execute.return_value = mock_result

        with patch(
            "app.config.get_settings",
            return_value=MagicMock(
                database_url=(
                    "postgresql+asyncpg://postgres:secret@db.example.supabase.co:5432/postgres"
                )
            ),
        ):
            with pytest.raises(RuntimeError, match="read-only"):
                await ensure_writable_connection(mock_connection, "analytics recompute")
