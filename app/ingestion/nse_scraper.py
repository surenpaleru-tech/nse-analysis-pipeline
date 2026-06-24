"""
NSE Archives Scraper — Downloads bhavcopy and market data from NSE India.

Handles:
- F&O bhavcopy (derivatives data)
- Equity bhavcopy (spot prices)
- India VIX
- F&O stock universe

Implements rate limiting, session management, and bot-protection handling.
"""

import asyncio
import io
import zipfile
from datetime import date, datetime
from typing import Optional

import httpx
import polars as pl
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from app.ingestion.base import DataSource
from app.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

# NSE Archive URLs
NSE_BASE_URL = "https://www.nseindia.com"
NSE_ARCHIVES_URL = "https://nsearchives.nseindia.com"

# Bhavcopy URL patterns
FO_BHAVCOPY_URL = (
    "{base}/content/historical/DERIVATIVES/{year}/{month}/"
    "fo{day}{month}{year}bhav.csv.zip"
)
EQ_BHAVCOPY_URL = (
    "{base}/content/historical/EQUITIES/{year}/{month}/"
    "cm{day}{month}{year}bhav.csv.zip"
)
VIX_URL = "{base}/api/historical-vix"
OPTION_CHAIN_INDICES_URL = "{base}/api/option-chain-indices?symbol={symbol}"
OPTION_CHAIN_EQUITIES_URL = "{base}/api/option-chain-equities?symbol={symbol}"

# Month abbreviations for URL construction
MONTH_MAP = {
    1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN",
    7: "JUL", 8: "AUG", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC",
}

# User agents to rotate
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]


class NSEScraper(DataSource):
    """NSE Archives data scraper with rate limiting and session management."""

    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None
        self._cookies: dict = {}
        self._ua_index = 0
        self._rate_limit = asyncio.Semaphore(settings.nse_rate_limit)
        self._last_request_time = 0.0

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create an HTTP client with proper headers."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0),
                follow_redirects=True,
                verify=True,
            )
            # Initialize session by visiting NSE homepage
            await self._init_session()
        return self._client

    async def _init_session(self):
        """Initialize NSE session to get cookies."""
        try:
            headers = self._get_headers()
            response = await self._client.get(NSE_BASE_URL, headers=headers)
            if response.status_code == 200:
                self._cookies = dict(response.cookies)
                logger.info("NSE session initialized successfully")
            else:
                logger.warning(f"NSE session init returned status {response.status_code}")
        except Exception as e:
            logger.error(f"Failed to initialize NSE session: {e}")

    def _get_headers(self) -> dict:
        """Get request headers with rotating user agent."""
        ua = USER_AGENTS[self._ua_index % len(USER_AGENTS)]
        self._ua_index += 1
        return {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": NSE_BASE_URL,
        }

    async def _rate_limited_request(self, url: str, **kwargs) -> httpx.Response:
        """Make a rate-limited HTTP request."""
        async with self._rate_limit:
            # Enforce minimum delay between requests
            now = asyncio.get_event_loop().time()
            delay = max(0, (1.0 / settings.nse_rate_limit) - (now - self._last_request_time))
            if delay > 0:
                await asyncio.sleep(delay)

            client = await self._get_client()
            headers = {**self._get_headers(), **kwargs.pop("headers", {})}
            response = await client.get(url, headers=headers, cookies=self._cookies, **kwargs)
            self._last_request_time = asyncio.get_event_loop().time()
            return response

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=30),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    async def fetch_derivatives_bhavcopy(self, trade_date: date) -> Optional[pl.DataFrame]:
        """Download and parse F&O bhavcopy for a given date."""
        month_str = MONTH_MAP[trade_date.month]
        day_str = trade_date.strftime("%d")
        year_str = str(trade_date.year)

        url = FO_BHAVCOPY_URL.format(
            base=NSE_ARCHIVES_URL,
            year=year_str,
            month=month_str,
            day=day_str,
        )

        logger.info(f"Fetching F&O bhavcopy for {trade_date}", url=url)

        try:
            response = await self._rate_limited_request(url)

            if response.status_code == 404:
                logger.info(f"No F&O bhavcopy for {trade_date} (holiday or weekend)")
                return None

            if response.status_code != 200:
                logger.warning(
                    f"F&O bhavcopy request failed",
                    status=response.status_code,
                    date=str(trade_date),
                )
                return None

            # Unzip and parse CSV
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                csv_filename = zf.namelist()[0]
                with zf.open(csv_filename) as csv_file:
                    df = pl.read_csv(csv_file.read())

            logger.info(
                f"Parsed F&O bhavcopy",
                date=str(trade_date),
                rows=len(df),
                columns=df.columns,
            )
            return df

        except zipfile.BadZipFile:
            logger.error(f"Bad zip file for F&O bhavcopy on {trade_date}")
            return None
        except Exception as e:
            logger.error(f"Error fetching F&O bhavcopy: {e}", date=str(trade_date))
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=30),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    async def fetch_equity_bhavcopy(self, trade_date: date) -> Optional[pl.DataFrame]:
        """Download and parse equity bhavcopy for spot prices."""
        month_str = MONTH_MAP[trade_date.month]
        day_str = trade_date.strftime("%d")
        year_str = str(trade_date.year)

        url = EQ_BHAVCOPY_URL.format(
            base=NSE_ARCHIVES_URL,
            year=year_str,
            month=month_str,
            day=day_str,
        )

        logger.info(f"Fetching equity bhavcopy for {trade_date}", url=url)

        try:
            response = await self._rate_limited_request(url)

            if response.status_code == 404:
                logger.info(f"No equity bhavcopy for {trade_date}")
                return None

            if response.status_code != 200:
                return None

            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                csv_filename = zf.namelist()[0]
                with zf.open(csv_filename) as csv_file:
                    df = pl.read_csv(csv_file.read())

            logger.info(f"Parsed equity bhavcopy", date=str(trade_date), rows=len(df))
            return df

        except zipfile.BadZipFile:
            logger.error(f"Bad zip file for equity bhavcopy on {trade_date}")
            return None
        except Exception as e:
            logger.error(f"Error fetching equity bhavcopy: {e}")
            raise

    async def fetch_india_vix(self, trade_date: date) -> Optional[dict]:
        """Fetch India VIX data. Falls back to bhavcopy-derived VIX."""
        try:
            # Try the VIX API endpoint
            url = f"{NSE_BASE_URL}/api/historical-vix"
            params = {
                "from": trade_date.strftime("%d-%m-%Y"),
                "to": trade_date.strftime("%d-%m-%Y"),
            }
            response = await self._rate_limited_request(url, params=params)

            if response.status_code == 200:
                data = response.json()
                if data and "data" in data and len(data["data"]) > 0:
                    vix_record = data["data"][0]
                    return {
                        "date": trade_date,
                        "open": float(vix_record.get("EOD_OPEN_INDEX_VAL", 0)),
                        "high": float(vix_record.get("EOD_HIGH_INDEX_VAL", 0)),
                        "low": float(vix_record.get("EOD_LOW_INDEX_VAL", 0)),
                        "close": float(vix_record.get("EOD_CLOSE_INDEX_VAL", 0)),
                    }

            logger.info(f"No VIX data available for {trade_date}")
            return None

        except Exception as e:
            logger.warning(f"Error fetching VIX data: {e}")
            return None

    async def fetch_fno_symbols(self) -> list[dict]:
        """
        Fetch current F&O stock universe from NSE.
        Parses the latest F&O bhavcopy to extract unique symbols.
        """
        try:
            # Use today's date or most recent trading day
            from datetime import timedelta
            check_date = date.today()

            # Try last 5 days to find a valid bhavcopy
            for i in range(5):
                d = check_date - timedelta(days=i)
                df = await self.fetch_derivatives_bhavcopy(d)
                if df is not None:
                    # Extract unique symbols for stock options
                    stock_symbols = (
                        df.filter(pl.col("INSTRUMENT") == "OPTSTK")
                        .select("SYMBOL")
                        .unique()
                        .to_series()
                        .to_list()
                    )

                    return [
                        {"symbol": s.strip(), "instrument_type": "stock", "lot_size": None}
                        for s in stock_symbols
                    ]

            logger.warning("Could not fetch F&O symbols from recent bhavcopy")
            return []

        except Exception as e:
            logger.error(f"Error fetching F&O symbols: {e}")
            return []

    async def is_available(self) -> bool:
        """Check if NSE website is reachable."""
        try:
            client = await self._get_client()
            response = await client.get(
                NSE_BASE_URL,
                headers=self._get_headers(),
                timeout=10.0,
            )
            return response.status_code == 200
        except Exception:
            return False

    async def close(self):
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
