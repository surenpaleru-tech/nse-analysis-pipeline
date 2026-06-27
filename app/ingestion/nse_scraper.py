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


def _normalize_udiff_df(df: pl.DataFrame, is_fo: bool = False) -> pl.DataFrame:
    """
    Normalizes UDiFF (new NSE format) columns and values to match the legacy format.
    If the dataframe is already in legacy format, it leaves it unchanged.
    """
    # 1. Column renaming
    col_map = {
        "TradDt": "TIMESTAMP",
        "TckrSymb": "SYMBOL",
        "SctySrs": "SERIES",
        "FinInstrmTp": "INSTRUMENT",
        "XpryDt": "EXPIRY_DT",
        "StrkPric": "STRIKE_PR",
        "OptnTp": "OPTION_TYP",
        "OpnPric": "OPEN",
        "HghPric": "HIGH",
        "LwPric": "LOW",
        "ClsPric": "CLOSE",
        "SttlmPric": "SETTLE_PR",
        "TtlTradgVol": "CONTRACTS" if is_fo else "TOTTRDQTY",
        "TtlTrfVal": "VAL_INLAKH",
        "OpnIntrst": "OPEN_INT",
        "ChngInOpnIntrst": "CHG_IN_OI",
        "UndrlygPric": "UNDERLYING_VAL",
    }
    
    rename_dict = {}
    for old_col, new_col in col_map.items():
        # Match case-insensitively
        matching_cols = [c for c in df.columns if c.lower().strip() == old_col.lower()]
        if matching_cols:
            rename_dict[matching_cols[0]] = new_col
            
    if rename_dict:
        df = df.rename(rename_dict)
        
    # 2. Value mapping for INSTRUMENT column if it exists and is F&O
    if is_fo and "INSTRUMENT" in df.columns:
        # IDO -> OPTIDX
        # STO -> OPTSTK
        # IDF -> FUTIDX
        # STF -> FUTSTK
        inst_map = {
            "IDO": "OPTIDX",
            "STO": "OPTSTK",
            "IDF": "FUTIDX",
            "STF": "FUTSTK"
        }
        df = df.with_columns(
            pl.col("INSTRUMENT").replace(inst_map, default=pl.col("INSTRUMENT"))
        )
        
    return df


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
            # Try to initialize session — but don't block if it fails.
            # The archive CDN (nsearchives.nseindia.com) works independently
            # from the main website (www.nseindia.com), so we can still
            # download bhavcopies even if session init returns 403.
            await self._init_session()
        return self._client

    async def _init_session(self):
        """Initialize NSE session to get cookies (best-effort)."""
        try:
            headers = self._get_headers()
            response = await self._client.get(NSE_BASE_URL, headers=headers)
            if response.status_code == 200:
                self._cookies = dict(response.cookies)
                logger.info("NSE session initialized successfully")
            else:
                # 403 is common from cloud IPs — archive downloads still work
                logger.warning(
                    f"NSE session init returned status {response.status_code} "
                    f"(archive downloads may still work without cookies)"
                )
        except Exception as e:
            logger.warning(f"NSE session init failed: {e} (continuing without session cookies)")

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
        month_num = trade_date.strftime("%m")
        day_str = trade_date.strftime("%d")
        year_str = str(trade_date.year)

        # Build new UDiFF URL
        udiff_url = f"{NSE_ARCHIVES_URL}/content/fo/BhavCopy_NSE_FO_0_0_0_{year_str}{month_num}{day_str}_F_0000.csv.zip"

        # Build legacy URL as fallback
        month_str = MONTH_MAP[trade_date.month]
        legacy_url = FO_BHAVCOPY_URL.format(
            base=NSE_ARCHIVES_URL,
            year=year_str,
            month=month_str,
            day=day_str,
        )

        logger.info(f"Fetching F&O bhavcopy for {trade_date}", url=udiff_url)

        try:
            # Try UDiFF first
            response = await self._rate_limited_request(udiff_url)
            is_udiff = True

            if response.status_code != 200:
                logger.info(
                    f"UDiFF F&O bhavcopy not available for {trade_date} (status {response.status_code}), trying legacy URL",
                    url=legacy_url,
                )
                response = await self._rate_limited_request(legacy_url)
                is_udiff = False

            if response.status_code == 404:
                logger.info(f"No F&O bhavcopy for {trade_date} (holiday or weekend)")
                return None

            if response.status_code != 200:
                logger.warning(
                    f"F&O bhavcopy request failed on both UDiFF and legacy URLs",
                    status=response.status_code,
                    date=str(trade_date),
                )
                return None

            # Unzip and parse CSV
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                csv_filename = zf.namelist()[0]
                with zf.open(csv_filename) as csv_file:
                    df = pl.read_csv(csv_file.read())

            # Normalize UDiFF columns and values if applicable
            if is_udiff:
                df = _normalize_udiff_df(df, is_fo=True)

            logger.info(
                f"Parsed F&O bhavcopy",
                date=str(trade_date),
                rows=len(df),
                columns=df.columns[:10],
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
        month_num = trade_date.strftime("%m")
        day_str = trade_date.strftime("%d")
        year_str = str(trade_date.year)

        # Build new UDiFF URL
        udiff_url = f"{NSE_ARCHIVES_URL}/content/cm/BhavCopy_NSE_CM_0_0_0_{year_str}{month_num}{day_str}_F_0000.csv.zip"

        # Build legacy URL as fallback
        month_str = MONTH_MAP[trade_date.month]
        legacy_url = EQ_BHAVCOPY_URL.format(
            base=NSE_ARCHIVES_URL,
            year=year_str,
            month=month_str,
            day=day_str,
        )

        logger.info(f"Fetching equity bhavcopy for {trade_date}", url=udiff_url)

        try:
            # Try UDiFF first
            response = await self._rate_limited_request(udiff_url)
            is_udiff = True

            if response.status_code != 200:
                logger.info(
                    f"UDiFF equity bhavcopy not available for {trade_date} (status {response.status_code}), trying legacy URL",
                    url=legacy_url,
                )
                response = await self._rate_limited_request(legacy_url)
                is_udiff = False

            if response.status_code == 404:
                logger.info(f"No equity bhavcopy for {trade_date} (holiday or weekend)")
                return None

            if response.status_code != 200:
                logger.warning(
                    f"Equity bhavcopy request failed on both UDiFF and legacy URLs",
                    status=response.status_code,
                    date=str(trade_date),
                )
                return None

            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                csv_filename = zf.namelist()[0]
                with zf.open(csv_filename) as csv_file:
                    df = pl.read_csv(csv_file.read())

            # Normalize UDiFF columns if applicable
            if is_udiff:
                df = _normalize_udiff_df(df, is_fo=False)

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
        Fetch current F&O universe (stocks and indices) from NSE.
        Attempts to load from the official market lots file (fo_mktlots.csv).
        Falls back to parsing recent bhavcopies if the market lots file is unavailable.
        """
        url = f"{NSE_ARCHIVES_URL}/content/fo/fo_mktlots.csv"
        logger.info(f"Fetching F&O universe and lot sizes from {url}")
        
        index_symbols = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}
        fallback_index_lots = {
            "NIFTY": 65,
            "BANKNIFTY": 30,
            "FINNIFTY": 60,
            "MIDCPNIFTY": 120,
            "NIFTYNXT50": 25,
        }
        
        try:
            response = await self._rate_limited_request(url)
            if response.status_code == 200:
                import csv
                import io
                
                reader = csv.reader(io.StringIO(response.text))
                # Skip header
                next(reader)
                
                symbols_data = []
                for row in reader:
                    if not row or len(row) < 3:
                        continue
                    underlying = row[0].strip()
                    symbol = row[1].strip()
                    
                    # Skip header/divider rows
                    if symbol.lower() in ("symbol", ""):
                        continue
                    if underlying.lower().startswith("derivatives on"):
                        continue
                        
                    # Find the first non-empty column starting from column 2 (JUN-26, etc.)
                    lot_size = None
                    for col_val in row[2:]:
                        val = col_val.strip()
                        if val:
                            try:
                                lot_size = int(val)
                                break
                            except ValueError:
                                pass
                                
                    inst_type = "index" if symbol in index_symbols else "stock"
                    symbols_data.append({
                        "symbol": symbol,
                        "instrument_type": inst_type,
                        "lot_size": lot_size
                    })
                
                if symbols_data:
                    logger.info(f"Successfully fetched {len(symbols_data)} F&O symbols with lot sizes from fo_mktlots.csv")
                    return symbols_data
                    
            logger.warning(f"Failed to fetch fo_mktlots.csv (status {response.status_code}), falling back to recent bhavcopies")
        except Exception as e:
            logger.warning(f"Error fetching fo_mktlots.csv: {e}, falling back to recent bhavcopies")
            
        # Fallback logic
        try:
            from datetime import timedelta
            check_date = date.today()
            
            for i in range(5):
                d = check_date - timedelta(days=i)
                df = await self.fetch_derivatives_bhavcopy(d)
                if df is not None:
                    # Extract unique symbols
                    # Get indices
                    optidx_symbols = (
                        df.filter(pl.col("INSTRUMENT") == "OPTIDX")
                        .select("SYMBOL")
                        .unique()
                        .to_series()
                        .to_list()
                    )
                    # Get stocks
                    optstk_symbols = (
                        df.filter(pl.col("INSTRUMENT") == "OPTSTK")
                        .select("SYMBOL")
                        .unique()
                        .to_series()
                        .to_list()
                    )
                    
                    symbols_data = []
                    # Add indices
                    for s in optidx_symbols:
                        s_clean = s.strip()
                        if s_clean in index_symbols:
                            symbols_data.append({
                                "symbol": s_clean,
                                "instrument_type": "index",
                                "lot_size": fallback_index_lots.get(s_clean)
                            })
                    # Add stocks
                    for s in optstk_symbols:
                        s_clean = s.strip()
                        symbols_data.append({
                            "symbol": s_clean,
                            "instrument_type": "stock",
                            "lot_size": None
                        })
                    
                    if symbols_data:
                        logger.info(f"Fallback: successfully extracted {len(symbols_data)} F&O symbols from recent bhavcopy")
                        return symbols_data
                        
            logger.warning("Fallback: could not extract F&O symbols from recent bhavcopy")
            return []
        except Exception as e:
            logger.error(f"Fallback failed: error fetching F&O symbols: {e}")
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
