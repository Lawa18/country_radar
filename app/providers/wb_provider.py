# app/providers/wb_provider.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import os
import time
import httpx

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
WB_TIMEOUT = float(os.getenv("WB_TIMEOUT", "6.0"))
WB_RETRIES = int(os.getenv("WB_RETRIES", "2"))
WB_BACKOFF = float(os.getenv("WB_BACKOFF", "0.6"))
WB_DEBUG = os.getenv("WB_DEBUG", "0") == "1"

# Keep payloads reasonable (WB returns newest->oldest anyway)
WB_PER_PAGE = int(os.getenv("WB_PER_PAGE", "200"))
MAX_YEARS_DEFAULT = int(os.getenv("WB_MAX_YEARS_DEFAULT", "20"))

WB_BASE = "https://api.worldbank.org/v2"

# Small in-process cache to avoid repeated WB calls across requests
WB_CACHE_TTL = float(os.getenv("WB_CACHE_TTL", "900"))  # 15 minutes
_WB_CACHE: Dict[str, Tuple[float, Any]] = {}

# Stable World Bank indicator codes used by Country Radar
WB_CODES = [
    "GC.DOD.TOTL.GD.ZS",   # Debt (% GDP)
    "GC.DOD.TOTL.CN",      # Debt (LCU)
    "NY.GDP.MKTP.CN",      # GDP (LCU)
    "GC.DOD.TOTL.CD",      # Debt (USD)
    "NY.GDP.MKTP.CD",      # GDP (USD)
    "SL.UEM.TOTL.ZS",      # Unemployment rate
    "BN.CAB.XOKA.GD.ZS",   # Current account (% GDP)
    "GE.EST",              # Government effectiveness (WGI)
    "NY.GDP.MKTP.KD.ZG",   # GDP growth (%)
    "FP.CPI.TOTL.ZG",      # CPI inflation (%)
    "PA.NUS.FCRF",         # FX rate (LCU per USD)
    "FI.RES.TOTL.CD",      # FX reserves (USD)
]

FISCAL_BALANCE_RATIO_CODES = [
    "GC.NLD.TOTL.GD.ZS",    # Net lending (+) / borrowing (-), % of GDP
    "GC.BAL.CASH.GD.ZS",    # Cash surplus / deficit, % of GDP
]

# -------------------------------------------------------------------
# HTTP CLIENT (shared)
# -------------------------------------------------------------------
def _timeout() -> httpx.Timeout:
    return httpx.Timeout(
        timeout=WB_TIMEOUT,
        connect=min(2.0, WB_TIMEOUT),
        read=WB_TIMEOUT,
        write=min(2.0, WB_TIMEOUT),
        pool=min(2.0, WB_TIMEOUT),
    )


_CLIENT: Optional[httpx.Client] = None


def _get_client() -> httpx.Client:
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT

    limits = httpx.Limits(
        max_connections=int(os.getenv("WB_MAX_CONNECTIONS", "20")),
        max_keepalive_connections=int(os.getenv("WB_MAX_KEEPALIVE", "10")),
        keepalive_expiry=float(os.getenv("WB_KEEPALIVE_EXPIRY", "30")),
    )

    _CLIENT = httpx.Client(
        timeout=_timeout(),
        headers={"Accept": "application/json"},
        follow_redirects=True,
        limits=limits,
    )
    return _CLIENT


def _cache_get(key: str) -> Optional[Any]:
    row = _WB_CACHE.get(key)
    if not row:
        return None
    ts, payload = row
    if (time.time() - ts) > WB_CACHE_TTL:
        return None
    return payload


def _cache_set(key: str, payload: Any) -> None:
    _WB_CACHE[key] = (time.time(), payload)


def _http_get_json(url: str) -> Optional[Any]:
    # cache first
    cached = _cache_get(url)
    if cached is not None:
        return cached

    client = _get_client()

    for attempt in range(1, WB_RETRIES + 1):
        try:
            if WB_DEBUG:
                print(f"[WB] GET {url} (attempt {attempt})")
            r = client.get(url)
            r.raise_for_status()
            data = r.json()
            _cache_set(url, data)
            return data
        except Exception as e:
            if WB_DEBUG:
                print(f"[WB] attempt {attempt} failed {url}: {e!r}")
            if attempt < WB_RETRIES:
                time.sleep(WB_BACKOFF * attempt)
    return None


def _build_url(iso3: str, code: str, per_page: int = WB_PER_PAGE) -> str:
    # Reduce payload: only pull last N years by using date=YYYY:YYYY
    try:
        y2 = time.gmtime().tm_year
        y1 = max(1960, y2 - (MAX_YEARS_DEFAULT + 5))  # a little buffer
        date_param = f"&date={y1}:{y2}"
    except Exception:
        date_param = ""

    return f"{WB_BASE}/country/{iso3}/indicator/{code}?format=json&per_page={per_page}{date_param}"


# -------------------------------------------------------------------
# RAW SERIES FETCH
# -------------------------------------------------------------------
def fetch_wb_indicator_raw(iso3: str, code: str) -> Optional[List[Dict[str, Any]]]:
    """
    Returns raw World Bank data array:
       [ {date: "2023", value: 4.3, ...}, ... ]
    """
    url = _build_url(iso3, code, per_page=WB_PER_PAGE)
    data = _http_get_json(url)

    if WB_DEBUG:
        print(f"[WB] raw for {iso3}/{code}: type={type(data)}")

    # WB returns: [ {metadata}, [data...] ]
    if not isinstance(data, list) or len(data) < 2:
        return None

    arr = data[1]
    if not isinstance(arr, list):
        return None

    return arr


# -------------------------------------------------------------------
# NORMALIZATION
# -------------------------------------------------------------------
def wb_year_dict_from_raw(raw: Optional[List[Dict[str, Any]]]) -> Dict[str, float]:
    """
    Converts raw WB list → { "YYYY": float }
    WB data is usually returned newest -> oldest.
    """
    out: Dict[str, float] = {}
    if not raw:
        return out

    for entry in raw:
        y = entry.get("date")
        v = entry.get("value")
        if y is None or v is None:
            continue
        try:
            out[str(y)] = float(v)
        except Exception:
            continue

    return dict(sorted(out.items(), key=lambda kv: kv[0]))


def _trim_last_n_years(series: Dict[str, float], years: int) -> Dict[str, float]:
    if not series:
        return {}
    items = sorted(series.items(), key=lambda kv: int(str(kv[0])))
    if len(items) <= years:
        return dict(items)
    return dict(items[-years:])


# -------------------------------------------------------------------
# PRIMARY ANNUAL HELPER
# -------------------------------------------------------------------
def _wb_indicator_annual(
    iso3: str,
    indicator: str,
    years: int = MAX_YEARS_DEFAULT,
) -> Dict[str, float]:
    raw = fetch_wb_indicator_raw(iso3, indicator)
    series = wb_year_dict_from_raw(raw)
    return _trim_last_n_years(series, years)


def _wb_years(iso3: str, code: str) -> Dict[str, float]:
    try:
        raw = fetch_wb_indicator_raw(iso3, code)
        return wb_year_dict_from_raw(raw)
    except Exception:
        return {}


# -------------------------------------------------------------------
# BATCH FETCHER (diagnostics only)
# -------------------------------------------------------------------
def fetch_worldbank_data(iso2: str, iso3: str) -> Dict[str, Optional[List[Dict[str, Any]]]]:
    out: Dict[str, Optional[List[Dict[str, Any]]]] = {}
    for code in WB_CODES:
        out[code] = fetch_wb_indicator_raw(iso3, code)
    return out


# -------------------------------------------------------------------
# DEBT (% GDP) WITH LEVEL-FALLBACK
# -------------------------------------------------------------------
def wb_gov_debt_pct_gdp_annual(
    iso3: str,
    years: int = MAX_YEARS_DEFAULT,
) -> Dict[str, float]:
    # Tier 1: direct ratio
    direct = _wb_indicator_annual(iso3, "GC.DOD.TOTL.GD.ZS", years)
    if direct:
        return direct

    # Tier 2: compute from levels
    debt_lcu = _wb_indicator_annual(iso3, "GC.DOD.TOTL.CN", years)
    gdp_lcu = _wb_indicator_annual(iso3, "NY.GDP.MKTP.CN", years)

    debt_usd = _wb_indicator_annual(iso3, "GC.DOD.TOTL.CD", years)
    gdp_usd = _wb_indicator_annual(iso3, "NY.GDP.MKTP.CD", years)

    def _compute_ratio(debt: Dict[str, float], gdp: Dict[str, float]) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for y, d in debt.items():
            g = gdp.get(y)
            if g is None:
                continue
            try:
                gv = float(g)
                if gv == 0.0:
                    continue
                out[y] = (float(d) / gv) * 100.0
            except Exception:
                continue
        return out

    ratio_lcu = _compute_ratio(debt_lcu, gdp_lcu) if debt_lcu and gdp_lcu else {}
    ratio_usd = _compute_ratio(debt_usd, gdp_usd) if debt_usd and gdp_usd else {}

    if ratio_lcu and len(ratio_lcu) >= len(ratio_usd):
        return _trim_last_n_years(ratio_lcu, years)
    if ratio_usd:
        return _trim_last_n_years(ratio_usd, years)
    return {}


# -------------------------------------------------------------------
# FISCAL BALANCE (% GDP) WITH CODE-FALLBACK
# -------------------------------------------------------------------
def wb_fiscal_balance_pct_gdp_annual(
    iso3: str,
    years: int = MAX_YEARS_DEFAULT,
) -> Dict[str, float]:
    for code in FISCAL_BALANCE_RATIO_CODES:
        s = _wb_indicator_annual(iso3, code, years)
        if s:
            return s
    return {}


# -------------------------------------------------------------------
# SIMPLE HELPERS (ANNUAL)
# -------------------------------------------------------------------
def wb_cpi_yoy_annual(iso3: str) -> Dict[str, float]:
    return _wb_years(iso3, "FP.CPI.TOTL.ZG")


def wb_unemployment_rate_annual(iso3: str) -> Dict[str, float]:
    return _wb_years(iso3, "SL.UEM.TOTL.ZS")


def wb_fx_rate_usd_annual(iso3: str) -> Dict[str, float]:
    return _wb_years(iso3, "PA.NUS.FCRF")


def wb_reserves_usd_annual(iso3: str) -> Dict[str, float]:
    return _wb_years(iso3, "FI.RES.TOTL.CD")


def wb_gdp_growth_annual_pct(iso3: str) -> Dict[str, float]:
    return _wb_years(iso3, "NY.GDP.MKTP.KD.ZG")


def wb_current_account_balance_pct_gdp_annual(iso3: str) -> Dict[str, float]:
    return _wb_years(iso3, "BN.CAB.XOKA.GD.ZS")


def wb_current_account_level_usd_annual(iso3: str) -> Dict[str, float]:
    return _wb_years(iso3, "BN.CAB.XOKA.CD")


def wb_government_effectiveness_annual(iso3: str) -> Dict[str, float]:
    return _wb_years(iso3, "GE.EST")


__all__ = [
    "fetch_worldbank_data",
    "fetch_wb_indicator_raw",
    "wb_year_dict_from_raw",
    "wb_gov_debt_pct_gdp_annual",
    "wb_fiscal_balance_pct_gdp_annual",
    "wb_cpi_yoy_annual",
    "wb_unemployment_rate_annual",
    "wb_fx_rate_usd_annual",
    "wb_reserves_usd_annual",
    "wb_gdp_growth_annual_pct",
    "wb_current_account_balance_pct_gdp_annual",
    "wb_current_account_level_usd_annual",
    "wb_government_effectiveness_annual",
]
