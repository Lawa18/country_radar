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

# Avoid absurd page sizes. WB API is fast enough with 200–500.
WB_PER_PAGE = int(os.getenv("WB_PER_PAGE", "200"))

MAX_YEARS_DEFAULT = int(os.getenv("WB_MAX_YEARS_DEFAULT", "20"))

WB_BASE = "https://api.worldbank.org/v2"

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
# HTTP WRAPPER
# -------------------------------------------------------------------
def _timeout() -> httpx.Timeout:
    # Separate phase timeouts helps avoid "stuck" calls.
    return httpx.Timeout(
        timeout=WB_TIMEOUT,
        connect=min(2.0, WB_TIMEOUT),
        read=WB_TIMEOUT,
        write=min(2.0, WB_TIMEOUT),
        pool=min(2.0, WB_TIMEOUT),
    )


def _http_get_json(client: httpx.Client, url: str) -> Optional[Any]:
    for attempt in range(1, WB_RETRIES + 1):
        try:
            if WB_DEBUG:
                print(f"[WB] GET {url} (attempt {attempt})")
            r = client.get(url)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if WB_DEBUG:
                print(f"[WB] attempt {attempt} failed {url}: {e!r}")
            if attempt < WB_RETRIES:
                time.sleep(WB_BACKOFF * attempt)
    return None


def _build_url(iso3: str, code: str, per_page: int = WB_PER_PAGE) -> str:
    # format=json ensures list response; per_page controls payload size.
    return f"{WB_BASE}/country/{iso3}/indicator/{code}?format=json&per_page={per_page}"


# -------------------------------------------------------------------
# RAW SERIES FETCH
# -------------------------------------------------------------------
def fetch_wb_indicator_raw(iso3: str, code: str) -> Optional[List[Dict[str, Any]]]:
    """
    Returns raw World Bank data array:
       [ {date: "2023", value: 4.3, ...}, ... ]
    """
    url = _build_url(iso3, code, per_page=WB_PER_PAGE)

    with httpx.Client(
        timeout=_timeout(),
        headers={"Accept": "application/json"},
        follow_redirects=True,
    ) as client:
        data = _http_get_json(client, url)

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

    # sort ascending by year string
    return dict(sorted(out.items(), key=lambda kv: kv[0]))


def _trim_last_n_years(series: Dict[str, float], years: int) -> Dict[str, float]:
    if not series:
        return {}
    items = sorted(series.items(), key=lambda kv: int(str(kv[0])))
    if len(items) <= years:
        return dict(items)
    return dict(items[-years:])


# -------------------------------------------------------------------
# PRIMARY "ANNUAL SERIES" HELPER (FIXES YOUR PLACEHOLDER ...)
# -------------------------------------------------------------------
def _wb_indicator_annual(
    iso3: str,
    indicator: str,
    years: int = MAX_YEARS_DEFAULT,
) -> Dict[str, float]:
    """
    Fetch indicator series and return { "YYYY": value } limited to last `years`.
    """
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
# BATCH FETCHER (used only in probe & diagnostics)
# -------------------------------------------------------------------
def fetch_worldbank_data(iso2: str, iso3: str) -> Dict[str, Optional[List[Dict[str, Any]]]]:
    """
    Returns raw WB arrays for all key indicators.
    NOTE: Keep for diagnostics; not optimized for runtime.
    """
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
    """
    Government debt as % of GDP, annual, last `years`.

    Priority:
      1) GC.DOD.TOTL.GD.ZS  (ratio directly from WB)
      2) If missing: compute ratio from levels:
           - GC.DOD.TOTL.CN vs NY.GDP.MKTP.CN  (LCU)
           - GC.DOD.TOTL.CD vs NY.GDP.MKTP.CD  (USD)
    """
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
            if g is None or g == 0:
                continue
            out[y] = (float(d) / float(g)) * 100.0
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
    """
    Fiscal balance (% of GDP), annual, last `years`.

    Tries multiple WB codes and returns the first one with data.
    """
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
    # raw/base
    "fetch_worldbank_data",
    "fetch_wb_indicator_raw",
    "wb_year_dict_from_raw",
    # annual helper used by services
    "wb_gov_debt_pct_gdp_annual",
    "wb_fiscal_balance_pct_gdp_annual",
    # common indicators
    "wb_cpi_yoy_annual",
    "wb_unemployment_rate_annual",
    "wb_fx_rate_usd_annual",
    "wb_reserves_usd_annual",
    "wb_gdp_growth_annual_pct",
    "wb_current_account_balance_pct_gdp_annual",
    "wb_current_account_level_usd_annual",
    "wb_government_effectiveness_annual",
]
