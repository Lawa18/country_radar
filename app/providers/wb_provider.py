# app/providers/wb_provider.py
from __future__ import annotations
from typing import Dict, Any, List, Optional
import time
import os
import httpx

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
WB_TIMEOUT = float(os.getenv("WB_TIMEOUT", "8.0"))
WB_RETRIES = int(os.getenv("WB_RETRIES", "3"))
WB_BACKOFF = float(os.getenv("WB_BACKOFF", "0.8"))
WB_DEBUG = os.getenv("WB_DEBUG", "0") == "1"

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

# -------------------------------------------------------------------
# HTTP WRAPPER
# -------------------------------------------------------------------
def _http_get_json(url: str) -> Optional[Any]:
    for attempt in range(1, WB_RETRIES + 1):
        try:
            if WB_DEBUG:
                print(f"[WB] GET {url} (attempt {attempt})")

            with httpx.Client(
                timeout=WB_TIMEOUT,
                headers={"Accept": "application/json"},
                follow_redirects=True,
            ) as client:
                r = client.get(url)
                r.raise_for_status()
                return r.json()

        except Exception as e:
            if WB_DEBUG:
                print(f"[WB] attempt {attempt} failed {url}: {e}")

            if attempt < WB_RETRIES:
                time.sleep(WB_BACKOFF * attempt)

    return None


# -------------------------------------------------------------------
# RAW SERIES FETCH
# -------------------------------------------------------------------
def fetch_wb_indicator_raw(iso3: str, code: str) -> Optional[List[Dict[str, Any]]]:
    """
    Returns raw World Bank data array:
       [ {date: "2023", value: 4.3, ...}, ... ]
    """
    url = (
        f"https://api.worldbank.org/v2/country/{iso3}/indicator/{code}"
        f"?format=json&per_page=20000"
    )

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

    return dict(sorted(out.items()))


# -------------------------------------------------------------------
# BATCH FETCHER (used only in probe & diagnostics)
# -------------------------------------------------------------------
def fetch_worldbank_data(iso2: str, iso3: str) -> Dict[str, Optional[List[Dict[str, Any]]]]:
    """
    Returns raw WB arrays for all key indicators.
    """
    out: Dict[str, Optional[List[Dict[str, Any]]]] = {}
    for code in WB_CODES:
        out[code] = fetch_wb_indicator_raw(iso3, code)
    return out


# -------------------------------------------------------------------
# INDICATOR HELPERS USED BY COUNTRY-LITE BUILDER
# -------------------------------------------------------------------
from typing import Dict, Optional

MAX_YEARS_DEFAULT = 20  # or whatever you're using elsewhere

def _wb_indicator_annual(
    iso3: str,
    indicator: str,
    years: int = MAX_YEARS_DEFAULT,
) -> Dict[str, float]:
    """
    Existing generic helper that:
      - calls World Bank
      - returns { "YYYY": value, ... } (only last `years` entries)
    You likely already have something like this – reuse your version.
    """
    ...
    # placeholder: use your existing implementation
    return {}


def _wb_years(iso3: str, code: str) -> Dict[str, float]:
    try:
        raw = fetch_wb_indicator_raw(iso3, code)
        return wb_year_dict_from_raw(raw)
    except Exception:
        return {}

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
    # ---- Tier 1: direct ratio
    direct = _wb_indicator_annual(iso3, "GC.DOD.TOTL.GD.ZS", years)
    if direct:
        return direct

    # ---- Tier 2: compute from levels in LCU or USD
    debt_lcu = _wb_indicator_annual(iso3, "GC.DOD.TOTL.CN", years)
    gdp_lcu = _wb_indicator_annual(iso3, "NY.GDP.MKTP.CN", years)

    debt_usd = _wb_indicator_annual(iso3, "GC.DOD.TOTL.CD", years)
    gdp_usd = _wb_indicator_annual(iso3, "NY.GDP.MKTP.CD", years)

    def _compute_ratio(
        debt: Dict[str, float],
        gdp: Dict[str, float],
    ) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for year, debt_val in debt.items():
            gdp_val = gdp.get(year)
            if gdp_val is None or gdp_val == 0:
                continue
            out[year] = float(debt_val) / float(gdp_val) * 100.0
        return out

    # Prefer consistent currency pairs
    ratio_lcu = _compute_ratio(debt_lcu, gdp_lcu) if debt_lcu and gdp_lcu else {}
    ratio_usd = _compute_ratio(debt_usd, gdp_usd) if debt_usd and gdp_usd else {}

    # Choose whichever has more data
    if ratio_lcu and (len(ratio_lcu) >= len(ratio_usd)):
        ratios = ratio_lcu
    elif ratio_usd:
        ratios = ratio_usd
    else:
        return {}

    # Optionally trim to last `years` entries if needed
    if len(ratios) > years:
        # sort by year, keep most recent `years`
        items = sorted(ratios.items(), key=lambda kv: kv[0])[-years:]
        ratios = {k: v for k, v in items}

    return ratios

FISCAL_BALANCE_RATIO_CODES = [
    "GC.NLD.TOTL.GD.ZS",    # Net lending (+) / borrowing (-), % of GDP
    "GC.BAL.CASH.GD.ZS",    # Cash surplus / deficit, % of GDP
]

def wb_fiscal_balance_pct_gdp_annual(
    iso3: str,
    years: int = MAX_YEARS_DEFAULT,
) -> Dict[str, float]:
    """
    Fiscal balance (% of GDP), annual, last `years`.

    Tries several WB codes and returns the first one that has data.
    """
    for code in FISCAL_BALANCE_RATIO_CODES:
        series = _wb_indicator_annual(iso3, code, years)
        if series:
            return series

    # (Optional) later: compute from level indicators if you want.
    return {}

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


def wb_government_effectiveness_annual(iso3: str) -> Dict[str, float]:
    return _wb_years(iso3, "GE.EST")


__all__ = [
    "fetch_worldbank_data",
    "fetch_wb_indicator_raw",
    "wb_year_dict_from_raw",
    "wb_cpi_yoy_annual",
    "wb_unemployment_rate_annual",
    "wb_fx_rate_usd_annual",
    "wb_reserves_usd_annual",
    "wb_gdp_growth_annual_pct",
    "wb_current_account_balance_pct_gdp_annual",
    "wb_government_effectiveness_annual",
]
