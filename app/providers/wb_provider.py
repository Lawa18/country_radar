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
def _wb_years(iso3: str, code: str) -> Dict[str, float]:
    try:
        raw = fetch_wb_indicator_raw(iso3, code)
        return wb_year_dict_from_raw(raw)
    except Exception:
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
