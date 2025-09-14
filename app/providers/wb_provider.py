# app/providers/wb_provider.py
from __future__ import annotations
from typing import Dict, Any, Optional, List
import time
import httpx

WB_TIMEOUT = 8.0
WB_RETRIES = 3
WB_BACKOFF = 0.8

# Indicators we rely on
WB_CODES = [
    "GC.DOD.TOTL.GD.ZS",  # Debt/GDP ratio (%)
    "GC.DOD.TOTL.CN",     # Gov debt (LCU)
    "NY.GDP.MKTP.CN",     # GDP (LCU)
    "GC.DOD.TOTL.CD",     # Gov debt (USD)
    "NY.GDP.MKTP.CD",     # GDP (USD)
    "SL.UEM.TOTL.ZS",     # Unemployment (%)
    "BN.CAB.XOKA.GD.ZS",  # Current account % GDP
    "GE.EST",             # Government effectiveness
    "NY.GDP.MKTP.KD.ZG",  # GDP growth (%)
    "FP.CPI.TOTL.ZG",     # CPI yoy (%)
    "PA.NUS.FCRF",        # FX to USD (LCU per USD)
    "FI.RES.TOTL.CD",     # Reserves USD
]

def _http_get_json(url: str) -> Optional[Any]:
    for attempt in range(1, WB_RETRIES + 1):
        try:
            with httpx.Client(timeout=WB_TIMEOUT, headers={"Accept": "application/json"}) as client:
                r = client.get(url)
                r.raise_for_status()
                return r.json()
        except Exception as e:
            print(f"[WB] attempt {attempt} failed {url}: {e}")
            if attempt < WB_RETRIES:
                time.sleep(WB_BACKOFF * attempt)
    return None

def fetch_wb_indicator_raw(iso3: str, code: str) -> Optional[List[Dict[str, Any]]]:
    """
    Returns raw 'indicator' array (World Bank v2 API) or None.
    """
    url = f"https://api.worldbank.org/v2/country/{iso3}/indicator/{code}?format=json&per_page=20000"
    data = _http_get_json(url)
    if not data or not isinstance(data, list) or len(data) < 2:
        return None
    return data[1]  # list of observations

def wb_year_dict_from_raw(raw: Optional[List[Dict[str, Any]]]) -> Dict[str, float]:
    """
    Convert raw WB array into { 'YYYY': value } map (newest first filtered later).
    """
    out: Dict[str, float] = {}
    if not raw:
        return out
    for entry in raw:
        y = entry.get("date")
        v = entry.get("value")
        if y is not None and v is not None:
            try:
                out[str(y)] = float(v)
            except Exception:
                pass
    return dict(sorted(out.items()))  # chronological

def fetch_worldbank_data(iso2: str, iso3: str) -> Dict[str, Optional[List[Dict[str, Any]]]]:
    """
    Fetch all needed WB series (raw). Caller can convert with wb_year_dict_from_raw.
    """
    out: Dict[str, Optional[List[Dict[str, Any]]]] = {}
    for code in WB_CODES:
        out[code] = fetch_wb_indicator_raw(iso3, code)
    return out


# --- World Bank indicator wrappers expected by indicator_service ----------------
# These thin wrappers fetch a single WB series and return a {year:int -> value:float}.

# Codes used (confirmed present in this wb_provider):
# FP.CPI.TOTL.ZG         -> Inflation, consumer prices (annual %)
# SL.UEM.TOTL.ZS         -> Unemployment, total (% of total labor force)
# PA.NUS.FCRF            -> Official exchange rate (LCU per USD, period average)
# FI.RES.TOTL.CD         -> Total reserves (current US$)
# NY.GDP.MKTP.KD.ZG      -> GDP growth (annual %)
# BN.CAB.XOKA.GD.ZS      -> Current account balance (% of GDP)
# GE.EST                 -> Government Effectiveness (WGI index)

from typing import Optional, Dict

def _wb_year_dict(iso3: str, code: str) -> Optional[Dict[int, float]]:
    """
    Fetch a single World Bank indicator for iso3 and convert to {year:int -> value:float}.
    Returns None if no data.
    """
    raw = fetch_wb_indicator_raw(iso3, code)
    return wb_year_dict_from_raw(raw)

def wb_cpi_yoy_annual(iso3: str) -> Optional[Dict[int, float]]:
    return _wb_year_dict(iso3, "FP.CPI.TOTL.ZG")

def wb_unemployment_rate_annual(iso3: str) -> Optional[Dict[int, float]]:
    return _wb_year_dict(iso3, "SL.UEM.TOTL.ZS")

def wb_fx_rate_usd_annual(iso3: str) -> Optional[Dict[int, float]]:
    return _wb_year_dict(iso3, "PA.NUS.FCRF")

def wb_reserves_usd_annual(iso3: str) -> Optional[Dict[int, float]]:
    return _wb_year_dict(iso3, "FI.RES.TOTL.CD")

def wb_gdp_growth_annual_pct(iso3: str) -> Optional[Dict[int, float]]:
    return _wb_year_dict(iso3, "NY.GDP.MKTP.KD.ZG")

def wb_current_account_balance_pct_gdp_annual(iso3: str) -> Optional[Dict[int, float]]:
    return _wb_year_dict(iso3, "BN.CAB.XOKA.GD.ZS")

def wb_government_effectiveness_annual(iso3: str) -> Optional[Dict[int, float]]:
    return _wb_year_dict(iso3, "GE.EST")
# -------------------------------------------------------------------------------
