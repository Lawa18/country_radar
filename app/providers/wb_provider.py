from __future__ import annotations
from typing import Dict, Any, List, Optional
import time
import httpx

WB_TIMEOUT = 8.0
WB_RETRIES = 3
WB_BACKOFF = 0.8

WB_CODES = [
    "GC.DOD.TOTL.GD.ZS",
    "GC.DOD.TOTL.CN",
    "NY.GDP.MKTP.CN",
    "GC.DOD.TOTL.CD",
    "NY.GDP.MKTP.CD",
    "SL.UEM.TOTL.ZS",
    "BN.CAB.XOKA.GD.ZS",
    "GE.EST",
    "NY.GDP.MKTP.KD.ZG",
    "FP.CPI.TOTL.ZG",
    "PA.NUS.FCRF",
    "FI.RES.TOTL.CD",
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
    url = f"https://api.worldbank.org/v2/country/{iso3}/indicator/{code}?format=json&per_page=20000"
    data = _http_get_json(url)
    if not data or not isinstance(data, list) or len(data) < 2:
        return None
    return data[1]

def wb_year_dict_from_raw(raw: Optional[List[Dict[str, Any]]]) -> Dict[str, float]:
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
    return dict(sorted(out.items()))

def fetch_worldbank_data(iso2: str, iso3: str) -> Dict[str, Optional[List[Dict[str, Any]]]]:
    out: Dict[str, Optional[List[Dict[str, Any]]]] = {}
    for code in WB_CODES:
        out[code] = fetch_wb_indicator_raw(iso3, code)
    return out

# --- Thin indicator wrappers used by indicator_service ----------------

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
