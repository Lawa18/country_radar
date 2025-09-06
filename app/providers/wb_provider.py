from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple
from functools import lru_cache
import time
import httpx

# ---- Indicators we rely on (keep these) ----
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
    "FR.INR.RINR",        # Policy rate (World Bank, Lending interest rate)
]

WB_BASE = "https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"
WB_DATE_RANGE = "1990:2050"  # broad, but bounded, so the payload stays small
WB_TIMEOUT = 6.0             # short timeouts to avoid Render stalls

# ---- simple in-process cache to avoid repeated network calls on Render ----
_CACHE: Dict[Tuple[str, str], Tuple[float, Any]] = {}  # key=(iso3, indicator) -> (ts, data)
_CACHE_TTL = 300.0  # seconds

def _http_get(url: str, timeout: float) -> Any:
    headers = {"User-Agent": "country-radar/1.0 (+https://country-radar.onrender.com)"}
    with httpx.Client(timeout=timeout, headers=headers) as client:
        r = client.get(url)
        r.raise_for_status()
        return r.json()

def _fetch_wb_indicator(iso3: str, indicator: str) -> Optional[List[dict]]:
    """
    Returns the 'data' list (index 1) from WB response or None on error.
    Uses ISO-3 (e.g., DEU) — this fixes the '1990 only' oddity you saw.
    """
    # cache key
    ck = (iso3.upper(), indicator)
    now = time.time()
    if ck in _CACHE:
        ts, data = _CACHE[ck]
        if now - ts < _CACHE_TTL:
            return data

    # Build URL — request only the date range we care about
    url = (
        f"{WB_BASE.format(country=iso3.upper(), indicator=indicator)}"
        f"?format=json&per_page=20000&date={WB_DATE_RANGE}"
    )
    try:
        j = _http_get(url, WB_TIMEOUT)
        if not isinstance(j, list) or len(j) < 2 or not isinstance(j[1], list):
            _CACHE[ck] = (now, None)
            return None
        data = j[1]
        _CACHE[ck] = (now, data)
        return data
    except Exception:
        _CACHE[ck] = (now, None)
        return None

def fetch_worldbank_data(iso2: str, iso3: str) -> Dict[str, Optional[List[dict]]]:
    """
    Fetch all needed WB indicators for this country (ISO-3 used for requests).
    Returns: { indicator_code: raw_data_list_or_None }
    """
    # Sequential is simpler; cache keeps it fast in practice on Render.
    out: Dict[str, Optional[List[dict]]] = {}
    for code in WB_CODES:
        out[code] = _fetch_wb_indicator(iso3, code)
    return out

def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        try:
            s = str(x).replace(",", "").strip()
            return float(s)
        except Exception:
            return None


def wb_year_dict_from_raw(raw: Optional[List[dict]]) -> Dict[int, float]:
    """
    Convert WB raw list for ONE indicator into {year:int -> value:float}
    Only keeps numeric values.
    """
    if not raw or not isinstance(raw, list):
        return {}
    out: Dict[int, float] = {}
    for row in raw:
        # rows typically have {"date": "2024", "value": 1.23, ...}
        y = row.get("date")
        v = _to_float(row.get("value"))
        if y is None or v is None:
            continue
        try:
            yi = int(str(y)[:4])
        except Exception:
            continue
        out[yi] = v
    return out


def wb_series(raw: Optional[List[dict]]) -> Dict[str, Any]:
    """
    Build a series block:
      {"latest": {"value","date","source"}, "series": {"YYYY": value, ...}}
    """
    d = wb_year_dict_from_raw(raw)
    if not d:
        return {"latest": {"value": None, "date": None, "source": None}, "series": {}}
    last_year = max(d.keys())
    latest = {"value": d[last_year], "date": str(last_year), "source": "World Bank WDI"}
    return {"latest": latest, "series": {str(k): v for k, v in sorted(d.items())}}


def wb_entry(raw: Optional[List[dict]]) -> Dict[str, Any]:
    """
    Latest single-value entry (WB source). Useful for annual indicators without series.
    """
    d = wb_year_dict_from_raw(raw)
    if not d:
        return {"value": None, "date": None, "source": None}
    last_year = max(d.keys())
    return {"value": d[last_year], "date": str(last_year), "source": "World Bank WDI"}
