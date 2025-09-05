from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple
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
]

# ---- tiny TTL cache to keep Render responsive ----
_CACHE: Dict[str, Tuple[float, Any]] = {}
_TTL_SECONDS = 6 * 60 * 60  # 6 hours

def _cache_get(key: str):
    row = _CACHE.get(key)
    if not row:
        return None
    ts, val = row
    if time.time() - ts > _TTL_SECONDS:
        _CACHE.pop(key, None)
        return None
    return val

def _cache_set(key: str, val: Any):
    _CACHE[key] = (time.time(), val)

def _wb_request(url: str) -> Any:
    cached = _cache_get(url)
    if cached is not None:
        return cached
    # Aggressive per_page to get full history in one call
    with httpx.Client(timeout=httpx.Timeout(5.0, read=12.0)) as client:
        r = client.get(url)
        r.raise_for_status()
        data = r.json()
    _cache_set(url, data)
    return data

def _wb_fetch_series(iso: str, indicator: str) -> List[dict]:
    # World Bank v2 JSON: [meta, data[]]
    url = f"https://api.worldbank.org/v2/country/{iso}/indicator/{indicator}?format=json&per_page=20000"
    try:
        data = _wb_request(url)
        if isinstance(data, list) and len(data) == 2 and isinstance(data[1], list):
            return data[1]
    except Exception:
        pass
    return []

def fetch_worldbank_data(iso2: str, iso3: str) -> Dict[str, List[dict]]:
    """
    Returns a dict {indicator_code: raw_list}, ready for helper parsing below.
    We fetch by ISO2 where possible (WB supports both), falling back to ISO3 if needed.
    """
    out: Dict[str, List[dict]] = {}
    for code in WB_CODES:
        rows = _wb_fetch_series(iso2.lower(), code)
        if not rows:
            rows = _wb_fetch_series(iso3.lower(), code)
        if rows:
            out[code] = rows
    return out

def wb_year_dict_from_raw(raw: Optional[List[dict]]) -> Dict[int, float]:
    """
    Convert raw WB rows into {year:int -> value:float}, across *all* available years.
    WB returns most-recent first; we normalize to a full dict.
    """
    if not raw:
        return {}
    out: Dict[int, float] = {}
    for row in raw:
        try:
            y = row.get("date")
            v = row.get("value")
            if y is None or v is None:
                continue
            y_int = int(str(y)[:4])
            out[y_int] = float(v)
        except Exception:
            continue
    return out

def wb_series(raw: Optional[List[dict]]) -> Dict[str, Any]:
    """
    Return {"latest": {...}, "series": {"YYYY": value}} with WB as source.
    """
    d = wb_year_dict_from_raw(raw)
    if not d:
        return {"latest": {"value": None, "date": None, "source": None}, "series": {}}
    last_year = max(d.keys())
    latest = {"value": d[last_year], "date": str(last_year), "source": "World Bank WDI"}
    return {"latest": latest, "series": {str(k): v for k, v in sorted(d.items())}}

def wb_entry(raw: Optional[List[dict]]) -> Dict[str, Any]:
    """
    Just the latest entry (WB source). Useful for single-value annual indicators.
    """
    d = wb_year_dict_from_raw(raw)
    if not d:
        return {"value": None, "date": None, "source": None}
    last_year = max(d.keys())
    return {"value": d[last_year], "date": str(last_year), "source": "World Bank WDI"}
