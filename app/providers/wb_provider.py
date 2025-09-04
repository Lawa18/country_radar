# app/providers/wb_provider.py
from __future__ import annotations
import os
from typing import Any, Dict, List, Optional
import httpx

# --- Indicators we rely on (unchanged) ---
WB_CODES: List[str] = [
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

WB_BASE = "https://api.worldbank.org/v2"
WB_TIMEOUT = float(os.getenv("UPSTREAM_TIMEOUT", "6.0"))
WB_DATE_START = os.getenv("WB_DATE_START", "1990")
WB_SERIES_MRV = os.getenv("WB_SERIES_MRV", "35")  # ~last 35 obs if no date

def _client() -> httpx.Client:
    return httpx.Client(timeout=WB_TIMEOUT)

def _indicator_url(iso2: str, code: str) -> str:
    return f"{WB_BASE}/country/{iso2}/indicator/{code}"

def _wb_fetch_series(iso2: str, code: str) -> Any:
    """
    Fetch a trimmed series for a single indicator.
    Preference: date=WB_DATE_START:9999 if provided; else MRV=WB_SERIES_MRV.
    """
    params = {"format": "json", "per_page": "200"}
    if WB_DATE_START:
        params["date"] = f"{WB_DATE_START}:9999"
    else:
        params["MRV"] = WB_SERIES_MRV

    url = _indicator_url(iso2, code)
    try:
        with _client() as c:
            r = c.get(url, params=params)
            r.raise_for_status()
            return r.json()
    except Exception as e:
        print(f"[WB] fetch fail {code} {iso2}: {e}")
        return {}

def fetch_worldbank_data(iso2: str, iso3: str) -> Dict[str, Any]:
    """
    Batch fetch for all WB_CODES. Returns {code: raw_json}.
    Keeping iso3 in signature to match previous call sites (not used here).
    """
    out: Dict[str, Any] = {}
    for code in WB_CODES:
        out[code] = _wb_fetch_series(iso2, code)
    return out

def wb_year_dict_from_raw(raw: Any) -> Dict[int, float]:
    """
    Convert World Bank raw JSON to {year:int -> value:float} (filters None).
    """
    d: Dict[int, float] = {}
    try:
        if isinstance(raw, list) and len(raw) >= 2 and raw[1]:
            for row in raw[1]:
                y, v = row.get("date"), row.get("value")
                if y and str(y).isdigit() and v is not None:
                    d[int(y)] = float(v)
    except Exception as e:
        print(f"[WB] parse error: {e}")
    # ascending by year
    return {k: d[k] for k in sorted(d)}

def wb_entry(raw: Any) -> Dict[str, Any]:
    """
    Latest single-entry shape: {"value":..., "date":..., "source":"World Bank WDI"}
    """
    series = wb_year_dict_from_raw(raw)
    if series:
        y = max(series.keys())
        return {"value": series[y], "date": str(y), "source": "World Bank WDI"}
    return {"value": None, "date": None, "source": None}

def wb_series(raw: Any) -> Dict[str, Any]:
    """
    Series block: {"latest": {...}, "series": {"YYYY": value, ...}}
    """
    series = wb_year_dict_from_raw(raw)
    latest = {"value": None, "date": None, "source": None}
    if series:
        y = max(series.keys())
        latest = {"value": series[y], "date": str(y), "source": "World Bank WDI"}
    # keys must be strings for JSON
    str_series = {str(k): v for k, v in series.items()}
    return {"latest": latest, "series": str_series}
