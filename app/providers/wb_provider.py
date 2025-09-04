import os
from typing import Dict, Any, List, Tuple, Optional
import httpx

WB_BASE = "https://api.worldbank.org/v2"
WB_TIMEOUT = float(os.getenv("UPSTREAM_TIMEOUT", "6.0"))
WB_START = os.getenv("WB_DATE_START", "1990")  # trims response size

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

def _wb_get(url: str, params: Dict[str, Any]) -> Any:
    try:
        with httpx.Client(timeout=WB_TIMEOUT) as client:
            r = client.get(url, params=params)
            r.raise_for_status()
            return r.json()
    except Exception as e:
        print(f"[WB] GET failed {url}: {e}")
        return {}

def _wb_indicator_url(iso2: str, code: str) -> str:
    return f"{WB_BASE}/country/{iso2}/indicator/{code}"

def fetch_worldbank_data(iso2: str, iso3: str) -> Dict[str, Any]:
    """
    Fetches a *trimmed* set of indicators since WB_START to keep payloads small.
    Returns raw per-indicator JSON so existing parsers still work.
    """
    out: Dict[str, Any] = {}
    for code in WB_CODES:
        url = _wb_indicator_url(iso2, code)
        data = _wb_get(url, {
            "format": "json",
            "per_page": "200",
            "date": f"{WB_START}:9999"  # reduce response size vs. 1960:...
        })
        out[code] = data
    return out

def wb_year_dict_from_raw(raw: Any) -> Dict[int, float]:
    """
    Convert WB raw JSON into {year: value}. Ignores None values.
    Handles both [] and {} inputs safely.
    """
    result: Dict[int, float] = {}
    try:
        if not isinstance(raw, list) or len(raw) < 2:
            return {}
        # WB returns [metadata, [{...}, {...}, ...]]
        data = raw[1] or []
        for row in data:
            y = row.get("date")
            v = row.get("value")
            if y is None or v is None:
                continue
            ys = str(y)
            if ys.isdigit():
                result[int(ys)] = float(v)
    except Exception as e:
        print(f"[WB] parse failed: {e}")
    return result

def wb_series(raw: Any) -> Dict[str, Any]:
    """
    Convert WB raw JSON into {"latest": {...}, "series": {...}} shape.
    Latest picks the max year with non-null value.
    """
    series = wb_year_dict_from_raw(raw)
    if not series:
        return {"latest": {"value": None, "date": None, "source": "World Bank WDI"}, "series": {}}
    yrs = sorted(series.keys())
    y = yrs[-1]
    return {
        "latest": {"value": series[y], "date": str(y), "source": "World Bank WDI"},
        "series": {str(k): v for k, v in sorted(series.items())}
    }

def wb_entry(raw: Any) -> Optional[Dict[str, Any]]:
    """
    Return only the latest point: {"value", "date", "source"} or None.
    """
    series = wb_year_dict_from_raw(raw)
    if not series:
        return None
    y = max(series.keys())
    return {"value": series[y], "date": str(y), "source": "World Bank WDI"}
