from typing import Dict, Any, Optional, Tuple
import httpx

# Simple World Bank indicator fetcher (ISO3 code, e.g., CAN, DEU)
def _wb_fetch_indicator(iso3: str, code: str) -> Dict[int, float]:
    url = f"https://api.worldbank.org/v2/country/{iso3}/indicator/{code}"
    params = {"format": "json", "per_page": "20000"}
    r = httpx.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list) or len(data) < 2 or data[1] is None:
        return {}
    out: Dict[int, float] = {}
    for row in data[1]:
        y = row.get("date")
        v = row.get("value")
        try:
            yy = int(str(y))
        except Exception:
            continue
        if v is not None:
            try:
                out[yy] = float(v)
            except Exception:
                pass
    return out

def fetch_worldbank_data(iso2: str, iso3: str) -> Dict[str, Any]:
    """
    Fetch a basket of indicators we need for the Country Radar.
    Keys mirror WDI codes.
    """
    codes = [
        "GC.DOD.TOTL.GD.ZS",  # debt/GDP (%)
        "GC.DOD.TOTL.CN",     # debt (LCU)
        "NY.GDP.MKTP.CN",     # GDP (LCU)
        "GC.DOD.TOTL.CD",     # debt (USD)
        "NY.GDP.MKTP.CD",     # GDP (USD)
        "FP.CPI.TOTL.ZG",     # CPI YoY (%)
        "PA.NUS.FCRF",        # FX rate to USD (LCU per USD)
        "FI.RES.TOTL.CD",     # Reserves (USD)
        "NY.GDP.MKTP.KD.ZG",  # GDP growth (%)
        "SL.UEM.TOTL.ZS",     # Unemployment (%)
        "BN.CAB.XOKA.GD.ZS",  # Current Account Balance (% of GDP)
        "GE.EST",             # Government Effectiveness (WGI)
    ]
    out: Dict[str, Any] = {}
    for c in codes:
        try:
            out[c] = _wb_fetch_indicator(iso3, c)
        except Exception:
            out[c] = {}
    return out

def wb_year_dict_from_raw(raw: Optional[Any]) -> Dict[int, float]:
    if isinstance(raw, dict):
        return {int(y): float(v) for y, v in raw.items() if v is not None}
    return {}

def _latest(d: Dict[int, float]) -> Optional[Tuple[int, float]]:
    years = [y for y, v in d.items() if v is not None]
    if not years:
        return None
    y = max(years)
    return y, d[y]

def wb_entry(raw: Optional[Any]) -> Optional[Dict[str, Any]]:
    d = wb_year_dict_from_raw(raw)
    if not d:
        return None
    lv = _latest(d)
    if not lv:
        return None
    y, v = lv
    return {"value": float(v), "date": str(y), "source": "World Bank WDI"}

def wb_series(raw: Optional[Any]) -> Optional[Dict[str, Any]]:
    d = wb_year_dict_from_raw(raw)
    if not d:
        return None
    lv = _latest(d)
    latest = {"value": None, "date": None, "source": "World Bank WDI"}
    if lv:
        y, v = lv
        latest = {"value": float(v), "date": str(y), "source": "World Bank WDI"}
    series = {str(yy): vv for yy, vv in sorted(d.items())}
    return {"latest": latest, "series": series}
