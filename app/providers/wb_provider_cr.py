# app/providers/wb_provider_cr.py
from __future__ import annotations

from typing import Dict, Any, Optional, Tuple
import time
import math
import httpx

# Minimal WDI client with 1h TTL cache
_WB_BASE = "https://api.worldbank.org/v2/country"
_TIMEOUT = 6.0
_CACHE_TTL = 3600

class _TTLCache:
    def __init__(self, ttl: int) -> None:
        self.ttl = ttl
        self._store: Dict[str, Tuple[float, Any]] = {}

    def get(self, k: str) -> Optional[Any]:
        hit = self._store.get(k)
        if not hit:
            return None
        exp, v = hit
        if exp < time.time():
            self._store.pop(k, None)
            return None
        return v

    def set(self, k: str, v: Any) -> None:
        self._store[k] = (time.time() + self.ttl, v)

_cache = _TTLCache(_CACHE_TTL)

_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "country-radar/1.0 (+wb_fallback)",
}

def _norm_area(code: str) -> str:
    # WB accepts iso2 or iso3; lower/upper both work. Keep simple.
    return (code or "").strip()

def _safe_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except Exception:
        return None

def _wb_fetch_series(area: str, indicator: str, per_page: int = 20000) -> Dict[str, float]:
    """
    Returns { 'YYYY': float, ... } skipping nulls.
    """
    area = _norm_area(area)
    if not area:
        return {}
    key = f"{area}::{indicator}"
    hit = _cache.get(key)
    if hit is not None:
        return hit

    url = f"{_WB_BASE}/{area}/indicator/{indicator}"
    params = {"format": "json", "per_page": str(per_page)}
    try:
        with httpx.Client(timeout=_TIMEOUT, follow_redirects=True, headers=_HEADERS) as client:
            r = client.get(url, params=params)
            if r.status_code != 200:
                return {}
            data = r.json()
    except Exception:
        return {}

    # Shape: [meta, [ {date:"2024", value:1.23}, ... ]]
    if not isinstance(data, list) or len(data) < 2 or not isinstance(data[1], list):
        return {}

    out: Dict[str, float] = {}
    for row in data[1]:
        y = str(row.get("date"))
        v = _safe_float(row.get("value"))
        if y and v is not None and y.isdigit() and len(y) == 4:
            out[y] = v
    _cache.set(key, out)
    return out

# ---- Public API expected by indicator_service ----
# CPI inflation, annual % — FP.CPI.TOTL.ZG
def wb_cpi_inflation_annual_pct(iso2_or_3: str) -> Dict[str, float]:
    return _wb_fetch_series(iso2_or_3, "FP.CPI.TOTL.ZG")

# Unemployment, total (% of labor force) — SL.UEM.TOTL.ZS
def wb_unemployment_rate_annual_pct(iso2_or_3: str) -> Dict[str, float]:
    return _wb_fetch_series(iso2_or_3, "SL.UEM.TOTL.ZS")

# Official FX rate (LCU per USD, period average) — PA.NUS.FCRF
def wb_fx_lcu_per_usd_annual(iso2_or_3: str) -> Dict[str, float]:
    return _wb_fetch_series(iso2_or_3, "PA.NUS.FCRF")

# Total reserves (includes gold, current US$) — FI.RES.TOTL.CD
def wb_total_reserves_usd_annual(iso2_or_3: str) -> Dict[str, float]:
    return _wb_fetch_series(iso2_or_3, "FI.RES.TOTL.CD")

# GDP growth (annual %) — NY.GDP.MKTP.KD.ZG (real)
def wb_gdp_growth_annual_pct(iso2_or_3: str) -> Dict[str, float]:
    return _wb_fetch_series(iso2_or_3, "NY.GDP.MKTP.KD.ZG")

# Current account balance (% of GDP) — BN.CAB.XOKA.GD.ZS
def wb_current_account_balance_pct_gdp_annual(iso2_or_3: str) -> Dict[str, float]:
    return _wb_fetch_series(iso2_or_3, "BN.CAB.XOKA.GD.ZS")

# Government Effectiveness (WGI) — GE.EST
def wb_government_effectiveness_index_annual(iso2_or_3: str) -> Dict[str, float]:
    return _wb_fetch_series(iso2_or_3, "GE.EST")
