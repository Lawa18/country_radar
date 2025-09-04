from __future__ import annotations
from typing import Dict, Any, Optional
import os
import httpx
from functools import lru_cache

IMF_BASE = os.getenv("IMF_BASE", "https://dataservices.imf.org/REST/SDMX_JSON.svc")
IMF_TIMEOUT = float(os.getenv("IMF_TIMEOUT", "8.0"))

def fetch_imf_sdmx_series(iso2: str) -> Dict[str, Dict[str, float]]:
    """
    Placeholder IMF fetcher returning empty maps.
    Your country-data logic will gracefully fallback to World Bank via wb_series/wb_entry.
    Later we will fill this with dataservices.imf.org calls.
    """
    return {}

def imf_debt_to_gdp_annual(iso3: str) -> Dict[str, float]:
    """
    Placeholder for IMF WEO annual debt/GDP ratio series (by ISO3).
    Returning {} ensures your strict compute order continues without breaking imports.
    """
    return {}
    
def _client() -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(IMF_TIMEOUT, connect=min(IMF_TIMEOUT/2, 4.0)),
        headers={"User-Agent": "country-radar/1.0 (+https://country-radar.onrender.com)"},
        limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
    )

def _compactdata(dataset: str, key: str, start: Optional[str], end: Optional[str]) -> Dict[str, Any]:
    params = {}
    if start: params["startPeriod"] = start
    if end:   params["endPeriod"]   = end
    url = f"{IMF_BASE}/CompactData/{dataset}/{key}"
    with _client() as c:
        r = c.get(url, params=params)
        r.raise_for_status()
        return r.json()

def _parse_obs_to_map(payload: Dict[str, Any]) -> Dict[str, float]:
    """
    Defensive SDMX parser: returns {period -> value} for either monthly (YYYY-MM) or annual (YYYY).
    Works with common SDMX 2.1 JSON variants.
    """
    try:
        compact = payload.get("CompactData") or {}
        dataset = compact.get("DataSet") or {}
        series = dataset.get("Series") or {}
        if isinstance(series, list):
            series = series[0] if series else {}
        obs = series.get("Obs") or []
        out: Dict[str, float] = {}
        for o in obs:
            period = o.get("TIME_PERIOD") or o.get("@TIME_PERIOD") or o.get("Time") or o.get("@Time")
            val = o.get("OBS_VALUE") or o.get("@OBS_VALUE") or o.get("value") or o.get("@value")
            if period is None or val in (None, "", "NA"):
                continue
            try:
                out[str(period)] = float(val)
            except Exception:
                continue
        # sort by period if it looks like year or year-month
        return dict(sorted(out.items(), key=lambda k: k[0]))
    except Exception as e:
        print(f"[IMF] parse error: {e}")
        return {}

@lru_cache(maxsize=256)
def imf_series_map(dataset: str, key: str, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, float]:
    """
    Fetches a trimmed IMF series and returns {period: value}.
    Example:
      dataset="IFS", key="M.DEU.PCPI_PC_CP_A_PT"   # CPI yoy %, Germany, monthly
      start="2010-01", end="2025-12"
    """
    try:
        data = _compactdata(dataset, key, start, end)
        return _parse_obs_to_map(data)
    except Exception as e:
        print(f"[IMF] fetch error {dataset}/{key}: {e}")
        return {}

def imf_latest(dataset: str, key: str, start: Optional[str] = None, end: Optional[str] = None) -> Optional[tuple[str, float]]:
    series = imf_series_map(dataset, key, start, end)
    if not series:
        return None
    # last period lexicographically -> latest
    last = sorted(series.keys())[-1]
    return last, series[last]
