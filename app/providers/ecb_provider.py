# app/providers/ecb_provider.py
from __future__ import annotations

from typing import Dict, List, Tuple, Optional, Any
import time
import httpx

"""
ECB Policy Rate (MRO) provider

- Uses ECB Data Portal SDMX API (JSON) to fetch the Main Refinancing Operations rate.
- Prefers monthly series; falls back to daily (compressed to monthly) and then business-week.
- Exposes:
    EURO_AREA_ISO2: List[str]
    ecb_policy_rate_for_country(iso2) -> Dict[str, float]  # {"YYYY-MM": rate, ...}
"""

# -------------------------------------------------------------------
# Euro area ISO2 membership (as of 2025; includes Croatia)
# -------------------------------------------------------------------
EURO_AREA_ISO2: List[str] = [
    "AT", "BE", "HR", "CY", "EE", "FI", "FR", "DE", "GR", "IE",
    "IT", "LV", "LT", "LU", "MT", "NL", "PT", "SK", "SI", "ES",
    # Accept the Eurostat alias 'EL' for Greece for convenience:
    "EL",
]

# -------------------------------------------------------------------
# HTTP settings & hosts
# -------------------------------------------------------------------
# New data portal (primary) + legacy SDW REST (fallback; transparently redirects)
_ECB_DATA_HOSTS = (
    "https://data-api.ecb.europa.eu/service/data",           # primary
    "https://sdw-wsrest.ecb.europa.eu/service/data",         # fallback/redirect
)

# FM dataset series keys for MRO (Main refinancing operations)
# Monthly preferred; fall back to daily, then business-week.
_MRO_KEYS = (
    "FM/M.U2.EUR.4F.KR.MRR_FR.LEV",  # Monthly
    "FM/D.U2.EUR.4F.KR.MRR_FR.LEV",  # Daily
    "FM/B.U2.EUR.4F.KR.MRR_FR.LEV",  # Business-week
)

_TIMEOUT = 8.0
_RETRIES = 2
_CACHE_TTL_SEC = 1800  # 30 minutes
_HEADERS = {
    "Accept": "application/json",  # we also append format=sdmx-json explicitly
    "User-Agent": "country-radar/1.0 (+ecb_provider)",
}

# -------------------------------------------------------------------
# Tiny in-process TTL cache
# -------------------------------------------------------------------
class _TTLCache:
    def __init__(self, ttl_seconds: int = _CACHE_TTL_SEC) -> None:
        self.ttl = ttl_seconds
        self._store: Dict[str, Tuple[float, Any]] = {}

    def get(self, key: str) -> Optional[Any]:
        hit = self._store.get(key)
        if not hit:
            return None
        exp, value = hit
        if exp < time.time():
            self._store.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any) -> None:
        self._store[key] = (time.time() + self.ttl, value)

_cache = _TTLCache()

def _client() -> httpx.Client:
    return httpx.Client(timeout=_TIMEOUT, follow_redirects=True, headers=_HEADERS)

# -------------------------------------------------------------------
# SDMX-JSON parse (ECB Data Portal)
# -------------------------------------------------------------------
def _parse_sdmx_json(payload: Dict[str, Any]) -> Dict[str, float]:
    """
    Parse SDMX-JSON ("format=sdmx-json") into {time_period -> value}.
    Works for single-series responses; returns {} on errors.
    """
    try:
        datasets = payload.get("dataSets")
        if not datasets:
            return {}
        ds0 = datasets[0]
        series_map = ds0.get("series") or {}
        if not series_map:
            return {}

        # There should be a single series in a fully specified key query
        first_key = next(iter(series_map.keys()))
        series = series_map[first_key] or {}
        obs = series.get("observations") or {}
        if not isinstance(obs, dict) or not obs:
            return {}

        # Time coordinates are in structure.dimensions.observation[0].values
        dims = payload.get("structure", {}).get("dimensions", {}).get("observation", [])
        if not dims:
            return {}
        time_values = dims[0].get("values") or []
        # Build mapping index -> time-label (e.g., "1999-01", "2024-09-18")
        idx_to_time: Dict[int, str] = {i: (v.get("id") or "") for i, v in enumerate(time_values)}

        out: Dict[str, float] = {}
        for k, arr in obs.items():
            # observations: { "index": [ value, ...attrs ] }
            try:
                i = int(k)
            except Exception:
                continue
            t = idx_to_time.get(i)
            if not t:
                continue
            # value can be number or [number, ...]
            val = None
            if isinstance(arr, list) and arr:
                val = arr[0]
            elif isinstance(arr, (int, float)):
                val = arr
            try:
                if val is not None:
                    out[str(t)] = float(val)
            except Exception:
                continue
        return out
    except Exception:
        return {}

# -------------------------------------------------------------------
# Fetch helpers
# -------------------------------------------------------------------
def _fetch_sdmx_series(series_key: str, start_period: str = "1999-01-01") -> Dict[str, float]:
    """
    Retrieve a series via SDMX JSON from ECB hosts with short retries.
    series_key like "FM/M.U2.EUR.4F.KR.MRR_FR.LEV"
    """
    cache_key = f"ECB::{series_key}::{start_period}"
    hit = _cache.get(cache_key)
    if hit is not None:
        return hit

    params = {
        "startPeriod": start_period,
        "format": "sdmx-json",
    }

    last_exc: Optional[Exception] = None
    for base in _ECB_DATA_HOSTS:
        url = f"{base}/{series_key}"
        for attempt in range(_RETRIES + 1):
            try:
                with _client() as client:
                    resp = client.get(url, params=params)
                if resp.status_code == 200:
                    data = resp.json()
                    series = _parse_sdmx_json(data)
                    if series:
                        _cache.set(cache_key, series)
                        return series
                    # Even if empty, keep trying fallbacks/hosts
            except Exception as e:
                last_exc = e
            time.sleep(0.25 * (attempt + 1))
        # try next host
    return {}

# -------------------------------------------------------------------
# Utilities to normalize to MONTHLY keys ("YYYY-MM")
# -------------------------------------------------------------------
def _daily_to_monthly_last(series_daily: Dict[str, float]) -> Dict[str, float]:
    """
    Compress a daily series ("YYYY-MM-DD") to monthly by taking the last
    available observation within each month.
    """
    if not series_daily:
        return {}
    tuples: List[Tuple[Tuple[int, int, int], str, float]] = []
    for t, v in series_daily.items():
        try:
            y, m, d = int(t[0:4]), int(t[5:7]), int(t[8:10])
            tuples.append(((y, m, d), t, float(v)))
        except Exception:
            continue
    # sort ascending by date, then keep last for each (y,m)
    tuples.sort(key=lambda x: x[0])
    out: Dict[str, float] = {}
    for (y, m, _), _, val in tuples:
        out[f"{y:04d}-{m:02d}"] = val
    return out

def _maybe_to_monthly(series: Dict[str, float]) -> Dict[str, float]:
    """
    If keys are daily (YYYY-MM-DD), compress to YYYY-MM.
    If keys already monthly (YYYY-MM), return as-is.
    """
    if not series:
        return {}
    # Peek at any key
    k0 = next(iter(series.keys()))
    if len(k0) == 10 and k0[4] == "-" and k0[7] == "-":
        return _daily_to_monthly_last(series)
    return series

# -------------------------------------------------------------------
# Public API
# -------------------------------------------------------------------
def ecb_policy_rate_for_country(iso2: str) -> Dict[str, float]:
    """
    Returns {"YYYY-MM": policy_rate_percent, ...} for euro-area ISO2 countries.
    Non-euro countries -> {}.

    Source priority:
      1) FM.M.U2.EUR.4F.KR.MRR_FR.LEV (monthly)
      2) FM.D.U2.EUR.4F.KR.MRR_FR.LEV (daily -> compressed to monthly)
      3) FM.B.U2.EUR.4F.KR.MRR_FR.LEV (business-week -> treated as daily-like)
    """
    code = (iso2 or "").strip().upper()
    # Greece may be reported 'EL' in Eurostat; treat both EL/GR as euro
    if code not in EURO_AREA_ISO2 and not (code == "GR"):
        return {}

    # Try monthly → daily → business-week
    # We always return monthly keys in the final dict.
    # Start period early enough to cover all history; values are small anyway.
    for key in _MRO_KEYS:
        series = _fetch_sdmx_series(key, start_period="1999-01-01")
        if series:
            monthly = _maybe_to_monthly(series)
            if monthly:
                return monthly
    return {}
