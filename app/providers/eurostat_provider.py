# app/providers/eurostat_provider.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import time
import math
import httpx

"""
Eurostat Statistics API (JSON-stat 2.0)

We keep requests narrow with lastTimePeriod to avoid async/huge payloads.
We also normalize common geo quirks (GB->UK, GR->EL).

Exports (used by Country Radar):
  - eurostat_hicp_yoy_monthly(iso2)            # HICP YoY, monthly
  - eurostat_unemployment_rate_monthly(iso2)   # Unemployment %, monthly SA
  - eurostat_debt_to_gdp_annual(iso2)          # General govt gross debt, %GDP, annual
"""

# --------------------------------------------------------------------
# Endpoint config (host failover) and HTTP behavior
# --------------------------------------------------------------------

# Prefer the new data-api host; keep the ec host as a failover.
_EUROSTAT_HOSTS = (
    "https://data-api.ec.europa.eu/api/dissemination/statistics/1.0/data",
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data",
)

_HTTP_TIMEOUT = 8.0   # seconds
_HTTP_RETRIES = 2      # attempts per host (1 + retries)
_BACKOFF_SEC = 0.35

# Narrow windows to keep responses small & fast
_LAST_PERIODS_MONTHLY = 36   # ~3 years
_LAST_PERIODS_ANNUAL  = 15   # ~15 years

_HEADERS = {
    "Accept": "application/json",
    "Accept-Language": "en",
    "User-Agent": "country-radar/1.0 (+eurostat_provider)",
}

# --------------------------------------------------------------------
# Tiny in-process TTL cache (sufficient for Render free/shared)
# --------------------------------------------------------------------

class _TTLCache:
    def __init__(self, ttl_seconds: int = 3600) -> None:
        self.ttl = ttl_seconds
        self._store: Dict[str, Tuple[float, Any]] = {}

    def _now(self) -> float:
        return time.time()

    def _mk(self, dataset: str, params: Dict[str, Any]) -> str:
        items = sorted((k, str(v)) for k, v in params.items())
        return f"{dataset}|{items}"

    def get(self, dataset: str, params: Dict[str, Any]) -> Optional[Any]:
        key = self._mk(dataset, params)
        hit = self._store.get(key)
        if not hit:
            return None
        exp, val = hit
        if exp < self._now():
            self._store.pop(key, None)
            return None
        return val

    def set(self, dataset: str, params: Dict[str, Any], value: Any) -> None:
        key = self._mk(dataset, params)
        self._store[key] = (self._now() + self.ttl, value)

_cache = _TTLCache(ttl_seconds=3600)

def _http_client() -> httpx.Client:
    return httpx.Client(
        timeout=_HTTP_TIMEOUT,
        follow_redirects=True,
        headers=_HEADERS,
    )

# --------------------------------------------------------------------
# Low-level fetch + JSON-stat 2.0 parsing
# --------------------------------------------------------------------

def _fetch_dataset_json(dataset: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Try the configured Eurostat hosts in order with brief retries.
    Returns parsed JSON or None on failure.
    """
    cached = _cache.get(dataset, params)
    if cached is not None:
        return cached

    for base in _EUROSTAT_HOSTS:
        url = f"{base}/{dataset}"
        for attempt in range(_HTTP_RETRIES + 1):
            try:
                with _http_client() as client:
                    resp = client.get(url, params=params)
                if resp.status_code == 200:
                    j = resp.json()
                    # Sometimes Eurostat responds with JSON carrying a 'warning' for async jobs;
                    # we avoid that by keeping lastTimePeriod tight, but still accept the payload.
                    _cache.set(dataset, params, j)
                    return j
            except Exception:
                pass
            time.sleep(_BACKOFF_SEC * (attempt + 1))
        # move to next host
    return None

def _jsonstat_series_from_dataset(j: Dict[str, Any]) -> Dict[str, float]:
    """
    Parse JSON-stat 2.0 dataset (already filtered to a single series except time)
    into {period -> float}. Accepts dense (list) and sparse (dict) 'value'.
    Period labels are Eurostat codes, usually 'YYYY-MM' (monthly) or 'YYYY' (annual).
    """
    if not isinstance(j, dict) or j.get("class") != "dataset":
        return {}

    dims = j.get("dimension") or {}
    dim_ids: List[str] = (dims.get("id") or [])[:]
    sizes: List[int] = (dims.get("size") or [])[:]
    if not dim_ids or not sizes or len(dim_ids) != len(sizes):
        return {}

    time_dim_name = "time" if "time" in dims else ("TIME_PERIOD" if "TIME_PERIOD" in dims else None)
    if not time_dim_name:
        return {}

    time_cat = (dims.get(time_dim_name) or {}).get("category") or {}
    idx_map: Dict[str, int] = time_cat.get("index") or {}
    ordered = sorted(((pos, code) for code, pos in idx_map.items()), key=lambda x: int(x[0]))
    time_labels: List[str] = [code for _, code in ordered]

    try:
        t_pos = dim_ids.index(time_dim_name)
    except ValueError:
        return {}

    # stride = product of sizes after t_pos
    stride = 1
    for s in sizes[t_pos + 1 :]:
        stride *= int(s)

    values = j.get("value")
    out: Dict[str, float] = {}

    def _get_at(flat_index: int) -> Optional[float]:
        if isinstance(values, list):
            if 0 <= flat_index < len(values):
                v = values[flat_index]
                try:
                    return float(v) if v is not None else None
                except Exception:
                    return None
            return None
        if isinstance(values, dict):
            v = values.get(str(flat_index))
            try:
                return float(v) if v is not None else None
            except Exception:
                return None
        return None

    fixed_coords = [0] * len(dim_ids)

    # Iterate time in order (Eurostat lists oldest→newest)
    prod_template: List[int] = [1] * len(dim_ids)
    prod = 1
    for k in range(len(dim_ids) - 1, -1, -1):
        prod_template[k] = prod
        prod *= sizes[k]

    for i, label in enumerate(time_labels):
        coords = fixed_coords[:]
        coords[t_pos] = i
        # row-major flat index
        flat = sum(coords[k] * prod_template[k] for k in range(len(dim_ids)))
        v = _get_at(flat)
        if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
            continue
        out[label] = v

    return out

def _get_jsonstat_series(dataset: str, params: Dict[str, Any]) -> Dict[str, float]:
    j = _fetch_dataset_json(dataset, params)
    if not isinstance(j, dict):
        return {}
    # Respect explicit error payloads
    if j.get("error") and j.get("class") != "dataset":
        return {}
    return _jsonstat_series_from_dataset(j)

# --------------------------------------------------------------------
# Public helpers (HICP YoY, Unemployment monthly, Debt %GDP annual)
# --------------------------------------------------------------------

def _normalize_geo(iso2: str) -> str:
    """
    Eurostat uses 'EL' (Greece) and 'UK' (United Kingdom).
    Normalize common inputs.
    """
    code = (iso2 or "").strip().upper()
    if code == "GR":
        return "EL"
    if code == "GB":
        return "UK"
    return code

def eurostat_hicp_yoy_monthly(iso2: str) -> Dict[str, float]:
    """
    HICP annual rate of change (YoY), monthly.
    Dataset: prc_hicp_manr
      - coicop: CP00 (All-items)
      - unit:   RCH_A (Annual rate of change)  [fallback: RTE if needed]
      - geo:    ISO2 (normalized)
      - lastTimePeriod: limited (fast)
    Returns: {"YYYY-MM": pct, ...}
    """
    base_params = {
        "coicop": "CP00",
        "geo": _normalize_geo(iso2),
        "lastTimePeriod": _LAST_PERIODS_MONTHLY,
        "lang": "EN",
        "format": "JSON",
    }
    # Try preferred unit, then a permissive fallback
    for unit in ("RCH_A", "RTE"):
        params = dict(base_params)
        params["unit"] = unit
        series = _get_jsonstat_series("prc_hicp_manr", params)
        if series:
            return series
    return {}

def eurostat_unemployment_rate_monthly(iso2: str) -> Dict[str, float]:
    """
    Unemployment rate (percentage of labour force), monthly, seasonally adjusted, total.
    Dataset: une_rt_m
      - s_adj: SA
      - sex:   T
      - age:   TOTAL
      - unit:  PC_ACT
      - geo:   ISO2 (normalized)
    Returns: {"YYYY-MM": pct, ...}
    """
    params = {
        "s_adj": "SA",
        "sex": "T",
        "age": "TOTAL",
        "unit": "PC_ACT",
        "geo": _normalize_geo(iso2),
        "lastTimePeriod": _LAST_PERIODS_MONTHLY,
        "lang": "EN",
        "format": "JSON",
    }
    return _get_jsonstat_series("une_rt_m", params)

def eurostat_debt_to_gdp_annual(iso2: str) -> Dict[str, float]:
    """
    General government gross debt, % of GDP (annual).
    Primary dataset: teina225   (clean single-indicator table)
      - unit:    PC_GDP
      - sector:  S13
      - na_item: GD
    Fallback dataset: gov_10dd_edpt1 (broader, but commonly used for debt ratios)

    Returns: {"YYYY": pct, ...}
    """
    base_geo = _normalize_geo(iso2)

    # Primary: teina225 (compact, reliable)
    p = {
        "unit": "PC_GDP",
        "sector": "S13",
        "na_item": "GD",
        "geo": base_geo,
        "lastTimePeriod": _LAST_PERIODS_ANNUAL,
        "lang": "EN",
        "format": "JSON",
    }
    series = _get_jsonstat_series("teina225", p)
    if series:
        return {k: v for k, v in series.items() if len(k) == 4 and k.isdigit()}

    # Fallback: gov_10dd_edpt1
    # Keep the same filters if accepted; if empty, try slightly looser filters.
    fb = dict(p)
    series = _get_jsonstat_series("gov_10dd_edpt1", fb)
    if series:
        return {k: v for k, v in series.items() if len(k) == 4 and k.isdigit()}

    # Final attempt with looser filter (drop na_item if needed)
    fb2 = {
        "unit": "PC_GDP",
        "sector": "S13",
        "geo": base_geo,
        "lastTimePeriod": _LAST_PERIODS_ANNUAL,
        "lang": "EN",
        "format": "JSON",
    }
    series = _get_jsonstat_series("gov_10dd_edpt1", fb2)
    if series:
        return {k: v for k, v in series.items() if len(k) == 4 and k.isdigit()}

    return {}
