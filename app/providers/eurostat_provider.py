from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import time
import math
import httpx

# --------------------------------------------------------------------
# Eurostat Statistics API (JSON-stat 2.0)
# Docs: host/service/version/response_type/dataset?filters
#   e.g. https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_manr?geo=DE&coicop=CP00&unit=RCH_A&lastTimePeriod=24
#   Time filters supported: lastTimePeriod, sinceTimePeriod, untilTimePeriod
# --------------------------------------------------------------------

# Prefer the new data-api host, but keep the official ec.europa.eu/eurostat host as failover.
_EUROSTAT_HOSTS = (
    "https://data-api.ec.europa.eu/api/dissemination/statistics/1.0/data",
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data",
)

_HTTP_TIMEOUT = 8.0  # seconds
_HTTP_RETRIES = 2     # total attempts per host (1 + retries)
_BACKOFF_SEC = 0.5

# Default narrow windows to avoid heavy async responses
_LAST_PERIODS_MONTHLY = 36  # 3 years
_LAST_PERIODS_ANNUAL = 15   # 15 years

# --------------------------------------------------------------------
# Tiny TTL cache (per-process; good enough for Render free tier)
# --------------------------------------------------------------------

class _TTLCache:
    def __init__(self, ttl_seconds: int = 3600) -> None:
        self.ttl = ttl_seconds
        self._store: Dict[str, Tuple[float, Any]] = {}

    def _now(self) -> float:
        return time.time()

    def _mk(self, dataset: str, params: Dict[str, Any]) -> str:
        # sort params for stable key
        items = sorted((k, str(v)) for k, v in params.items())
        return f"{dataset}|{items}"

    def get(self, dataset: str, params: Dict[str, Any]) -> Optional[Any]:
        key = self._mk(dataset, params)
        hit = self._store.get(key)
        if not hit:
            return None
        exp, val = hit
        if exp < self._now():
            # expired
            self._store.pop(key, None)
            return None
        return val

    def set(self, dataset: str, params: Dict[str, Any], value: Any) -> None:
        key = self._mk(dataset, params)
        self._store[key] = (self._now() + self.ttl, value)


_cache = _TTLCache(ttl_seconds=3600)

# --------------------------------------------------------------------
# HTTP helpers
# --------------------------------------------------------------------

def _http_client() -> httpx.Client:
    # new client per call keeps it simple and safe on Render; cost is tiny
    return httpx.Client(
        timeout=_HTTP_TIMEOUT,
        follow_redirects=True,
        headers={
            "Accept": "application/json",
            "Accept-Language": "en",
            "User-Agent": "country-radar/1.0 (+eurostat; contact=api@country-radar)",
        },
    )

def _fetch_dataset_json(dataset: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Try hosts in order with small retries.
    Returns parsed JSON (dict) or None on failure.
    """
    # Cache first
    cached = _cache.get(dataset, params)
    if cached is not None:
        return cached

    last_exc: Optional[Exception] = None
    for base in _EUROSTAT_HOSTS:
        url = f"{base}/{dataset}"
        for attempt in range(_HTTP_RETRIES + 1):
            try:
                with _http_client() as client:
                    resp = client.get(url, params=params)
                # Basic success
                if resp.status_code == 200:
                    j = resp.json()
                    # Some big pulls can trigger async 'warning' objects; our requests are filtered to avoid that.
                    _cache.set(dataset, params, j)
                    return j
                # If API returns application/json with error/warning bodies, try to surface it but still allow failover
                # 413-like async warnings may return 200 with {"warning": ...}; we avoid by keeping lastTimePeriod tight.
            except Exception as e:
                last_exc = e
            # tiny backoff then retry (or switch host)
            time.sleep(_BACKOFF_SEC * (attempt + 1))
        # try next host
    # total failure
    return None

# --------------------------------------------------------------------
# JSON-stat 2.0 -> time series parsing
# --------------------------------------------------------------------

def _jsonstat_series_from_dataset(j: Dict[str, Any]) -> Dict[str, float]:
    """
    Given a JSON-stat 2.0 dataset (already filtered to ONE series except time),
    return an ordered dict-like (regular dict in chronological order):
        { "YYYY" or "YYYY-MM": float, ... }
    Works with dense list or sparse dict 'value' representations.
    """
    if not j or j.get("class") != "dataset":
        return {}

    dims = j.get("dimension") or {}
    dim_ids: List[str] = (dims.get("id") or [])[:]
    sizes: List[int] = (dims.get("size") or [])[:]

    if not dim_ids or not sizes or len(dim_ids) != len(sizes):
        return {}

    # Time dimension can be named "time" or "TIME_PERIOD"
    time_dim_name = "time" if "time" in dims else ("TIME_PERIOD" if "TIME_PERIOD" in dims else None)
    if not time_dim_name:
        return {}

    # Build ordered list of time labels
    time_cat = (dims.get(time_dim_name) or {}).get("category") or {}
    idx_map: Dict[str, int] = time_cat.get("index") or {}
    # idx_map maps code -> position; we need codes ordered by position
    ordered = sorted(((pos, code) for code, pos in idx_map.items()), key=lambda x: int(x[0]))
    time_labels: List[str] = [code for _, code in ordered]

    # Locate time dimension index and stride
    try:
        t_pos = dim_ids.index(time_dim_name)
    except ValueError:
        return {}
    # product of sizes after t_pos
    stride = 1
    for s in sizes[t_pos + 1 :]:
        stride *= int(s)

    values = j.get("value")
    out: Dict[str, float] = {}

    def _get_at(flat_index: int) -> Optional[float]:
        if isinstance(values, list):
            if 0 <= flat_index < len(values):
                v = values[flat_index]
                return float(v) if v is not None else None
            return None
        elif isinstance(values, dict):
            v = values.get(str(flat_index))
            return float(v) if v is not None else None
        else:
            return None

    # Build a zero vector index for other dimensions (fixed by our filters)
    fixed_coords = [0] * len(dim_ids)

    # Iterate time indices in order
    for i, label in enumerate(time_labels):
        # flat index = sum_k coord[k] * product_{k+1..} size
        coords = fixed_coords[:]
        coords[t_pos] = i
        # compute flat index in row-major order
        flat = 0
        prod = 1
        for k in range(len(dim_ids) - 1, -1, -1):
            flat += coords[k] * prod
            prod *= sizes[k]
        v = _get_at(flat)
        if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
            continue
        out[label] = v

    return out

def _get_jsonstat_series(dataset: str, params: Dict[str, Any]) -> Dict[str, float]:
    j = _fetch_dataset_json(dataset, params)
    if not isinstance(j, dict):
        return {}
    # Some error payloads return {"error": {...}} — just bail if so
    if j.get("error") or j.get("warning"):
        # Still try to parse if a dataset exists (rare), otherwise empty
        if j.get("class") != "dataset":
            return {}
    return _jsonstat_series_from_dataset(j)

# --------------------------------------------------------------------
# Public helpers for Country Radar
# --------------------------------------------------------------------

def _normalize_geo(iso2: str) -> str:
    """
    Eurostat uses 'EL' for Greece and 'UK' for United Kingdom.
    Accept common inputs and normalize.
    """
    code = (iso2 or "").strip().upper()
    if code == "GR":
        return "EL"
    if code == "GB":
        return "UK"
    return code

def eurostat_hicp_yoy_monthly(iso2: str) -> Dict[str, float]:
    """
    HICP - monthly, annual rate of change (YoY).
    Dataset: prc_hicp_manr
      - coicop: CP00  (All-items)
      - unit:   RCH_A (Annual rate of change)   [fallback: RTE]
      - geo:    ISO2 (normalized)
      - lastTimePeriod: ~3 years (trim payload)
    Returns: {"YYYY-MM": pct, ...}
    """
    base_params = {
        "coicop": "CP00",
        "geo": _normalize_geo(iso2),
        "lastTimePeriod": _LAST_PERIODS_MONTHLY,
        "lang": "EN",
        "format": "JSON",
    }
    # Try preferred 'RCH_A' then fallback to 'RTE' (some mirrors/snapshots use different unit codes)
    for unit in ("RCH_A", "RTE"):
        params = dict(base_params)
        params["unit"] = unit
        series = _get_jsonstat_series("prc_hicp_manr", params)
        if series:
            return series
    return {}

def eurostat_unemployment_rate_monthly(iso2: str) -> Dict[str, float]:
    """
    Unemployment rate, monthly, seasonally adjusted, total.
    Dataset: une_rt_m
      - s_adj: SA
      - sex:   T
      - age:   TOTAL  (dataset has TOTAL, Y_LT25, Y25-74)
      - unit:  PC_ACT (percentage of labour force)
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
    Primary dataset: teina225 (cleaner for the single indicator)
      - unit:   PC_GDP
      - sector: S13
      - na_item: GD  (Government consolidated gross debt)
      - geo: ISO2 (normalized)
    Returns: {"YYYY": pct, ...}
    """
    params = {
        "unit": "PC_GDP",
        "sector": "S13",
        "na_item": "GD",
        "geo": _normalize_geo(iso2),
        "lastTimePeriod": _LAST_PERIODS_ANNUAL,
        "lang": "EN",
        "format": "JSON",
    }
    series = _get_jsonstat_series("teina225", params)
    if series:
        # Keep only year labels just in case
        return {k: v for k, v in series.items() if len(k) == 4 and k.isdigit()}

    # Fallback: broader GOV_10DD_EDPT1 (same filters should work there)
    fb_params = dict(params)
    series = _get_jsonstat_series("gov_10dd_edpt1", fb_params)
    if series:
        return {k: v for k, v in series.items() if len(k) == 4 and k.isdigit()}

    return {}
