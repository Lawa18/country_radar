# app/providers/eurostat_provider.py
from __future__ import annotations

"""
Eurostat provider (Data API) for Country Radar
- HICP YoY (monthly):   prc_hicp_manr, coicop=CP00, geo=ISO2
- Unemployment (monthly): une_rt_m, s_adj=SA, sex=T, age=Y15-74, unit=PC_ACT, geo=ISO2
- Debt-to-GDP (annual): gov_10dd_edpt1, sector=S13, na_item=GD, unit=PC_GDP, geo=ISO2

Returns:
- monthly series as {"YYYY-MM": float, ...}
- annual series as {"YYYY": float, ...}
- {} on failure (never None)

Notes:
- Primary host: https://data-api.ec.europa.eu/api/v2/statistics/1.0/data/<dataset>?<filters>
- You can override base via env EUROSTAT_BASE_URL.
- We intentionally constrain filters so that the only varying dimension is "time".
- If Eurostat temporarily returns empty/invalid, upstream fallbacks (IMF/WB) take over.
"""

import os
import time
from typing import Dict, Any, Optional, Tuple

import httpx

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
EUROSTAT_BASE_URL = os.getenv(
    "EUROSTAT_BASE_URL",
    "https://data-api.ec.europa.eu/api/v2/statistics/1.0/data",
)
TIMEOUT = float(os.getenv("EUROSTAT_TIMEOUT_SEC", "8.0"))
RETRIES = int(os.getenv("EUROSTAT_RETRIES", "3"))
BACKOFF = float(os.getenv("EUROSTAT_BACKOFF", "0.8"))
TTL_SEC = int(os.getenv("EUROSTAT_TTL_SEC", "3600"))  # 1 hour default

USER_AGENT = "country-radar/1.0 (+eurostat_provider)"

# ------------------------------------------------------------------------------
# Tiny in-process TTL cache
# ------------------------------------------------------------------------------
class _TTLCache:
    def __init__(self, ttl_sec: int):
        self.ttl = ttl_sec
        self._data: Dict[str, Tuple[float, Any]] = {}

    def get(self, key: str) -> Optional[Any]:
        row = self._data.get(key)
        if not row:
            return None
        ts, val = row
        if (time.time() - ts) > self.ttl:
            try:
                del self._data[key]
            except Exception:
                pass
            return None
        return val

    def set(self, key: str, val: Any) -> None:
        self._data[key] = (time.time(), val)


_cache = _TTLCache(TTL_SEC)

# ------------------------------------------------------------------------------
# HTTP helpers
# ------------------------------------------------------------------------------
def _http_get_json(url: str, params: Dict[str, str]) -> Optional[Dict[str, Any]]:
    headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
    for attempt in range(1, RETRIES + 1):
        try:
            with httpx.Client(timeout=TIMEOUT, headers=headers) as client:
                r = client.get(url, params=params)
                r.raise_for_status()
                data = r.json()
                if isinstance(data, dict):
                    return data
        except Exception as e:
            # lightweight trace
            print(f"[Eurostat] attempt {attempt} failed {url} params={params}: {e}")
            if attempt < RETRIES:
                time.sleep(BACKOFF * attempt)
    return None


def _build_url(dataset: str) -> str:
    base = EUROSTAT_BASE_URL.rstrip("/")
    return f"{base}/{dataset}"


# ------------------------------------------------------------------------------
# SDMX-JSON parsing (Eurostat Data API)
# We assume only 'time' varies; other dimensions are pinned by filters.
# ------------------------------------------------------------------------------
def _parse_sdmx_time_series(payload: Dict[str, Any]) -> Dict[str, float]:
    """
    Parse Eurostat SDMX-JSON when only 'time' varies.
    Returns a dict mapping period string -> float value.
    If structure is unexpected or empty, returns {}.
    """
    if not payload or "value" not in payload or "dimension" not in payload:
        return {}

    value = payload.get("value", {})
    if not isinstance(value, dict) or not value:
        return {}

    dim = payload.get("dimension", {})
    if not isinstance(dim, dict):
        return {}

    # time dimension can be labeled 'time' or 'TIME' depending on dataset
    time_dim = dim.get("time") or dim.get("TIME") or {}
    categories = time_dim.get("category", {}) if isinstance(time_dim, dict) else {}
    time_index = categories.get("index", {}) if isinstance(categories, dict) else {}

    if not isinstance(time_index, dict) or not time_index:
        # Some responses have a flat 'label' form; try to fall back to keys in 'value'
        # but without reliable time mapping we can't safely parse chronological order.
        # Return {} to let upstream fallbacks take over.
        return {}

    # Reverse map: obs_key (str index) -> time_label (e.g., "2024-06" or "2023")
    # SDMX-JSON: value keys are observation indices (as strings); here we map them to time labels.
    index_to_time = {str(idx): tlabel for tlabel, idx in time_index.items()}

    out: Dict[str, float] = {}
    for obs_idx_str, v in value.items():
        tlabel = index_to_time.get(str(obs_idx_str))
        if tlabel is None:
            continue
        try:
            fv = float(v)
        except Exception:
            continue
        out[tlabel] = fv

    # sort chronologically if labels are consistent; otherwise return as-is
    # Labels are 'YYYY' for annual or 'YYYY-MM' for monthly. Lexicographic sort works.
    return dict(sorted(out.items()))


# ------------------------------------------------------------------------------
# ISO helpers
# ------------------------------------------------------------------------------
def _normalize_iso2(iso2: str) -> str:
    # Eurostat uses GB (not UK), EL (not GR) in some contexts; handle the common edge cases.
    if not iso2:
        return iso2
    iso2 = iso2.strip().upper()
    if iso2 == "UK":
        return "GB"
    if iso2 == "EL":
        return "GR"
    return iso2


# ------------------------------------------------------------------------------
# Public API: Three wrapper functions aligned with the service
# ------------------------------------------------------------------------------
def eurostat_hicp_yoy_monthly(iso2: str) -> Dict[str, float]:
    """
    HICP YoY (%) monthly:
    - Dataset: prc_hicp_manr
    - Filters: coicop=CP00 (All-items HICP), geo=ISO2
    Output: {"YYYY-MM": float, ...}
    """
    iso2n = _normalize_iso2(iso2)
    cache_key = f"eurostat:hicp:{iso2n}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    url = _build_url("prc_hicp_manr")
    params = {
        "coicop": "CP00",  # All-items HICP
        "geo": iso2n,
        # keep default: no 'unit' here; dataset is annual rate by construction
    }
    data = _http_get_json(url, params)
    series = _parse_sdmx_time_series(data or {})
    _cache.set(cache_key, series)
    return series


def eurostat_unemployment_rate_monthly(iso2: str) -> Dict[str, float]:
    """
    Unemployment rate (% of active population), monthly, seasonally adjusted:
    - Dataset: une_rt_m
    - Filters: s_adj=SA, sex=T, age=Y15-74, unit=PC_ACT, geo=ISO2
    Output: {"YYYY-MM": float, ...}
    """
    iso2n = _normalize_iso2(iso2)
    cache_key = f"eurostat:unemp:{iso2n}"
    if (cached := _cache.get(cache_key)) is not None:
        return cached

    url = _build_url("une_rt_m")
    params = {
        "s_adj": "SA",
        "sex": "T",
        "age": "Y15-74",
        "unit": "PC_ACT",
        "geo": iso2n,
    }
    data = _http_get_json(url, params)
    series = _parse_sdmx_time_series(data or {})
    _cache.set(cache_key, series)
    return series


def eurostat_debt_to_gdp_annual(iso2: str) -> Dict[str, float]:
    """
    General government gross debt (% of GDP), annual:
    - Dataset: gov_10dd_edpt1
    - Filters: sector=S13 (general government), na_item=GD (gross debt), unit=PC_GDP, geo=ISO2
    Output: {"YYYY": float, ...}
    """
    iso2n = _normalize_iso2(iso2)
    cache_key = f"eurostat:debtgdp:{iso2n}"
    if (cached := _cache.get(cache_key)) is not None:
        return cached

    url = _build_url("gov_10dd_edpt1")
    params = {
        "sector": "S13",
        "na_item": "GD",
        "unit": "PC_GDP",
        "geo": iso2n,
    }
    data = _http_get_json(url, params)
    series = _parse_sdmx_time_series(data or {})
    _cache.set(cache_key, series)
    return series
