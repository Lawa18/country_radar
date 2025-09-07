# app/providers/imf_provider.py
from __future__ import annotations

from typing import Dict, List, Tuple, Optional, Any
import time
import math

import httpx

"""
IMF providers (IFS + WEO) via SDMX-JSON CompactData API.

Exports used by Country Radar:
  - imf_cpi_yoy_monthly(iso2)              -> {"YYYY-MM": float}
  - imf_unemployment_rate_monthly(iso2)    -> {"YYYY-MM": float}
  - imf_fx_usd_monthly(iso2)               -> {"YYYY-MM": float}
  - imf_reserves_usd_monthly(iso2)         -> {"YYYY-MM": float}
  - imf_policy_rate_monthly(iso2)          -> {"YYYY-MM": float}
  - imf_gdp_growth_quarterly(iso2)         -> {"YYYY-Qn": float}
  - imf_weo_debt_to_gdp_annual(iso2)       -> {"YYYY": float}   # NEW (for debt_service tier 2)
"""

# ----------------------------
# Config & small in-memory TTL
# ----------------------------
_IMF_COMPACT_BASE = "https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData"
_DEFAULT_TIMEOUT = 7.5  # seconds
_MAX_RETRIES = 2
_CACHE_TTL = 3600  # seconds (~1 hour)

class _TTLCache:
    def __init__(self, ttl_seconds: int = _CACHE_TTL) -> None:
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

# -------------
# HTTP helpers
# -------------
_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "CountryRadar/1.0 (imf_provider)",
}

def _http_get_json(url: str, timeout: float = _DEFAULT_TIMEOUT) -> Optional[Dict[str, Any]]:
    for attempt in range(_MAX_RETRIES + 1):
        try:
            with httpx.Client(timeout=timeout, follow_redirects=True, headers=_HEADERS) as client:
                resp = client.get(url)
                if resp.status_code == 200:
                    return resp.json()
        except Exception:
            time.sleep(0.2 * (attempt + 1))
    return None

# -------------------------
# IMF SDMX-JSON parse utils
# -------------------------
def _norm_iso2_for_ifs(iso2: str) -> List[str]:
    """
    IMF IFS usually uses ISO2 like DE, FR, GB. Normalize aliases and try a couple of variants.
    """
    iso2 = (iso2 or "").upper()
    tries = [iso2]
    if iso2 == "UK":
        tries.append("GB")
    if iso2 == "EL":   # Eurostat alias for Greece
        tries.append("GR")
    return list(dict.fromkeys(tries))  # de-dup, preserve order

def _iso2_to_iso3(iso2: str) -> Optional[str]:
    try:
        import pycountry
        code = (iso2 or "").upper()
        if code == "UK":  # pycountry uses GB
            code = "GB"
        if code == "EL":  # pycountry uses GR
            code = "GR"
        country = pycountry.countries.get(alpha_2=code)
        return country.alpha_3 if country else None
    except Exception:
        return None

def _parse_compact_series(payload: Dict[str, Any]) -> Dict[str, float]:
    """
    Parses IMF CompactData JSON into {time_period -> value} with floats only.
    Returns empty dict if missing/invalid.
    """
    try:
        series = payload["CompactData"]["DataSet"]["Series"]
    except Exception:
        return {}
    if isinstance(series, list):
        if not series:
            return {}
        series = series[0]
    obs = series.get("Obs")
    if not obs:
        return {}

    out: Dict[str, float] = {}
    rows = obs if isinstance(obs, list) else [obs]
    for row in rows:
        t = row.get("@TIME_PERIOD")
        v = row.get("@OBS_VALUE")
        if t is None or v is None:
            continue
        try:
            fv = float(v)
            if math.isfinite(fv):
                out[str(t)] = fv
        except Exception:
            continue
    return out

def _fetch_compact_series(dataset: str, key: str, start_period: str = "2000") -> Dict[str, float]:
    """
    Pull a single SDMX series via CompactData:
      URL = {BASE}/{dataset}/{key}?startPeriod=YYYY
    e.g., IFS/M.DE.PCPI_IX or WEO/A.DEU.GGXWDG_NGDP
    """
    cache_key = f"IMF::{dataset}::{key}::{start_period}"
    hit = _cache.get(cache_key)
    if hit is not None:
        return hit

    url = f"{_IMF_COMPACT_BASE}/{dataset}/{key}?startPeriod={start_period}"
    data = _http_get_json(url)
    series = _parse_compact_series(data or {})
    if series:
        _cache.set(cache_key, series)
    return series

# ------------------------
# Transform helpers (YoY)
# ------------------------
def _yymm_key_to_tuple(k: str) -> Tuple[int, int]:
    # Accept "YYYY-MM" or "YYYYMmm"
    k = (k or "").strip()
    if len(k) == 7 and k[4] == "-":
        return int(k[:4]), int(k[5:7])
    if len(k) == 6 and (k[4] in ("M", "m")):
        return int(k[:4]), int(k[5:6])
    # fallback
    try:
        y = int(k[:4]); m = int(k[-2:])
        return y, m if 1 <= m <= 12 else 0
    except Exception:
        return 0, 0

def _yyqq_key_to_tuple(k: str) -> Tuple[int, int]:
    # Accept "YYYY-Qn"
    try:
        y = int(k[:4]); q = int(k[-1])
        return y, q if 1 <= q <= 4 else 0
    except Exception:
        return 0, 0

def _compute_yoy_from_level_monthly(level_series: Dict[str, float]) -> Dict[str, float]:
    """
    Given monthly levels (e.g., CPI index), compute YoY %:
      yoy_t = (level_t / level_{t-12} - 1) * 100
    """
    if not level_series:
        return {}
    items = sorted(level_series.items(), key=lambda kv: _yymm_key_to_tuple(kv[0]))
    out: Dict[str, float] = {}
    for i, (t, v) in enumerate(items):
        if i < 12:
            continue
        _, v_prev = items[i - 12]
        if v_prev and math.isfinite(v_prev) and v_prev != 0:
            out[t] = (v / v_prev - 1.0) * 100.0
    return out

def _compute_yoy_from_level_quarterly(level_series: Dict[str, float]) -> Dict[str, float]:
    """
    Given quarterly levels (e.g., real GDP SA), compute YoY % vs same quarter prev year:
      yoy_t = (level_t / level_{t-4} - 1) * 100
    """
    if not level_series:
        return {}
    items = sorted(level_series.items(), key=lambda kv: _yyqq_key_to_tuple(kv[0]))
    out: Dict[str, float] = {}
    for i, (t, v) in enumerate(items):
        if i < 4:
            continue
        _, v_prev = items[i - 4]
        if v_prev and math.isfinite(v_prev) and v_prev != 0:
            out[t] = (v / v_prev - 1.0) * 100.0
    return out

# ---------------------------
# Public provider functions
# ---------------------------
def imf_cpi_yoy_monthly(iso2: str) -> Dict[str, float]:
    """
    CPI YoY % (monthly), computed from CPI index (PCPI_IX).
    Returns {YYYY-MM -> yoy_percent}.
    """
    for area in _norm_iso2_for_ifs(iso2):
        idx = _fetch_compact_series("IFS", f"M.{area}.PCPI_IX", start_period="2000")
        if idx:
            return _compute_yoy_from_level_monthly(idx)
    return {}

def imf_unemployment_rate_monthly(iso2: str) -> Dict[str, float]:
    """
    Unemployment rate, percent, monthly (LUR_PT).
    Returns {YYYY-MM -> percent}.
    """
    for area in _norm_iso2_for_ifs(iso2):
        ser = _fetch_compact_series("IFS", f"M.{area}.LUR_PT", start_period="2000")
        if ser:
            return ser
    return {}

def imf_fx_usd_monthly(iso2: str) -> Dict[str, float]:
    """
    Domestic currency per U.S. dollar, monthly.
    Prefer end-of-period (ENDE_XDC_USD_RATE), fallback to period average (ENDA_XDC_USD_RATE).
    Returns {YYYY-MM -> rate}.
    """
    for area in _norm_iso2_for_ifs(iso2):
        ser = _fetch_compact_series("IFS", f"M.{area}.ENDE_XDC_USD_RATE", start_period="2000")
        if ser:
            return ser
        ser = _fetch_compact_series("IFS", f"M.{area}.ENDA_XDC_USD_RATE", start_period="2000")
        if ser:
            return ser
    return {}

def imf_reserves_usd_monthly(iso2: str) -> Dict[str, float]:
    """
    Total reserves excluding gold, US dollars (RAXG_USD), monthly.
    Returns {YYYY-MM -> millions USD}.
    """
    for area in _norm_iso2_for_ifs(iso2):
        ser = _fetch_compact_series("IFS", f"M.{area}.RAXG_USD", start_period="2000")
        if ser:
            return ser
    return {}

def imf_policy_rate_monthly(iso2: str) -> Dict[str, float]:
    """
    Central bank policy rate, percent per annum, monthly (FPOLM_PA).
    Returns {YYYY-MM -> percent}.
    """
    for area in _norm_iso2_for_ifs(iso2):
        ser = _fetch_compact_series("IFS", f"M.{area}.FPOLM_PA", start_period="2000")
        if ser:
            return ser
    return {}

def imf_gdp_growth_quarterly(iso2: str) -> Dict[str, float]:
    """
    Real GDP YoY % (quarterly), computed from NGDP_R_SA_XDC (Real GDP, SA, national currency).
    Returns {YYYY-Qn -> yoy_percent}.
    """
    for area in _norm_iso2_for_ifs(iso2):
        lvl = _fetch_compact_series("IFS", f"Q.{area}.NGDP_R_SA_XDC", start_period="2000")
        if lvl:
            return _compute_yoy_from_level_quarterly(lvl)
    return {}

# ---------------------------
# IMF WEO: Debt-to-GDP (annual)
# ---------------------------
# WEO dataset code can be published as "WEO" (stable) or stamped releases like "WEO:2024-10".
# We try the stable alias first, then a short list of recent release tags as fallbacks.
_WEO_DATASETS_TRY: List[str] = [
    "WEO",
    "WEO:2025-04",  # Spring 2025 (approx.)
    "WEO:2024-10",  # Fall 2024
    "WEO:2024-04",  # Spring 2024
    "WEO:2023-10",
]

# Indicator: General Government Gross Debt, % of GDP
_WEO_DEBT_INDICATORS: List[str] = [
    "GGXWDG_NGDP",  # main target
]

def imf_weo_debt_to_gdp_annual(iso2: str) -> Dict[str, float]:
    """
    IMF WEO: General Government Gross Debt (% of GDP), annual.

    Attempts:
      - area = ISO3 (e.g., DEU, USA); WEO typically keys by ISO3 or WEO country codes.
      - dataset: try "WEO" first, then a few recent stamped releases.
      - indicator: GGXWDG_NGDP.

    Returns {YYYY -> percent} or {} if not available.
    """
    iso3 = _iso2_to_iso3(iso2)
    if not iso3:
        return {}

    # Key format for CompactData/WEO is typically: A.{AREA}.{INDICATOR}
    key_variants: List[str] = [f"A.{iso3}.{ind}" for ind in _WEO_DEBT_INDICATORS]

    for dataset in _WEO_DATASETS_TRY:
        for key in key_variants:
            ser = _fetch_compact_series(dataset, key, start_period="2000")
            if ser:
                # Keep only annual 'YYYY' labels
                return {k: v for k, v in ser.items() if isinstance(k, str) and len(k) == 4 and k.isdigit()}
    return {}
