# app/providers/imf_provider.py
from __future__ import annotations

"""
Robust IMF provider for Country Radar.

Design goals
------------
- Prefer DBnomics mirror first (fast & reliable), then IMF CompactData, then SDMX Central.
- Dataset-aware lookups (CPI, LP, IFS, WEO).
- Compute YoY when a direct YoY series is missing (e.g., CPI: PCPI_IX -> YoY).
- Small in-memory cache (TTL ~1 hour) to smooth Render cold starts.
- Minimal external deps (httpx, pycountry).

Public functions used by the service
------------------------------------
- imf_cpi_yoy_monthly(iso2)              -> { "YYYY-MM": float }
- imf_unemployment_rate_monthly(iso2)    -> { "YYYY-MM": float }
- imf_fx_usd_monthly(iso2)               -> { "YYYY-MM": float }
- imf_reserves_usd_monthly(iso2)         -> { "YYYY-MM": float }
- imf_policy_rate_monthly(iso2)          -> { "YYYY-MM": float }
- imf_gdp_growth_quarterly(iso2)         -> { "YYYY-Qn": float }
- imf_weo_debt_to_gdp_annual(iso2)       -> { "YYYY": float }
- imf_debt_to_gdp_annual(iso2)           -> alias of the above (back-compat)
"""

from typing import Dict, List, Tuple, Optional, Any
import time
import math
import os
import httpx

# ----------------------------
# Config
# ----------------------------
IMF_DISABLE = os.getenv("IMF_DISABLE", "0") == "1"
IMF_TRY_SDMXCENTRAL = os.getenv("IMF_TRY_SDMXCENTRAL", "0") == "1"
IMF_DEBUG = os.getenv("IMF_DEBUG", "0") == "1"

# Hosts
_IMF_COMPACT_BASE     = "https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData"
_SDMXCENTRAL_V21_BASE = "https://sdmxcentral.imf.org/ws/public/sdmxapi/rest"
_SDMXCENTRAL_V3_BASE  = "https://sdmxcentral.imf.org/sdmx/v2"
_DBNOMICS_BASE        = "https://api.db.nomics.world/v22"

# HTTP behavior
_DEFAULT_TIMEOUT = 6.0  # seconds
_MAX_RETRIES = 1

# Cache
_CACHE_TTL = 3600  # seconds == 1 hour


# ----------------------------
# Small in-memory TTL cache
# ----------------------------
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


# ----------------------------
# HTTP helpers
# ----------------------------
_HEADERS = {
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate, br",
    "User-Agent": "CountryRadar/1.0 (imf_provider)",
}

def _http_get_json(url: str, timeout: float = _DEFAULT_TIMEOUT) -> Optional[Dict[str, Any]]:
    for attempt in range(_MAX_RETRIES + 1):
        try:
            with httpx.Client(timeout=timeout, follow_redirects=True, headers=_HEADERS, http2=True) as client:
                resp = client.get(url)
                if IMF_DEBUG:
                    print(f"[http] GET {url} -> {resp.status_code} (len={len(resp.content)})")
                if resp.status_code == 200:
                    return resp.json()
        except Exception as e:
            if IMF_DEBUG:
                print(f"[http] GET {url} raised {type(e).__name__}: {e}")
            time.sleep(0.2 * (attempt + 1))
    return None


# ----------------------------
# Utilities & parsing
# ----------------------------
def _norm_iso2_for_ifs(iso2: str) -> List[str]:
    """
    Return candidate ISO2 area codes to try for IMF datasets.
    Handles common aliases: UK->GB, EL->GR.
    """
    iso2 = (iso2 or "").upper()
    tries = [iso2]
    if iso2 == "UK":
        tries.append("GB")
    if iso2 == "EL":
        tries.append("GR")
    # dedupe while preserving order
    return list(dict.fromkeys(tries))

def _iso2_to_iso3(iso2: str) -> Optional[str]:
    try:
        import pycountry
        code = (iso2 or "").upper()
        if code == "UK":
            code = "GB"
        if code == "EL":
            code = "GR"
        c = pycountry.countries.get(alpha_2=code)
        return c.alpha_3 if c else None
    except Exception:
        return None

def _safe_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return None

def _yymm_key_to_tuple(k: str) -> Tuple[int, int]:
    k = (k or "").strip()
    if len(k) == 7 and k[4] == "-":
        return int(k[:4]), int(k[5:7])
    if len(k) == 6 and (k[4] in ("M", "m")):
        return int(k[:4]), int(k[5:6])
    try:
        y = int(k[:4]); m = int(k[-2:])
        return y, m if 1 <= m <= 12 else 0
    except Exception:
        return 0, 0

def _yyqq_key_to_tuple(k: str) -> Tuple[int, int]:
    try:
        y = int(k[:4]); q = int(k[-1])
        return y, q if 1 <= q <= 4 else 0
    except Exception:
        return 0, 0

def _compute_yoy_from_level_monthly(level_series: Dict[str, float]) -> Dict[str, float]:
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


# ----------------------------
# DBnomics parsing
# ----------------------------
def _normalize_period_key(p: Any) -> Optional[str]:
    """
    Normalize DBnomics period strings:
    - 'YYYY-MM-DD' -> 'YYYY-MM'
    - 'YYYYMmm'    -> 'YYYY-MM'
    - 'YYYY-Qn' or 'YYYYQn' -> 'YYYY-Qn'
    - 'YYYY' (annual) left as-is
    """
    if p is None:
        return None
    s = str(p).strip()
    if not s:
        return None
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return f"{s[:4]}-{s[5:7]}"
    if len(s) == 7 and s[4] == "-":
        return s
    if len(s) == 7 and (s[4] in ("M", "m")):
        yy = s[:4]; mm = s[5:]
        if mm.isdigit() and len(mm) == 2:
            return f"{yy}-{mm}"
    if len(s) == 7 and s[4] == "-" and (s[5] in ("Q", "q")):
        return f"{s[:4]}-Q{s[-1]}"
    if len(s) == 6 and (s[4] in ("Q", "q")) and s[-1].isdigit():
        return f"{s[:4]}-Q{s[-1]}"
    if len(s) == 4 and s.isdigit():
        return s
    return s

def _parse_dbnomics_series(payload: Dict[str, Any]) -> Dict[str, float]:
    """
    Robust parser for DBnomics v22 `/series` responses:
      - series.docs[0].period + .value (parallel arrays), or
      - series.docs[0].observations = [{period, value}, ...], or
      - series.docs[0].original_period + .value
    """
    if not isinstance(payload, dict):
        return {}

    series = payload.get("series")
    if not series:
        return {}

    doc = None
    if isinstance(series, dict):
        docs = series.get("docs")
        if isinstance(docs, list) and docs:
            doc = docs[0]
    elif isinstance(series, list) and series:
        doc = series[0]

    if not isinstance(doc, dict):
        return {}

    out: Dict[str, float] = {}

    periods = doc.get("period")
    values = doc.get("value")
    if isinstance(periods, list) and isinstance(values, list) and len(periods) == len(values):
        for p, v in zip(periods, values):
            key = _normalize_period_key(p); fv = _safe_float(v)
            if key and fv is not None:
                out[key] = fv

    if not out and isinstance(doc.get("observations"), list):
        for obs in doc["observations"]:
            if not isinstance(obs, dict):
                continue
            p = obs.get("period") or obs.get("original_period")
            v = obs.get("value")
            key = _normalize_period_key(p); fv = _safe_float(v)
            if key and fv is not None:
                out[key] = fv

    if not out:
        o_periods = doc.get("original_period")
        o_values  = doc.get("value")
        if isinstance(o_periods, list) and isinstance(o_values, list) and len(o_periods) == len(o_values):
            for p, v in zip(o_periods, o_values):
                key = _normalize_period_key(p); fv = _safe_float(v)
                if key and fv is not None:
                    out[key] = fv

    return out

def _fetch_db_series(dataset: str, key: str) -> Dict[str, float]:
    url = f"{_DBNOMICS_BASE}/series/IMF/{dataset}/{key}?observations=1&format=json"
    data = _http_get_json(url)
    ser = _parse_dbnomics_series(data or {})
    if IMF_DEBUG:
        print(f"[dbn] {dataset}/{key} -> {'HIT ' + str(len(ser)) if ser else 'EMPTY'}")
    return ser


# ----------------------------
# IMF CompactData parsing
# ----------------------------
def _parse_imf_compact(payload: Dict[str, Any]) -> Dict[str, float]:
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
        fv = _safe_float(v)
        if t is not None and fv is not None:
            out[str(t)] = fv
    return out


# ----------------------------
# General IMF fetcher (DB → IMF → SDMX)
# ----------------------------
def _fetch_imf_series(dataset: str, key: str, start_period: str = "2000") -> Dict[str, float]:
    """
    General-purpose fetcher for a dataset+key.
    Order: DBnomics → IMF CompactData → SDMX Central (v2.1 then v3).
    """
    if IMF_DISABLE:
        return {}

    cache_key = f"IMF::{dataset}::{key}::{start_period}"
    hit = _cache.get(cache_key)
    if hit is not None:
        return hit

    # 1) DBnomics first (best availability)
    ser3 = _fetch_db_series(dataset, key)
    if ser3:
        _cache.set(cache_key, ser3)
        if IMF_DEBUG: print(f"[imf] {dataset}/{key} -> DBnomics ({len(ser3)} pts)")
        return ser3

    # 2) IMF CompactData (primary)
    url1 = f"{_IMF_COMPACT_BASE}/{dataset}/{key}?startPeriod={start_period}"
    data1 = _http_get_json(url1)
    ser1 = _parse_imf_compact(data1 or {})
    if ser1:
        _cache.set(cache_key, ser1)
        if IMF_DEBUG: print(f"[imf] {dataset}/{key} -> IMF primary ({len(ser1)} pts)")
        return ser1

    # 3) SDMX Central (optional)
    if IMF_TRY_SDMXCENTRAL:
        url2a = f"{_SDMXCENTRAL_V21_BASE}/{dataset}/{key}?startPeriod={start_period}&format=sdmx-json"
        data2a = _http_get_json(url2a)
        ser2a = _parse_imf_compact(data2a or {})
        if ser2a:
            _cache.set(cache_key, ser2a)
            if IMF_DEBUG: print(f"[imf] {dataset}/{key} -> SDMX v2.1 ({len(ser2a)} pts)")
            return ser2a

        url2b = f"{_SDMXCENTRAL_V3_BASE}/data/{dataset}/{key}?startPeriod={start_period}&format=sdmx-json"
        data2b = _http_get_json(url2b)
        ser2b = _parse_imf_compact(data2b or {})
        if ser2b:
            _cache.set(cache_key, ser2b)
            if IMF_DEBUG: print(f"[imf] {dataset}/{key} -> SDMX v3 ({len(ser2b)} pts)")
            return ser2b

    if IMF_DEBUG: print(f"[imf] {dataset}/{key} -> EMPTY")
    return {}


# ----------------------------
# WEO fetcher (annual; DB → IMF → SDMX)
# ----------------------------
def _fetch_weo_series(key: str, start_period: str = "2000") -> Dict[str, float]:
    """
    key pattern: "A.{ISO3}.{INDICATOR}"
    e.g., "A.NGA.GGXWDG_NGDP"
    """
    if IMF_DISABLE:
        return {}

    cache_key = f"IMF::WEO::{key}::{start_period}"
    hit = _cache.get(cache_key)
    if hit is not None:
        return hit

    # 1) DBnomics first (latest snapshot is usually exposed)
    for ds in ["WEO:latest", "WEO", "WEO:2025-04", "WEO:2024-10", "WEO:2024-04", "WEO:2023-10"]:
        ser3 = _fetch_db_series(ds, key)
        if ser3:
            _cache.set(cache_key, ser3)
            if IMF_DEBUG: print(f"[weo] {ds}/{key} -> DBnomics ({len(ser3)} pts)")
            return ser3

    # 2) IMF CompactData
    for ds in ["WEO", "WEO:2025-04", "WEO:2024-10", "WEO:2024-04", "WEO:2023-10"]:
        url = f"{_IMF_COMPACT_BASE}/{ds}/{key}?startPeriod={start_period}"
        data = _http_get_json(url)
        ser = _parse_imf_compact(data or {})
        if ser:
            _cache.set(cache_key, ser)
            if IMF_DEBUG: print(f"[weo] {ds}/{key} -> IMF primary ({len(ser)} pts)")
            return ser

    # 3) SDMX Central
    if IMF_TRY_SDMXCENTRAL:
        for ds in ["WEO", "WEO:2025-04", "WEO:2024-10", "WEO:2024-04", "WEO:2023-10"]:
            url2 = f"{_SDMXCENTRAL_V21_BASE}/{ds}/{key}?startPeriod={start_period}&format=sdmx-json"
            data2 = _http_get_json(url2)
            ser2 = _parse_imf_compact(data2 or {})
            if ser2:
                _cache.set(cache_key, ser2)
                if IMF_DEBUG: print(f"[weo] {ds}/{key} -> SDMX v2.1 ({len(ser2)} pts)")
                return ser2

    if IMF_DEBUG: print(f"[weo] {key} -> EMPTY")
    return {}


# ----------------------------
# Public provider functions
# ----------------------------
def imf_cpi_yoy_monthly(iso2: str) -> Dict[str, float]:
    """
    CPI YoY % (monthly).
    Prefer IMF CPI dataset:
      1) Direct YoY: PCPI_YY
      2) Compute YoY from index: PCPI_IX
    Fallback to IFS if CPI is missing.
    DBnomics-first is used inside the fetchers.
    """
    if IMF_DISABLE:
        return {}
    for area in _norm_iso2_for_ifs(iso2):
        # Try direct YoY first, CPI then IFS
        for ds in ("CPI", "IFS"):
            yoy = _fetch_imf_series(ds, f"M.{area}.PCPI_YY", start_period="2000")
            if yoy:
                return yoy
        # Compute YoY from index, CPI then IFS
        for ds in ("CPI", "IFS"):
            idx = _fetch_imf_series(ds, f"M.{area}.PCPI_IX", start_period="2000")
            if idx:
                return _compute_yoy_from_level_monthly(idx)
    return {}

def imf_unemployment_rate_monthly(iso2: str) -> Dict[str, float]:
    """
    Unemployment rate (%), monthly.
    Prefer IMF Labor dataset (LP), then fallback to IFS.
    Codes:
      - LUR_PT : Percent (preferred)
      - LUR    : Level/index (rare; if present we still return raw values)
    """
    if IMF_DISABLE:
        return {}
    for area in _norm_iso2_for_ifs(iso2):
        for ds in ("LP", "IFS"):
            for ind in ("LUR_PT", "LUR"):
                ser = _fetch_imf_series(ds, f"M.{area}.{ind}", start_period="2000")
                if ser:
                    return ser
    return {}

def imf_fx_usd_monthly(iso2: str) -> Dict[str, float]:
    """
    LCU per USD, monthly.
    Prefer end-of-period rate (ENDE_XDC_USD_RATE), fallback to period average (ENDA_XDC_USD_RATE).
    """
    if IMF_DISABLE:
        return {}
    for area in _norm_iso2_for_ifs(iso2):
        ser = _fetch_imf_series("IFS", f"M.{area}.ENDE_XDC_USD_RATE", start_period="2000")
        if ser:
            return ser
        ser = _fetch_imf_series("IFS", f"M.{area}.ENDA_XDC_USD_RATE", start_period="2000")
        if ser:
            return ser
    return {}

def imf_reserves_usd_monthly(iso2: str) -> Dict[str, float]:
    """
    Total reserves excl. gold, USD (RAXG_USD), monthly.
    """
    if IMF_DISABLE:
        return {}
    for area in _norm_iso2_for_ifs(iso2):
        ser = _fetch_imf_series("IFS", f"M.{area}.RAXG_USD", start_period="2000")
        if ser:
            return ser
    return {}

def imf_policy_rate_monthly(iso2: str) -> Dict[str, float]:
    """
    Policy rate, % p.a. (FPOLM_PA), monthly.
    Note: For euro area, your ECB provider should override this elsewhere.
    """
    if IMF_DISABLE:
        return {}
    for area in _norm_iso2_for_ifs(iso2):
        ser = _fetch_imf_series("IFS", f"M.{area}.FPOLM_PA", start_period="2000")
        if ser:
            return ser
    return {}

def imf_gdp_growth_quarterly(iso2: str) -> Dict[str, float]:
    """
    Real GDP YoY % (quarterly). We compute YoY from levels:
      - Prefer SA series: NGDP_R_SA_XDC
      - Fallback to non-SA: NGDP_R_XDC
    """
    if IMF_DISABLE:
        return {}
    for area in _norm_iso2_for_ifs(iso2):
        for code in (f"Q.{area}.NGDP_R_SA_XDC", f"Q.{area}.NGDP_R_XDC"):
            lvl = _fetch_imf_series("IFS", code, start_period="2000")
            if lvl:
                return _compute_yoy_from_level_quarterly(lvl)
    return {}

_WEO_DEBT_INDICATORS: List[str] = ["GGXWDG_NGDP"]  # General Gov. Gross Debt (% of GDP)

def imf_weo_debt_to_gdp_annual(iso2: str) -> Dict[str, float]:
    """
    IMF WEO: General government gross debt (% of GDP), annual.
    """
    if IMF_DISABLE:
        return {}
    iso3 = _iso2_to_iso3(iso2)
    if not iso3:
        return {}
    for ind in _WEO_DEBT_INDICATORS:
        key = f"A.{iso3}.{ind}"
        ser = _fetch_weo_series(key, start_period="2000")
        if ser:
            # keep only annual keys
            return {k: v for k, v in ser.items() if isinstance(k, str) and len(k) == 4 and k.isdigit()}
    return {}

# Back-compat alias
imf_debt_to_gdp_annual = imf_weo_debt_to_gdp_annual


# Explicit export list (helps avoid drift and lets you sanity-check imports)
__all__ = [
    "imf_cpi_yoy_monthly",
    "imf_unemployment_rate_monthly",
    "imf_fx_usd_monthly",
    "imf_reserves_usd_monthly",
    "imf_policy_rate_monthly",
    "imf_gdp_growth_quarterly",
    "imf_weo_debt_to_gdp_annual",
    "imf_debt_to_gdp_annual",
]
