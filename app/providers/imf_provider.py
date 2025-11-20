# app/providers/imf_provider.py
from __future__ import annotations

"""
IMF provider for Country Radar (DB.Nomics + IFS/CPI only).

We deliberately avoid:
  - sdmxcentral.imf.org (v2/v3) – many endpoints are now dead / unstable
  - IMF CompactData REST – not needed for our current working stack
  - WEO-based debt (GGXWDG_NGDP) – this path is too brittle; debt is handled
    via World Bank in wb_provider instead.

Public functions (all using DB.Nomics + IMF datasets):

- imf_cpi_yoy_monthly(iso2)              -> { "YYYY-MM": float }   (CPI YoY from index)
- imf_unemployment_rate_monthly(iso2)    -> { "YYYY-MM": float }   (IFS LUR_PT)
- imf_fx_usd_monthly(iso2)               -> { "YYYY-MM": float }   (IFS ENDE_XDC_USD_RATE)
- imf_reserves_usd_monthly(iso2)         -> { "YYYY-MM": float }   (IFS RAXG_USD)
- imf_policy_rate_monthly(iso2)          -> { "YYYY-MM": float }   (IFS FPOLM_PA)
- imf_gdp_growth_quarterly(iso2)         -> { "YYYY-Qn": float }   (YoY from IFS NGDP_R_SA_XDC)

Legacy / compatibility exports:
- imf_weo_debt_to_gdp_annual(iso2)       -> {}  (stub; use WB instead)
- imf_debt_to_gdp_annual(iso2)           -> alias of the stub above
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
IMF_DEBUG = os.getenv("IMF_DEBUG", "0") == "1"

# Hosts
_DBNOMICS_BASE = "https://api.db.nomics.world/v22"

# HTTP & cache
_DEFAULT_TIMEOUT = 6.0
_MAX_RETRIES = 1
_CACHE_TTL = 3600  # 1 hour


# ----------------------------
# Tiny in-memory TTL cache
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
            with httpx.Client(
                timeout=timeout,
                follow_redirects=True,
                headers=_HEADERS,
                http2=True,
            ) as client:
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
    Normalize country codes for IMF/IFS quirks.
    """
    iso2 = (iso2 or "").upper()
    tries = [iso2]
    if iso2 == "UK":
        tries.append("GB")
    if iso2 == "EL":  # Eurostat alias for Greece
        tries.append("GR")
    # remove duplicates but keep order
    return list(dict.fromkeys(tries))


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
        # YYYY-MM
        return int(k[:4]), int(k[5:7])
    if len(k) == 6 and (k[4] in ("M", "m")):
        # YYYYMn
        return int(k[:4]), int(k[5:6])
    try:
        y = int(k[:4])
        m = int(k[-2:])
        return y, m if 1 <= m <= 12 else 0
    except Exception:
        return 0, 0


def _yyqq_key_to_tuple(k: str) -> Tuple[int, int]:
    try:
        y = int(k[:4])
        q = int(k[-1])
        return y, q if 1 <= q <= 4 else 0
    except Exception:
        return 0, 0


def _compute_yoy_from_level_monthly(level_series: Dict[str, float]) -> Dict[str, float]:
    """
    Compute YoY % from a monthly level series (needs at least 12 months).
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
    Compute YoY % from a quarterly level series (needs at least 4 quarters).
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


# ----------------------------
# DBnomics parsing
# ----------------------------
def _normalize_period_key(p: Any) -> Optional[str]:
    if p is None:
        return None
    s = str(p).strip()
    if not s:
        return None

    # Daily → monthly
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        # YYYY-MM-DD -> YYYY-MM
        return f"{s[:4]}-{s[5:7]}"

    # Monthly YYYY-MM
    if len(s) == 7 and s[4] == "-":
        return s

    # Monthly YYYYMn → YYYY-MM
    if len(s) == 7 and (s[4] in ("M", "m")):
        yy = s[:4]
        mm = s[5:]
        if mm.isdigit() and len(mm) == 2:
            return f"{yy}-{mm}"

    # Quarterly YYYY-Qn
    if len(s) == 7 and s[4] == "-" and (s[5] in ("Q", "q")):
        return f"{s[:4]}-Q{s[-1]}"

    if len(s) == 6 and (s[4] in ("Q", "q")) and s[-1].isdigit():
        # YYYYQn → YYYY-Qn
        return f"{s[:4]}-Q{s[-1]}"

    # Annual
    if len(s) == 4 and s.isdigit():
        return s

    # Fallback: return as-is
    return s


def _parse_dbnomics_series(payload: Dict[str, Any]) -> Dict[str, float]:
    """
    Parse a DB.Nomics IMF series payload into {period -> value}.
    Handles both "period/value" and "observations" layouts.
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

    # Typical DB.Nomics format: period[] + value[]
    periods = doc.get("period")
    values = doc.get("value")
    if isinstance(periods, list) and isinstance(values, list) and len(periods) == len(values):
        for p, v in zip(periods, values):
            key = _normalize_period_key(p)
            fv = _safe_float(v)
            if key and fv is not None:
                out[key] = fv

    # Fallback: observations list (some variants)
    if not out and isinstance(doc.get("observations"), list):
        for obs in doc["observations"]:
            if not isinstance(obs, dict):
                continue
            p = obs.get("period") or obs.get("original_period")
            v = obs.get("value")
            key = _normalize_period_key(p)
            fv = _safe_float(v)
            if key and fv is not None:
                out[key] = fv

    # Fallback: original_period + value
    if not out:
        o_periods = doc.get("original_period")
        o_values = doc.get("value")
        if isinstance(o_periods, list) and isinstance(o_values, list) and len(o_periods) == len(o_values):
            for p, v in zip(o_periods, o_values):
                key = _normalize_period_key(p)
                fv = _safe_float(v)
                if key and fv is not None:
                    out[key] = fv

    return out


def _fetch_db_series(dataset: str, key: str) -> Dict[str, float]:
    """
    DB.Nomics direct fetch (full series) for IMF datasets.

    dataset: "IFS", "CPI", etc.
    key:     e.g. "M.MX.ENDE_XDC_USD_RATE", "Q.MX.NGDP_R_SA_XDC"
    """
    if IMF_DISABLE:
        return {}

    cache_key = f"DB::IMF::{dataset}::{key}"
    hit = _cache.get(cache_key)
    if hit is not None:
        return hit

    url = f"{_DBNOMICS_BASE}/series/IMF/{dataset}/{key}?observations=1&format=json"
    data = _http_get_json(url)
    ser = _parse_dbnomics_series(data or {})

    if IMF_DEBUG:
        print(f"[dbn] IMF/{dataset}/{key} -> {'HIT ' + str(len(ser)) if ser else 'EMPTY'}")

    _cache.set(cache_key, ser)
    return ser


# ----------------------------
# Public provider functions
# ----------------------------
def imf_cpi_yoy_monthly(iso2: str) -> Dict[str, float]:
    """
    CPI YoY % (monthly), computed from CPI index (PCPI_IX).

    Logic:
      - use IMF/CPI, series: M.<iso2>.PCPI_IX via DB.Nomics
      - compute YoY from level (12-month difference)
      - fall back to IFS with the same key if needed
    """
    if IMF_DISABLE:
        return {}

    for area in _norm_iso2_for_ifs(iso2):
        # Try CPI dataset first
        idx = _fetch_db_series("CPI", f"M.{area}.PCPI_IX")
        if idx:
            return _compute_yoy_from_level_monthly(idx)

        # Fallback: IFS variant if present
        idx_ifs = _fetch_db_series("IFS", f"M.{area}.PCPI_IX")
        if idx_ifs:
            return _compute_yoy_from_level_monthly(idx_ifs)

    return {}


def imf_unemployment_rate_monthly(iso2: str) -> Dict[str, float]:
    """
    Unemployment rate (%), monthly.

    Uses IMF/IFS, indicator:
      - LUR_PT (percent)
    Key: M.<iso2>.LUR_PT
    """
    if IMF_DISABLE:
        return {}

    for area in _norm_iso2_for_ifs(iso2):
        ser = _fetch_db_series("IFS", f"M.{area}.LUR_PT")
        if ser:
            return ser

    return {}


def imf_fx_usd_monthly(iso2: str) -> Dict[str, float]:
    """
    LCU per USD, monthly.

    Prefer:
      - ENDE_XDC_USD_RATE (end-of-period)
    Fallback:
      - ENDA_XDC_USD_RATE (period average)
    Dataset: IMF/IFS
    """
    if IMF_DISABLE:
        return {}

    for area in _norm_iso2_for_ifs(iso2):
        ser = _fetch_db_series("IFS", f"M.{area}.ENDE_XDC_USD_RATE")
        if ser:
            return ser
        ser = _fetch_db_series("IFS", f"M.{area}.ENDA_XDC_USD_RATE")
        if ser:
            return ser

    return {}


def imf_reserves_usd_monthly(iso2: str) -> Dict[str, float]:
    """
    Total reserves excl. gold, USD (RAXG_USD), monthly.

    Dataset: IMF/IFS
    Key: M.<iso2>.RAXG_USD
    """
    if IMF_DISABLE:
        return {}

    for area in _norm_iso2_for_ifs(iso2):
        ser = _fetch_db_series("IFS", f"M.{area}.RAXG_USD")
        if ser:
            return ser

    return {}


def imf_policy_rate_monthly(iso2: str) -> Dict[str, float]:
    """
    Policy rate, % p.a. (FPOLM_PA), monthly.

    Dataset: IMF/IFS
    Key: M.<iso2>.FPOLM_PA

    Note: For euro area, your ECB provider should override downstream.
    """
    if IMF_DISABLE:
        return {}

    for area in _norm_iso2_for_ifs(iso2):
        ser = _fetch_db_series("IFS", f"M.{area}.FPOLM_PA")
        if ser:
            return ser

    return {}


def imf_gdp_growth_quarterly(iso2: str) -> Dict[str, float]:
    """
    Real GDP YoY % (quarterly), computed from level:

    Dataset: IMF/IFS
    Prefer:
      - Q.<iso2>.NGDP_R_SA_XDC (seasonally adjusted)
    Fallback:
      - Q.<iso2>.NGDP_R_XDC

    We compute YoY (t vs t-4) from the level series.
    """
    if IMF_DISABLE:
        return {}

    for area in _norm_iso2_for_ifs(iso2):
        for code in (f"Q.{area}.NGDP_R_SA_XDC", f"Q.{area}.NGDP_R_XDC"):
            lvl = _fetch_db_series("IFS", code)
            if lvl:
                return _compute_yoy_from_level_quarterly(lvl)

    return {}


# ----------------------------
# Legacy WEO debt stubs
# ----------------------------
def imf_weo_debt_to_gdp_annual(iso2: str) -> Dict[str, float]:
    """
    Legacy stub for IMF WEO General Government Gross Debt (% of GDP), annual.

    We no longer attempt to pull this from IMF WEO because:
      - the WEO endpoints via DB.Nomics and SDMXCentral are highly unstable
      - World Bank provides GC.DOD.TOTL.GD.ZS (central govt debt % GDP) which is
        now used in wb_provider for Country Radar.

    Returning {} is intentional; Country Radar's country-lite builder should use
    WB-based debt metrics instead.
    """
    _ = iso2  # unused
    return {}


# Back-compat alias
imf_debt_to_gdp_annual = imf_weo_debt_to_gdp_annual


# Explicit export list
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
