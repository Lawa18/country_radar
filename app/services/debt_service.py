# app/services/debt_service.py
from __future__ import annotations

from typing import Dict, Any, Optional, Tuple, List
import time
import math

import httpx

# ---------------------------------------------------------------------
# TTL cache (per-process) to keep /v1/debt and /country-data snappy
# ---------------------------------------------------------------------
class _TTLCache:
    def __init__(self, ttl_seconds: int = 3600) -> None:
        self.ttl = ttl_seconds
        self._store: Dict[str, Tuple[float, Any]] = {}

    def get(self, key: str) -> Optional[Any]:
        hit = self._store.get(key)
        if not hit:
            return None
        exp, val = hit
        if exp < time.time():
            self._store.pop(key, None)
            return None
        return val

    def set(self, key: str, val: Any) -> None:
        self._store[key] = (time.time() + self.ttl, val)

_cache = _TTLCache(ttl_seconds=3600)

# ---------------------------------------------------------------------
# Helpers: latest picker and safe float
# ---------------------------------------------------------------------
def _safe_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if math.isfinite(v):
            return v
        return None
    except Exception:
        return None

def _latest_from_series(series: Dict[str, Any]) -> Optional[Tuple[str, float]]:
    if not isinstance(series, dict) or not series:
        return None
    items: List[Tuple[int, float]] = []
    for k, v in series.items():
        if isinstance(k, str) and len(k) == 4 and k.isdigit():
            fv = _safe_float(v)
            if fv is not None:
                items.append((int(k), fv))
    if not items:
        return None
    items.sort(key=lambda kv: kv[0])  # ascending year
    y, val = items[-1]
    return str(y), val

# ---------------------------------------------------------------------
# Country code handling
# ---------------------------------------------------------------------
def _normalize_iso2_for_partners(iso2: str) -> str:
    """
    Normalizes Eurostat quirks back to ISO: EL->GR, UK->GB.
    """
    code = (iso2 or "").strip().upper()
    if code == "EL":
        return "GR"
    if code == "UK":
        return "GB"
    return code

def _iso2_to_iso3(iso2: str) -> Optional[str]:
    try:
        import pycountry  # in requirements
        code = _normalize_iso2_for_partners(iso2)
        c = pycountry.countries.get(alpha_2=code)
        return c.alpha_3 if c else None
    except Exception:
        return None

# ---------------------------------------------------------------------
# Providers (defensive imports)
# ---------------------------------------------------------------------
# Eurostat – annual general-gov debt %GDP
try:
    from app.providers.eurostat_provider import eurostat_debt_to_gdp_annual  # type: ignore
except Exception:  # pragma: no cover
    def eurostat_debt_to_gdp_annual(iso2: str) -> Dict[str, float]:  # type: ignore
        return {}

# IMF WEO – annual general-gov debt %GDP (if you expose one; optional)
# If not available, this step will be skipped gracefully.
try:
    from app.providers.imf_provider import imf_weo_debt_to_gdp_annual  # type: ignore
except Exception:  # pragma: no cover
    def imf_weo_debt_to_gdp_annual(iso2: str) -> Dict[str, float]:  # type: ignore
        return {}

# If your project already has World Bank helpers for debt ratio/levels,
# you can import them here and they will override the generic HTTP fetchers below.
try:
    from app.providers.wb_provider import (  # type: ignore
        wb_central_gov_debt_pct_gdp_annual,   # -> Dict[year,str/float]
        wb_central_gov_debt_lcu_annual,       # -> Dict[year,str/float]
        wb_gdp_nominal_lcu_annual,            # -> Dict[year,str/float]
        wb_central_gov_debt_usd_annual,       # optional
        wb_gdp_nominal_usd_annual,            # optional
    )
    _HAS_WB_HELPERS = True
except Exception:  # pragma: no cover
    _HAS_WB_HELPERS = False
    def wb_central_gov_debt_pct_gdp_annual(iso3: str) -> Dict[str, float]:  # type: ignore
        return {}
    def wb_central_gov_debt_lcu_annual(iso3: str) -> Dict[str, float]:  # type: ignore
        return {}
    def wb_gdp_nominal_lcu_annual(iso3: str) -> Dict[str, float]:  # type: ignore
        return {}
    def wb_central_gov_debt_usd_annual(iso3: str) -> Dict[str, float]:  # type: ignore
        return {}
    def wb_gdp_nominal_usd_annual(iso3: str) -> Dict[str, float]:  # type: ignore
        return {}

# ---------------------------------------------------------------------
# World Bank generic fetchers (only used if you haven't provided wb_provider helpers)
#   WDI indicators used:
#     - GC.DOD.TOTL.GD.ZS : Central government debt, total (% of GDP)
#     - GC.DOD.TOTL.CN    : Central government debt, total (current LCU)
#     - GC.DOD.TOTL.CD    : Central government debt, total (current US$)
#     - NY.GDP.MKTP.CN    : GDP (current LCU)
#     - NY.GDP.MKTP.CD    : GDP (current US$)
# ---------------------------------------------------------------------
_WB_BASE = "https://api.worldbank.org/v2/country"  # needs ISO3
_WB_TIMEOUT = 8.0
_WB_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "country-radar/1.0 (+debt_service)",
}

def _wb_fetch_indicator(iso3: str, indicator: str, per_page: int = 20000) -> Dict[str, float]:
    """
    Minimal World Bank WDI fetcher to {year -> value}.
    """
    if not iso3 or len(iso3) != 3:
        return {}
    cache_key = f"WB::{iso3}::{indicator}"
    hit = _cache.get(cache_key)
    if hit is not None:
        return hit

    url = f"{_WB_BASE}/{iso3}/indicator/{indicator}"
    params = {"format": "json", "per_page": str(per_page)}
    try:
        with httpx.Client(timeout=_WB_TIMEOUT, follow_redirects=True, headers=_WB_HEADERS) as client:
            resp = client.get(url, params=params)
            if resp.status_code != 200:
                return {}
            data = resp.json()
    except Exception:
        return {}

    # Response shape: [meta, [ {date: "2023", value: 12.3}, ... ]]
    if not isinstance(data, list) or len(data) < 2 or not isinstance(data[1], list):
        return {}
    out: Dict[str, float] = {}
    for row in data[1]:
        try:
            year = str(row.get("date"))
            val = row.get("value")
            fv = _safe_float(val)
            if year and fv is not None:
                out[year] = fv
        except Exception:
            continue
    _cache.set(cache_key, out)
    return out

def _wb_ratio_pct_gdp(iso3: str) -> Dict[str, float]:
    """
    World Bank central government debt, % of GDP (GC.DOD.TOTL.GD.ZS).
    """
    if _HAS_WB_HELPERS:
        return wb_central_gov_debt_pct_gdp_annual(iso3)
    return _wb_fetch_indicator(iso3, "GC.DOD.TOTL.GD.ZS")

def _wb_debt_lcu(iso3: str) -> Dict[str, float]:
    if _HAS_WB_HELPERS:
        return wb_central_gov_debt_lcu_annual(iso3)
    return _wb_fetch_indicator(iso3, "GC.DOD.TOTL.CN")

def _wb_gdp_lcu(iso3: str) -> Dict[str, float]:
    if _HAS_WB_HELPERS:
        return wb_gdp_nominal_lcu_annual(iso3)
    return _wb_fetch_indicator(iso3, "NY.GDP.MKTP.CN")

def _wb_debt_usd(iso3: str) -> Dict[str, float]:
    if _HAS_WB_HELPERS:
        return wb_central_gov_debt_usd_annual(iso3)
    return _wb_fetch_indicator(iso3, "GC.DOD.TOTL.CD")

def _wb_gdp_usd(iso3: str) -> Dict[str, float]:
    if _HAS_WB_HELPERS:
        return wb_gdp_nominal_usd_annual(iso3)
    return _wb_fetch_indicator(iso3, "NY.GDP.MKTP.CD")

def _compute_ratio_from_levels(debt_levels: Dict[str, float], gdp_levels: Dict[str, float]) -> Dict[str, float]:
    """
    Compute % of GDP from matching-year level series.
    """
    if not debt_levels or not gdp_levels:
        return {}
    out: Dict[str, float] = {}
    common_years = set(k for k in debt_levels.keys() if isinstance(k, str) and k.isdigit() and len(k) == 4) & \
                   set(k for k in gdp_levels.keys()  if isinstance(k, str) and k.isdigit() and len(k) == 4)
    if not common_years:
        return {}
    for y in common_years:
        d = _safe_float(debt_levels.get(y))
        g = _safe_float(gdp_levels.get(y))
        if d is None or g is None or g == 0:
            continue
        out[y] = (d / g) * 100.0
    # Return years as strings; caller picks latest
    return dict(sorted(out.items(), key=lambda kv: int(kv[0])))  # ascending by year

# ---------------------------------------------------------------------
# Core: build the debt payload for a given ISO2
# ---------------------------------------------------------------------
def _build_payload(source: Optional[str],
                   latest_period: Optional[str],
                   latest_value: Optional[float],
                   series_map: Dict[str, Dict[str, float]]) -> Dict[str, Any]:
    """
    Standardized block, plus a backward-compatible 'latest' object.
    """
    if source and latest_period and latest_value is not None:
        return {
            "latest_value": latest_value,
            "latest_period": latest_period,
            "source": source,
            "series": series_map,
            "latest": {"period": latest_period, "value": latest_value, "source": source},
        }
    return {
        "latest_value": None,
        "latest_period": None,
        "source": "N/A",
        "series": series_map,
        "latest": {"period": None, "value": None, "source": "N/A"},
    }

def debt_payload_for_iso2(iso2: str) -> Dict[str, Any]:
    """
    Tiered selection:
      1) Eurostat (general gov, %GDP, annual)
      2) IMF WEO (general gov, %GDP, annual) — optional if provider is present
      3) World Bank WDI (central gov, %GDP, annual)
      4) Computed (WB levels: central gov debt ÷ GDP) — LCU preferred, else USD

    Returns a block with:
      - latest_value (float or None)
      - latest_period (YYYY or None)
      - source ("Eurostat" | "IMF WEO" | "WorldBank" | "Computed (WB)" | "N/A")
      - series: { "Eurostat": {...}, "IMF_WEO": {...}, "WorldBank": {...}, "Computed_WB": {...} }
      - latest: { period, value, source }  # for backward compatibility
    """
    iso2 = (iso2 or "").strip().upper()
    cache_key = f"debt::{iso2}"
    hit = _cache.get(cache_key)
    if hit is not None:
        return hit

    # Collect series by source for transparency
    series_map: Dict[str, Dict[str, float]] = {}

    # 1) Eurostat
    try:
        eu_ser = eurostat_debt_to_gdp_annual(iso2) or {}
    except Exception:
        eu_ser = {}
    if eu_ser:
        series_map["Eurostat"] = eu_ser
        latest = _latest_from_series(eu_ser)
        if latest:
            y, v = latest
            payload = _build_payload("Eurostat", y, v, series_map)
            _cache.set(cache_key, payload)
            return payload

    # 2) IMF WEO (optional; only if a provider exists)
    try:
        weo_ser = imf_weo_debt_to_gdp_annual(iso2) or {}
    except Exception:
        weo_ser = {}
    if weo_ser:
        series_map["IMF_WEO"] = weo_ser
        latest = _latest_from_series(weo_ser)
        if latest:
            y, v = latest
            payload = _build_payload("IMF WEO", y, v, series_map)
            _cache.set(cache_key, payload)
            return payload

    # ISO3 for World Bank
    iso3 = _iso2_to_iso3(iso2)

    # 3) World Bank – direct ratio (% of GDP)
    wb_ratio = {}
    if iso3:
        try:
            wb_ratio = _wb_ratio_pct_gdp(iso3) or {}
        except Exception:
            wb_ratio = {}
    if wb_ratio:
        series_map["WorldBank"] = wb_ratio
        latest = _latest_from_series(wb_ratio)
        if latest:
            y, v = latest
            payload = _build_payload("WorldBank", y, v, series_map)
            _cache.set(cache_key, payload)
            return payload

    # 4) Computed (WB levels): prefer LCU consistency, else USD
    computed = {}
    if iso3:
        try:
            debt_lcu = _wb_debt_lcu(iso3)
            gdp_lcu  = _wb_gdp_lcu(iso3)
            computed = _compute_ratio_from_levels(debt_lcu, gdp_lcu)
            if not computed:
                # Fallback to USD levels if LCU not available
                debt_usd = _wb_debt_usd(iso3)
                gdp_usd  = _wb_gdp_usd(iso3)
                computed = _compute_ratio_from_levels(debt_usd, gdp_usd)
        except Exception:
            computed = {}

    if computed:
        series_map["Computed_WB"] = computed
        latest = _latest_from_series(computed)
        if latest:
            y, v = latest
            payload = _build_payload("Computed (WB)", y, v, series_map)
            _cache.set(cache_key, payload)
            return payload

    # Nothing available
    payload = _build_payload(None, None, None, series_map)
    _cache.set(cache_key, payload)
    return payload

# Convenience (if you want to call with country name in a route elsewhere)
def debt_payload_for_country(country: str) -> Dict[str, Any]:
    """
    Resolve ISO2 from a country name (or alias) and return the same payload.
    """
    try:
        from app.utils.country_codes import resolve_country_codes  # type: ignore
        codes = resolve_country_codes(country)
        iso2 = (codes or {}).get("iso_alpha_2")
        if not iso2:
            return _build_payload(None, None, None, {})
        return debt_payload_for_iso2(iso2)
    except Exception:
        return _build_payload(None, None, None, {})
