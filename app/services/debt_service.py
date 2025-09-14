# app/services/debt_service.py
from __future__ import annotations

from typing import Dict, Any, Optional, Tuple
import math, time

from app.utils.country_codes import resolve_country_codes

# Defensive provider imports
try:
    from app.providers import eurostat_provider as euro
except Exception:
    euro = None  # type: ignore

try:
    from app.providers import imf_provider as imf
except Exception:
    imf = None  # type: ignore

try:
    from app.providers.wb_provider import fetch_wb_indicator_raw, wb_year_dict_from_raw
except Exception:
    fetch_wb_indicator_raw = None  # type: ignore
    wb_year_dict_from_raw = None   # type: ignore

# ------------------------ tiny TTL cache (compat) ------------------------
class _TTLCache:
    def __init__(self, ttl_sec: int = 900):
        self.ttl = ttl_sec
        self.data: Dict[str, Tuple[float, Dict[str, Any]]] = {}

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        row = self.data.get(key)
        if not row:
            return None
        ts, payload = row
        if time.time() - ts > self.ttl:
            try:
                del self.data[key]
            except Exception:
                pass
            return None
        return payload

    def set(self, key: str, payload: Dict[str, Any]) -> None:
        self.data[key] = (time.time(), payload)

_cache = _TTLCache(ttl_sec=900)

# ------------------------ core helpers ------------------------
def _latest(dict_yyyy_val: Dict[str, float]) -> Optional[Tuple[str, float]]:
    if not dict_yyyy_val:
        return None
    try:
        year = max(dict_yyyy_val.keys())
        return year, float(dict_yyyy_val[year])
    except Exception:
        return None

def _clean_series(data: Optional[Dict[str, float]]) -> Dict[str, float]:
    if not data:
        return {}
    out: Dict[str, float] = {}
    for k, v in data.items():
        try:
            out[str(k)] = float(v)
        except Exception:
            continue
    return dict(sorted(out.items()))  # chronological

# ----------------- provider-specific fetchers -----------------
def _eurostat_ratio_annual(iso2: str) -> Dict[str, float]:
    if euro is None:
        return {}
    for fn in [
        "eurostat_debt_to_gdp_ratio_annual",
        "eurostat_gov_debt_gdp_ratio_annual",
        "eurostat_gg_debt_gdp_ratio_annual",
    ]:
        if hasattr(euro, fn):
            try:
                return _clean_series(getattr(euro, fn)(iso2))
            except Exception:
                continue
    return {}

def _imf_weo_ratio_annual(iso3: str) -> Dict[str, float]:
    if imf is None:
        return {}
    for fn in [
        "imf_weo_debt_to_gdp_ratio_annual",
        "imf_weo_gg_debt_gdp_ratio_annual",
        "imf_weo_debt_gdp_ratio_annual",
    ]:
        if hasattr(imf, fn):
            try:
                return _clean_series(getattr(imf, fn)(iso3))
            except Exception:
                continue
    return {}

def _wb_series(iso3: str, code: str) -> Dict[str, float]:
    if fetch_wb_indicator_raw is None or wb_year_dict_from_raw is None:
        return {}
    try:
        raw = fetch_wb_indicator_raw(iso3, code)
        return _clean_series(wb_year_dict_from_raw(raw))
    except Exception:
        return {}

def _wb_ratio_direct_annual(iso3: str) -> Dict[str, float]:
    return _wb_series(iso3, "GC.DOD.TOTL.GD.ZS")

def _wb_computed_ratio_annual(iso3: str) -> Dict[str, float]:
    # USD first
    debt_usd = _wb_series(iso3, "GC.DOD.TOTL.CD")
    gdp_usd  = _wb_series(iso3, "NY.GDP.MKTP.CD")
    years = set(debt_usd.keys()) & set(gdp_usd.keys())
    if years:
        out: Dict[str, float] = {}
        for y in years:
            try:
                if gdp_usd[y] != 0:
                    out[y] = (debt_usd[y] / gdp_usd[y]) * 100.0
            except Exception:
                continue
        return dict(sorted(out.items()))
    # LCU fallback
    debt_lcu = _wb_series(iso3, "GC.DOD.TOTL.CN")
    gdp_lcu  = _wb_series(iso3, "NY.GDP.MKTP.CN")
    years = set(debt_lcu.keys()) & set(gdp_lcu.keys())
    if not years:
        return {}
    out2: Dict[str, float] = {}
    for y in years:
        try:
            if gdp_lcu[y] != 0:
                out2[y] = (debt_lcu[y] / gdp_lcu[y]) * 100.0
        except Exception:
            continue
    return dict(sorted(out2.items()))

# ------------------------ main API ------------------------
def get_debt_to_gdp(country: str) -> Dict[str, Any]:
    """
    Tiered: Eurostat -> IMF WEO -> World Bank ratio -> Computed (WB levels).
    Returns:
    {
      "latest": {"year": "2023", "value": 61.2, "source": "Eurostat"} | None,
      "series": {"2019": 59.4, "2020": 73.1, ...},
      "source": "Eurostat" | "IMF WEO" | "World Bank (ratio)" | "Computed (WB levels)" | "unavailable"
    }
    """
    cache_key = f"debt:{country}"
    cached = _cache.get(cache_key)
    if cached:
        return cached

    codes = resolve_country_codes(country)
    if not codes:
        payload = {"latest": None, "series": {}, "source": "invalid_country"}
        _cache.set(cache_key, payload)
        return payload

    iso2, iso3 = codes["iso_alpha_2"], codes["iso_alpha_3"]

    # 1) Eurostat
    s = _eurostat_ratio_annual(iso2)
    if s:
        latest = _latest(s)
        payload = {
            "latest": {"year": latest[0], "value": latest[1], "source": "Eurostat"} if latest else None,
            "series": s,
            "source": "Eurostat",
        }
        _cache.set(cache_key, payload)
        return payload

    # 2) IMF WEO
    s = _imf_weo_ratio_annual(iso3)
    if s:
        latest = _latest(s)
        payload = {
            "latest": {"year": latest[0], "value": latest[1], "source": "IMF WEO"} if latest else None,
            "series": s,
            "source": "IMF WEO",
        }
        _cache.set(cache_key, payload)
        return payload

    # 3) World Bank ratio
    s = _wb_ratio_direct_annual(iso3)
    if s:
        latest = _latest(s)
        payload = {
            "latest": {"year": latest[0], "value": latest[1], "source": "World Bank (ratio)"} if latest else None,
            "series": s,
            "source": "World Bank (ratio)",
        }
        _cache.set(cache_key, payload)
        return payload

    # 4) Computed from WB levels
    s = _wb_computed_ratio_annual(iso3)
    if s:
        latest = _latest(s)
        payload = {
            "latest": {"year": latest[0], "value": latest[1], "source": "Computed (WB levels)"} if latest else None,
            "series": s,
            "source": "Computed (WB levels)",
        }
        _cache.set(cache_key, payload)
        return payload

    payload = {"latest": None, "series": {}, "source": "unavailable"}
    _cache.set(cache_key, payload)
    return payload

# ------------------------ legacy aliases (compat) ------------------------
def debt_payload_for_country(country: str) -> Dict[str, Any]:
    return get_debt_to_gdp(country)

def debt_payload_for_iso2(iso2: str) -> Dict[str, Any]:
    # Let resolver map iso2 to names consistently
    return get_debt_to_gdp(iso2)

def compute_debt_payload(country: str) -> Dict[str, Any]:
    return get_debt_to_gdp(country)

def compute_debt_payload_iso2(iso2: str) -> Dict[str, Any]:
    return get_debt_to_gdp(iso2)
