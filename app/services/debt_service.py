# app/services/debt_service.py
from __future__ import annotations

from typing import Dict, Any, Optional, Tuple
import time
from collections import OrderedDict

# ISO resolution — support either function name to match your utils
try:
    from app.utils.country_codes import get_country_codes as _resolve_codes
except Exception:
    try:
        from app.utils.country_codes import resolve_country_codes as _resolve_codes  # type: ignore
    except Exception:
        _resolve_codes = None  # type: ignore

# Compat layer (preferred first)
try:
    from app.providers import compat as compat_prov  # type: ignore
except Exception:
    compat_prov = None  # type: ignore

# Defensive provider imports (as in your file)
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

# ------------------------ tiny TTL cache ------------------------
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

# ------------------------ canonical empty ------------------------
EMPTY = {
    "latest": {"year": None, "value": None, "source": "computed:NA/NA"},
    "series": {},
}

# ------------------------ normalization helpers ------------------------
def _normalize_year_value_map(d: Optional[Dict[Any, Any]]) -> Dict[str, float]:
    """
    Normalize {period: value} => {"YYYY": float}
    Accepts keys like 2024, "2024", "2024-01", "2024-Q3"; extracts a 4-digit year.
    Filters non-numerics. Returns sorted by year ascending.
    """
    if not isinstance(d, dict):
        return {}

    out: Dict[str, float] = {}
    for k, v in d.items():
        if v is None:
            continue
        try:
            val = float(v)
        except Exception:
            continue

        ks = str(k)
        year = None
        if len(ks) >= 4 and ks[:4].isdigit():
            year = ks[:4]
        else:
            for i in range(len(ks) - 3):
                seg = ks[i : i + 4]
                if seg.isdigit():
                    year = seg
                    break
        if not year:
            continue
        out[year] = val

    return dict(OrderedDict(sorted(out.items(), key=lambda kv: kv[0])))

def _latest_from_series(series: Dict[str, float]) -> Optional[Tuple[int, float]]:
    if not series:
        return None
    y = sorted(series.keys())[-1]
    try:
        return int(y), float(series[y])
    except Exception:
        return None

def _safe_call(fn, *args, **kwargs) -> Optional[Dict[str, float]]:
    try:
        data = fn(*args, **kwargs)
        if isinstance(data, dict) and data:
            return data
    except Exception:
        pass
    return None

# ----------------- provider-specific fetchers -----------------
def _eurostat_ratio_annual(iso2: str) -> Dict[str, float]:
    if euro is None:
        return {}
    for fn in (
        "eurostat_debt_to_gdp_ratio_annual",
        "eurostat_gov_debt_gdp_ratio_annual",
        "eurostat_gg_debt_gdp_ratio_annual",
    ):
        if hasattr(euro, fn):
            data = _safe_call(getattr(euro, fn), iso2)
            if data:
                return _normalize_year_value_map(data)
    return {}

def _imf_weo_ratio_annual(iso3: str) -> Dict[str, float]:
    if imf is None:
        return {}
    for fn in (
        "imf_weo_debt_to_gdp_ratio_annual",
        "imf_weo_gg_debt_gdp_ratio_annual",
        "imf_weo_debt_gdp_ratio_annual",
    ):
        if hasattr(imf, fn):
            data = _safe_call(getattr(imf, fn), iso3)
            if data:
                return _normalize_year_value_map(data)
    return {}

def _wb_series(iso3: str, code: str) -> Dict[str, float]:
    if fetch_wb_indicator_raw is None or wb_year_dict_from_raw is None:
        return {}
    try:
        raw = fetch_wb_indicator_raw(iso3, code)
        return _normalize_year_value_map(wb_year_dict_from_raw(raw))
    except Exception:
        return {}

def _wb_ratio_direct_annual(iso3: str) -> Dict[str, float]:
    # GC.DOD.TOTL.GD.ZS = Central government debt, total (% of GDP)
    return _wb_series(iso3, "GC.DOD.TOTL.GD.ZS")

def _wb_computed_ratio_annual(iso3: str) -> Dict[str, float]:
    # USD level series first
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
        return dict(OrderedDict(sorted(out.items())))

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
    return dict(OrderedDict(sorted(out2.items())))

# ------------------------ main API ------------------------
def compute_debt_payload(country: str) -> Dict[str, Any]:
    """
    Canonical /v1/debt payload builder.
    Order:
      1) compat.get_debt_to_gdp_annual(country)
      2) Eurostat (ISO2)
      3) IMF WEO (ISO3)
      4) World Bank ratio (% of GDP)
      5) Computed (WB levels, USD -> LCU fallback)

    Returns:
      {
        "latest": {"year": int|None, "value": float|None, "source": str},
        "series": {"YYYY": float, ...}
      }
    """
    cache_key = f"debt:{country}"
    cached = _cache.get(cache_key)
    if cached:
        return cached

    # 0) Resolve ISO codes
    codes = _resolve_codes(country) if callable(_resolve_codes) else None
    if not codes:
        payload = EMPTY.copy()
        _cache.set(cache_key, payload)
        return payload

    iso2 = (codes or {}).get("iso_alpha_2")
    iso3 = (codes or {}).get("iso_alpha_3")

    # 1) Compat first
    if compat_prov and hasattr(compat_prov, "get_debt_to_gdp_annual"):
        data = _safe_call(compat_prov.get_debt_to_gdp_annual, country)
        if data:
            series = _normalize_year_value_map(data)
            latest = _latest_from_series(series)
            if latest:
                y, v = latest
                payload = {"latest": {"year": y, "value": v, "source": "compat:get_debt_to_gdp_annual"}, "series": series}
                _cache.set(cache_key, payload)
                return payload

    # 2) Eurostat (ISO2)
    if iso2:
        s = _eurostat_ratio_annual(iso2)
        if s:
            latest = _latest_from_series(s)
            if latest:
                y, v = latest
                payload = {"latest": {"year": y, "value": v, "source": "Eurostat"}, "series": s}
                _cache.set(cache_key, payload)
                return payload

    # 3) IMF WEO (ISO3)
    if iso3:
        s = _imf_weo_ratio_annual(iso3)
        if s:
            latest = _latest_from_series(s)
            if latest:
                y, v = latest
                payload = {"latest": {"year": y, "value": v, "source": "IMF WEO"}, "series": s}
                _cache.set(cache_key, payload)
                return payload

    # 4) World Bank ratio
    if iso3:
        s = _wb_ratio_direct_annual(iso3)
        if s:
            latest = _latest_from_series(s)
            if latest:
                y, v = latest
                payload = {"latest": {"year": y, "value": v, "source": "World Bank (ratio)"}, "series": s}
                _cache.set(cache_key, payload)
                return payload

        # 5) WB computed ratio
        s = _wb_computed_ratio_annual(iso3)
        if s:
            latest = _latest_from_series(s)
            if latest:
                y, v = latest
                payload = {"latest": {"year": y, "value": v, "source": "Computed (WB levels)"}, "series": s}
                _cache.set(cache_key, payload)
                return payload

    payload = EMPTY.copy()
    _cache.set(cache_key, payload)
    return payload

# ------------------------ legacy aliases (compat) ------------------------
def get_debt_to_gdp(country: str) -> Dict[str, Any]:
    # Backward-compat alias to your previous symbol
    return compute_debt_payload(country)

def debt_payload_for_country(country: str) -> Dict[str, Any]:
    return compute_debt_payload(country)

def debt_payload_for_iso2(iso2: str) -> Dict[str, Any]:
    # Let resolver map iso2 to names consistently
    return compute_debt_payload(iso2)

def compute_debt_payload_iso2(iso2: str) -> Dict[str, Any]:
    return compute_debt_payload(iso2)
