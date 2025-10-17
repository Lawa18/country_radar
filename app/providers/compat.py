# app/providers/compat.py — provider bridge that matches your deployed functions exactly
from __future__ import annotations
from typing import Any, Dict, Mapping, Sequence, Optional

def _safe_import(path: str):
    try:
        return __import__(path, fromlist=["*"])
    except Exception:
        return None

def _get_codes(country: str) -> Dict[str, Optional[str]]:
    iso2 = iso3 = numeric = None
    name = country
    try:
        cc = _safe_import("app.utils.country_codes")
        if cc and hasattr(cc, "get_country_codes"):
            row = cc.get_country_codes(country) or {}
            name = row.get("name") or country
            iso2 = row.get("iso_alpha_2") or row.get("alpha2") or row.get("iso2")
            iso3 = row.get("iso_alpha_3") or row.get("alpha3") or row.get("iso3")
            numeric = row.get("iso_numeric") or row.get("numeric")
    except Exception:
        pass
    return {"name": name, "iso2": iso2, "iso3": iso3, "numeric": numeric}

def _coerce_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if v != v:  # NaN
            return None
        return v
    except Exception:
        return None

def _normalize_series(data: Any) -> Dict[str, float]:
    """Flexible normalizer → {period: float} for common shapes."""
    if data is None:
        return {}
    # already a mapping of period->value (most of your providers)
    if isinstance(data, Mapping):
        out: Dict[str, float] = {}
        for k, v in data.items():
            if isinstance(v, Mapping):
                # handles {period: {"value": ...}} or {period: {"OBS_VALUE": ...}}
                for vk in ("value", "val", "v", "y", "OBS_VALUE", "obs_value"):
                    if vk in v:
                        fv = _coerce_float(v[vk])
                        if fv is not None:
                            out[str(k)] = fv
                            break
            else:
                fv = _coerce_float(v)
                if fv is not None:
                    out[str(k)] = fv
        return out
    # sequence of (period, value) or sequence of dicts
    if isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
        out: Dict[str, float] = {}
        for row in data:
            if isinstance(row, (list, tuple)) and len(row) >= 2:
                p = str(row[0]); fv = _coerce_float(row[1])
                if fv is not None:
                    out[p] = fv
            elif isinstance(row, Mapping):
                period = row.get("period") or row.get("date") or row.get("time") or row.get("TIME_PERIOD")
                if not period:
                    y = row.get("year") or row.get("y")
                    m = row.get("month") or row.get("m")
                    q = row.get("quarter") or row.get("q")
                    if y and q: period = f"{int(y)}-Q{int(q)}"
                    elif y and m: period = f"{int(y)}-{int(m):02d}"
                    elif y: period = str(y)
                if period:
                    for vk in ("value", "val", "v", "y", "OBS_VALUE", "obs_value"):
                        if vk in row:
                            fv = _coerce_float(row[vk])
                            if fv is not None:
                                out[str(period)] = fv
                                break
        return out
    return {}

# ------------------ Exact wrappers for the functions you have ------------------

def get_cpi_yoy_monthly(country: str) -> Dict[str, float]:
    codes = _get_codes(country)
    mod = _safe_import("app.providers.imf_provider")
    fn = getattr(mod, "imf_cpi_yoy_monthly", None) if mod else None
    if callable(fn) and codes["iso2"]:
        return _normalize_series(fn(iso2=codes["iso2"]))  # IMF expects iso2
    # WB annual fallback
    wb = _safe_import("app.providers.wb_provider")
    wbf = getattr(wb, "wb_cpi_yoy_annual", None) if wb else None
    if callable(wbf) and codes["iso3"]:
        return _normalize_series(wbf(iso3=codes["iso3"]))
    return {}

def get_unemployment_rate_monthly(country: str) -> Dict[str, float]:
    codes = _get_codes(country)
    mod = _safe_import("app.providers.imf_provider")
    fn = getattr(mod, "imf_unemployment_rate_monthly", None) if mod else None
    if callable(fn) and codes["iso2"]:
        return _normalize_series(fn(iso2=codes["iso2"]))
    wb = _safe_import("app.providers.wb_provider")
    wbf = getattr(wb, "wb_unemployment_rate_annual", None) if wb else None
    if callable(wbf) and codes["iso3"]:
        return _normalize_series(wbf(iso3=codes["iso3"]))
    return {}

def get_fx_rate_usd_monthly(country: str) -> Dict[str, float]:
    codes = _get_codes(country)
    mod = _safe_import("app.providers.imf_provider")
    fn = getattr(mod, "imf_fx_usd_monthly", None) if mod else None
    if callable(fn) and codes["iso2"]:
        return _normalize_series(fn(iso2=codes["iso2"]))
    wb = _safe_import("app.providers.wb_provider")
    wbf = getattr(wb, "wb_fx_rate_usd_annual", None) if wb else None
    if callable(wbf) and codes["iso3"]:
        return _normalize_series(wbf(iso3=codes["iso3"]))
    return {}

def get_reserves_usd_monthly(country: str) -> Dict[str, float]:
    codes = _get_codes(country)
    mod = _safe_import("app.providers.imf_provider")
    fn = getattr(mod, "imf_reserves_usd_monthly", None) if mod else None
    if callable(fn) and codes["iso2"]:
        return _normalize_series(fn(iso2=codes["iso2"]))
    wb = _safe_import("app.providers.wb_provider")
    wbf = getattr(wb, "wb_reserves_usd_annual", None) if wb else None
    if callable(wbf) and codes["iso3"]:
        return _normalize_series(wbf(iso3=codes["iso3"]))
    return {}

def get_policy_rate_monthly(country: str) -> Dict[str, float]:
    codes = _get_codes(country)
    # Prefer ECB if available (EU countries), else IMF
    ecb = _safe_import("app.providers.ecb_provider")
    ecbf = getattr(ecb, "ecb_policy_rate_for_country", None) if ecb else None
    if callable(ecbf) and codes["iso2"]:
        res = _normalize_series(ecbf(iso2=codes["iso2"]))
        if res:
            return res
    imf = _safe_import("app.providers.imf_provider")
    imff = getattr(imf, "imf_policy_rate_monthly", None) if imf else None
    if callable(imff) and codes["iso2"]:
        return _normalize_series(imff(iso2=codes["iso2"]))
    return {}

def get_gdp_growth_quarterly(country: str) -> Dict[str, float]:
    codes = _get_codes(country)
    mod = _safe_import("app.providers.imf_provider")
    fn = getattr(mod, "imf_gdp_growth_quarterly", None) if mod else None
    if callable(fn) and codes["iso2"]:
        return _normalize_series(fn(iso2=codes["iso2"]))
    # WB annual growth fallback if needed
    wb = _safe_import("app.providers.wb_provider")
    wbf = getattr(wb, "wb_gdp_growth_annual_pct", None) if wb else None
    if callable(wbf) and codes["iso3"]:
        return _normalize_series(wbf(iso3=codes["iso3"]))
    return {}

def get_debt_to_gdp_annual(country: str) -> Dict[str, float]:
    codes = _get_codes(country)
    # Prefer IMF WEO, then IMF, then WB
    imf = _safe_import("app.providers.imf_provider")
    weo = getattr(imf, "imf_weo_debt_to_gdp_annual", None) if imf else None
    if callable(weo) and codes["iso2"]:
        res = _normalize_series(weo(iso2=codes["iso2"]))
        if res:
            return res
    imf2 = getattr(imf, "imf_debt_to_gdp_annual", None) if imf else None
    if callable(imf2) and codes["iso2"]:
        res = _normalize_series(imf2(iso2=codes["iso2"]))
        if res:
            return res
    wb = _safe_import("app.providers.wb_provider")
    wbf = getattr(wb, "wb_year_dict_from_raw", None) if wb else None
    # or direct convenience if present
    wbd = getattr(wb, "wb_fx_rate_usd_annual", None) if wb else None
    # standard WB debt code is keyed in wb_provider via fetch_worldbank_data; if you have a wrapper expose it here
    if hasattr(wb, "wb_year_dict_from_raw"):
        # If you later add a wb_debt_to_gdp_annual(iso3) wrapper, just call and normalize it.
        pass
    return {}
