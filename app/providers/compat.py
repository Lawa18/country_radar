# app/providers/compat.py — provider bridge (matches deployed IMF provider functions)
from __future__ import annotations

from typing import Any, Dict, Mapping, Sequence, Optional, Callable

# -----------------------------------------------------------------------------
# safe import + country codes
# -----------------------------------------------------------------------------

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


# -----------------------------------------------------------------------------
# normalization + trimming
# -----------------------------------------------------------------------------

def _coerce_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if v != v:  # NaN
            return None
        return v
    except Exception:
        return None


def _normalize_series(data: Any) -> Dict[str, float]:
    """Normalize common shapes → {period: float}."""
    if data is None:
        return {}

    if isinstance(data, Mapping):
        out: Dict[str, float] = {}
        for k, v in data.items():
            if isinstance(v, Mapping):
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

    if isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
        out: Dict[str, float] = {}
        for row in data:
            if isinstance(row, (list, tuple)) and len(row) >= 2:
                p = str(row[0])
                fv = _coerce_float(row[1])
                if fv is not None:
                    out[p] = fv
            elif isinstance(row, Mapping):
                period = row.get("period") or row.get("date") or row.get("time") or row.get("TIME_PERIOD")
                if not period:
                    y = row.get("year")
                    m = row.get("month")
                    q = row.get("quarter")
                    if y and q:
                        period = f"{int(y)}-Q{int(q)}"
                    elif y and m:
                        period = f"{int(y)}-{int(m):02d}"
                    elif y:
                        period = str(y)

                if period:
                    for vk in ("value", "val", "v", "y", "OBS_VALUE", "obs_value"):
                        if vk in row:
                            fv = _coerce_float(row[vk])
                            if fv is not None:
                                out[str(period)] = fv
                                break
        return out

    return {}


def _trim_keep(series: Dict[str, float], keep: int) -> Dict[str, float]:
    if not series or keep <= 0:
        return series or {}
    keys = sorted(series.keys())
    if len(keys) <= keep:
        return {k: series[k] for k in keys}
    tail = keys[-keep:]
    return {k: series[k] for k in tail}


def _call_iso2(fn: Callable[..., Any], iso2: str) -> Any:
    """Try iso2= kw then positional."""
    try:
        return fn(iso2=iso2)
    except TypeError:
        try:
            return fn(iso2)
        except Exception:
            return None
    except Exception:
        return None


def _call_iso3(fn: Callable[..., Any], iso3: str) -> Any:
    """Try iso3= kw then positional."""
    try:
        return fn(iso3=iso3)
    except TypeError:
        try:
            return fn(iso3)
        except Exception:
            return None
    except Exception:
        return None


# -----------------------------------------------------------------------------
# Public functions used by probe.py
# -----------------------------------------------------------------------------

def get_cpi_yoy_monthly(country: str, keep: int = 36) -> Dict[str, float]:
    codes = _get_codes(country)
    iso2, iso3 = codes.get("iso2"), codes.get("iso3")

    # IMF monthly CPI YoY (or computed YoY from index inside provider)
    imf = _safe_import("app.providers.imf_provider")
    if imf and iso2:
        fn = getattr(imf, "imf_cpi_yoy_monthly", None)
        if callable(fn):
            ser = _normalize_series(_call_iso2(fn, iso2))
            if ser:
                return _trim_keep(ser, keep)

    # Fallback WB annual inflation (%)
    wb = _safe_import("app.providers.wb_provider")
    if wb and iso3:
        wbf = getattr(wb, "wb_cpi_yoy_annual", None)
        if callable(wbf):
            ser = _normalize_series(_call_iso3(wbf, iso3))
            if ser:
                return _trim_keep(ser, keep)

    return {}


def get_unemployment_rate_monthly(country: str, keep: int = 36) -> Dict[str, float]:
    codes = _get_codes(country)
    iso2, iso3 = codes.get("iso2"), codes.get("iso3")

    imf = _safe_import("app.providers.imf_provider")
    if imf and iso2:
        fn = getattr(imf, "imf_unemployment_rate_monthly", None)
        if callable(fn):
            ser = _normalize_series(_call_iso2(fn, iso2))
            if ser:
                return _trim_keep(ser, keep)

    wb = _safe_import("app.providers.wb_provider")
    if wb and iso3:
        wbf = getattr(wb, "wb_unemployment_rate_annual", None)
        if callable(wbf):
            ser = _normalize_series(_call_iso3(wbf, iso3))
            if ser:
                return _trim_keep(ser, keep)

    return {}


def get_fx_rate_usd_monthly(country: str, keep: int = 36) -> Dict[str, float]:
    codes = _get_codes(country)
    iso2, iso3 = codes.get("iso2"), codes.get("iso3")

    imf = _safe_import("app.providers.imf_provider")
    if imf and iso2:
        fn = getattr(imf, "imf_fx_usd_monthly", None)
        if callable(fn):
            ser = _normalize_series(_call_iso2(fn, iso2))
            if ser:
                return _trim_keep(ser, keep)

    wb = _safe_import("app.providers.wb_provider")
    if wb and iso3:
        wbf = getattr(wb, "wb_fx_rate_usd_annual", None)
        if callable(wbf):
            ser = _normalize_series(_call_iso3(wbf, iso3))
            if ser:
                return _trim_keep(ser, keep)

    return {}


def get_reserves_usd_monthly(country: str, keep: int = 36) -> Dict[str, float]:
    codes = _get_codes(country)
    iso2, iso3 = codes.get("iso2"), codes.get("iso3")

    imf = _safe_import("app.providers.imf_provider")
    if imf and iso2:
        fn = getattr(imf, "imf_reserves_usd_monthly", None)
        if callable(fn):
            ser = _normalize_series(_call_iso2(fn, iso2))
            if ser:
                return _trim_keep(ser, keep)

    wb = _safe_import("app.providers.wb_provider")
    if wb and iso3:
        wbf = getattr(wb, "wb_reserves_usd_annual", None)
        if callable(wbf):
            ser = _normalize_series(_call_iso3(wbf, iso3))
            if ser:
                return _trim_keep(ser, keep)

    return {}


def get_policy_rate_monthly(country: str, keep: int = 48) -> Dict[str, float]:
    codes = _get_codes(country)
    iso2 = codes.get("iso2")

    # ECB override (EU only)
    ecb = _safe_import("app.providers.ecb_provider")
    if ecb and iso2:
        ecbf = getattr(ecb, "ecb_policy_rate_for_country", None)
        if callable(ecbf):
            ser = _normalize_series(_call_iso2(ecbf, iso2))
            if ser:
                return _trim_keep(ser, keep)

    # IMF policy rate monthly
    imf = _safe_import("app.providers.imf_provider")
    if imf and iso2:
        fn = getattr(imf, "imf_policy_rate_monthly", None)
        if callable(fn):
            ser = _normalize_series(_call_iso2(fn, iso2))
            if ser:
                return _trim_keep(ser, keep)

    return {}


def get_gdp_growth_quarterly(country: str, keep: int = 12) -> Dict[str, float]:
    """
    IMF provider returns *YoY quarterly* (computed from levels).
    We'll still call it "gdp_growth_quarterly" for the route's schema.
    """
    codes = _get_codes(country)
    iso2, iso3 = codes.get("iso2"), codes.get("iso3")

    imf = _safe_import("app.providers.imf_provider")
    if imf and iso2:
        fn = getattr(imf, "imf_gdp_growth_quarterly", None)
        if callable(fn):
            ser = _normalize_series(_call_iso2(fn, iso2))
            if ser:
                return _trim_keep(ser, keep)

    # WB annual growth fallback if IMF quarterly missing
    wb = _safe_import("app.providers.wb_provider")
    if wb and iso3:
        wbf = getattr(wb, "wb_gdp_growth_annual_pct", None)
        if callable(wbf):
            ser = _normalize_series(_call_iso3(wbf, iso3))
            if ser:
                return _trim_keep(ser, keep)

    return {}


def get_debt_to_gdp_annual(country: str, keep: int = 20) -> Dict[str, float]:
    """
    Prefer IMF WEO debt-to-GDP (annual). Fallback WB debt ratio helper if present.
    """
    codes = _get_codes(country)
    iso2, iso3 = codes.get("iso2"), codes.get("iso3")

    imf = _safe_import("app.providers.imf_provider")
    if imf and iso2:
        fn = getattr(imf, "imf_weo_debt_to_gdp_annual", None) or getattr(imf, "imf_debt_to_gdp_annual", None)
        if callable(fn):
            ser = _normalize_series(_call_iso2(fn, iso2))
            if ser:
                return _trim_keep(ser, keep)

    wb = _safe_import("app.providers.wb_provider")
    if wb and iso3:
        wbf = getattr(wb, "wb_gov_debt_pct_gdp_annual", None)
        if callable(wbf):
            ser = _normalize_series(_call_iso3(wbf, iso3))
            if ser:
                return _trim_keep(ser, keep)

    return {}


__all__ = [
    "get_cpi_yoy_monthly",
    "get_unemployment_rate_monthly",
    "get_fx_rate_usd_monthly",
    "get_reserves_usd_monthly",
    "get_policy_rate_monthly",
    "get_gdp_growth_quarterly",
    "get_debt_to_gdp_annual",
]
