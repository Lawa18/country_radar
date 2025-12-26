# app/providers/compat.py — provider bridge (robust fallbacks + shape normalization)
from __future__ import annotations

from typing import Any, Dict, Mapping, Sequence, Optional, Callable, List, Tuple

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
# normalization utilities
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
    """Flexible normalizer → {period: float} for common shapes."""
    if data is None:
        return {}

    # mapping of period->value
    if isinstance(data, Mapping):
        out: Dict[str, float] = {}
        for k, v in data.items():
            if isinstance(v, Mapping):
                # {period: {"value": ...}} or {period: {"OBS_VALUE": ...}}
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

    # list of (period, value) or list of dicts
    if isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
        out: Dict[str, float] = {}
        for row in data:
            if isinstance(row, (list, tuple)) and len(row) >= 2:
                p = str(row[0])
                fv = _coerce_float(row[1])
                if fv is not None:
                    out[p] = fv
            elif isinstance(row, Mapping):
                period = (
                    row.get("period")
                    or row.get("date")
                    or row.get("time")
                    or row.get("TIME_PERIOD")
                )
                if not period:
                    y = row.get("year") or row.get("y")
                    m = row.get("month") or row.get("m")
                    q = row.get("quarter") or row.get("q")
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
    """Keep last N points (best-effort order by key)."""
    if not series or keep <= 0:
        return series or {}
    keys = sorted(series.keys())
    if len(keys) <= keep:
        return {k: series[k] for k in keys}
    tail = keys[-keep:]
    return {k: series[k] for k in tail}


# -----------------------------------------------------------------------------
# robust calling: try multiple signatures
# -----------------------------------------------------------------------------

def _call_provider(fn: Callable[..., Any], *, country: str, iso2: Optional[str], iso3: Optional[str]) -> Any:
    """
    Try a few common call signatures:
      - fn(iso2=...)
      - fn(iso2)
      - fn(country=...)
      - fn(country)
      - fn(iso3=...) / fn(iso3) (rare)
    """
    # keyword iso2
    if iso2:
        try:
            return fn(iso2=iso2)
        except TypeError:
            pass
        except Exception:
            return None
        try:
            return fn(iso2)
        except Exception:
            return None

    # keyword country
    try:
        return fn(country=country)
    except TypeError:
        pass
    except Exception:
        return None
    try:
        return fn(country)
    except Exception:
        return None

    # keyword iso3 (rare)
    if iso3:
        try:
            return fn(iso3=iso3)
        except TypeError:
            pass
        except Exception:
            return None
        try:
            return fn(iso3)
        except Exception:
            return None

    return None


def _try_imf(mod: Any, fn_names: List[str], *, country: str, iso2: Optional[str], iso3: Optional[str]) -> Dict[str, float]:
    """
    Try IMF provider functions in order until one returns a non-empty series.
    """
    if not mod:
        return {}
    for name in fn_names:
        fn = getattr(mod, name, None)
        if not callable(fn):
            continue
        raw = _call_provider(fn, country=country, iso2=iso2, iso3=iso3)
        ser = _normalize_series(raw)
        if ser:
            return ser
    return {}


def _try_wb(mod: Any, fn_names: List[str], *, iso3: Optional[str]) -> Dict[str, float]:
    """
    Try WB helper functions in order until one returns a non-empty series.
    """
    if not mod or not iso3:
        return {}
    for name in fn_names:
        fn = getattr(mod, name, None)
        if not callable(fn):
            continue
        try:
            raw = fn(iso3=iso3)
        except TypeError:
            try:
                raw = fn(iso3)
            except Exception:
                continue
        except Exception:
            continue
        ser = _normalize_series(raw)
        if ser:
            return ser
    return {}


# -----------------------------------------------------------------------------
# Public API (these are what probe.py calls)
# -----------------------------------------------------------------------------

def get_cpi_yoy_monthly(country: str, keep: int = 36) -> Dict[str, float]:
    codes = _get_codes(country)
    imf = _safe_import("app.providers.imf_provider")

    # Try a few plausible IMF function names (DBnomics endpoints change)
    ser = _try_imf(
        imf,
        fn_names=[
            "imf_cpi_yoy_monthly",
            "imf_cpi_yoy_m",
            "imf_cpi_yoy",
            "imf_cpi_inflation_yoy_monthly",
        ],
        country=country,
        iso2=codes["iso2"],
        iso3=codes["iso3"],
    )
    if ser:
        return _trim_keep(ser, keep)

    # Fallback to WB annual CPI inflation (%)
    wb = _safe_import("app.providers.wb_provider")
    ser = _try_wb(wb, fn_names=["wb_cpi_yoy_annual"], iso3=codes["iso3"])
    return _trim_keep(ser, keep) if ser else {}


def get_unemployment_rate_monthly(country: str, keep: int = 36) -> Dict[str, float]:
    codes = _get_codes(country)
    imf = _safe_import("app.providers.imf_provider")

    ser = _try_imf(
        imf,
        fn_names=[
            "imf_unemployment_rate_monthly",
            "imf_unemployment_monthly",
            "imf_labor_unemployment_rate_monthly",
            "imf_lur_monthly",
        ],
        country=country,
        iso2=codes["iso2"],
        iso3=codes["iso3"],
    )
    if ser:
        return _trim_keep(ser, keep)

    wb = _safe_import("app.providers.wb_provider")
    ser = _try_wb(wb, fn_names=["wb_unemployment_rate_annual"], iso3=codes["iso3"])
    return _trim_keep(ser, keep) if ser else {}


def get_fx_rate_usd_monthly(country: str, keep: int = 36) -> Dict[str, float]:
    codes = _get_codes(country)
    imf = _safe_import("app.providers.imf_provider")

    ser = _try_imf(
        imf,
        fn_names=[
            "imf_fx_usd_monthly",
            "imf_fx_rate_usd_monthly",
            "imf_exchange_rate_usd_monthly",
        ],
        country=country,
        iso2=codes["iso2"],
        iso3=codes["iso3"],
    )
    if ser:
        return _trim_keep(ser, keep)

    wb = _safe_import("app.providers.wb_provider")
    ser = _try_wb(wb, fn_names=["wb_fx_rate_usd_annual"], iso3=codes["iso3"])
    return _trim_keep(ser, keep) if ser else {}


def get_reserves_usd_monthly(country: str, keep: int = 36) -> Dict[str, float]:
    codes = _get_codes(country)
    imf = _safe_import("app.providers.imf_provider")

    ser = _try_imf(
        imf,
        fn_names=[
            "imf_reserves_usd_monthly",
            "imf_fx_reserves_usd_monthly",
            "imf_reserves_monthly_usd",
            "imf_reserves_usd",
        ],
        country=country,
        iso2=codes["iso2"],
        iso3=codes["iso3"],
    )
    if ser:
        return _trim_keep(ser, keep)

    wb = _safe_import("app.providers.wb_provider")
    ser = _try_wb(wb, fn_names=["wb_reserves_usd_annual"], iso3=codes["iso3"])
    return _trim_keep(ser, keep) if ser else {}


def get_policy_rate_monthly(country: str, keep: int = 48) -> Dict[str, float]:
    codes = _get_codes(country)

    # ECB first (EU-only), then IMF
    ecb = _safe_import("app.providers.ecb_provider")
    ecbf = getattr(ecb, "ecb_policy_rate_for_country", None) if ecb else None
    if callable(ecbf) and codes["iso2"]:
        try:
            raw = ecbf(iso2=codes["iso2"])
        except TypeError:
            try:
                raw = ecbf(codes["iso2"])
            except Exception:
                raw = None
        except Exception:
            raw = None
        ser = _normalize_series(raw)
        if ser:
            return _trim_keep(ser, keep)

    imf = _safe_import("app.providers.imf_provider")
    ser = _try_imf(
        imf,
        fn_names=[
            "imf_policy_rate_monthly",
            "imf_policy_rate_m",
            "imf_fpolm_monthly",
        ],
        country=country,
        iso2=codes["iso2"],
        iso3=codes["iso3"],
    )
    return _trim_keep(ser, keep) if ser else {}


def get_gdp_growth_quarterly(country: str, keep: int = 12) -> Dict[str, float]:
    """
    WARNING: true q/q GDP growth is patchy.
    This wrapper tries IMF quarterly series; if missing, falls back to WB annual growth.
    """
    codes = _get_codes(country)
    imf = _safe_import("app.providers.imf_provider")

    ser = _try_imf(
        imf,
        fn_names=[
            "imf_gdp_growth_quarterly",
            "imf_gdp_qoq_quarterly",
            "imf_real_gdp_growth_quarterly",
            "imf_ngdp_r_growth_quarterly",
        ],
        country=country,
        iso2=codes["iso2"],
        iso3=codes["iso3"],
    )
    if ser:
        return _trim_keep(ser, keep)

    wb = _safe_import("app.providers.wb_provider")
    ser = _try_wb(wb, fn_names=["wb_gdp_growth_annual_pct"], iso3=codes["iso3"])
    return _trim_keep(ser, keep) if ser else {}


def get_debt_to_gdp_annual(country: str, keep: int = 20) -> Dict[str, float]:
    """
    Keep this for legacy callers; primary debt logic lives in debt_service.
    We try:
      1) IMF WEO (if provider has it)
      2) IMF debt_to_gdp
      3) WB gov debt % GDP (ratio/derived levels) via wb_provider helper if present
      4) debt_service.compute_debt_payload as last resort (normalized)
    """
    codes = _get_codes(country)
    imf = _safe_import("app.providers.imf_provider")

    ser = _try_imf(
        imf,
        fn_names=[
            "imf_weo_debt_to_gdp_annual",
            "imf_debt_to_gdp_annual",
        ],
        country=country,
        iso2=codes["iso2"],
        iso3=codes["iso3"],
    )
    if ser:
        return _trim_keep(ser, keep)

    wb = _safe_import("app.providers.wb_provider")
    ser = _try_wb(wb, fn_names=["wb_gov_debt_pct_gdp_annual"], iso3=codes["iso3"])
    if ser:
        return _trim_keep(ser, keep)

    # Last resort: call debt_service normalized output
    ds = _safe_import("app.services.debt_service")
    compute = getattr(ds, "compute_debt_payload", None) if ds else None
    if callable(compute):
        try:
            bundle = compute(country) or {}
            block = bundle.get("debt_to_gdp") or {}
            series = block.get("series") or bundle.get("debt_to_gdp_series") or {}
            ser2 = _normalize_series(series)
            return _trim_keep(ser2, keep) if ser2 else {}
        except Exception:
            return {}

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
