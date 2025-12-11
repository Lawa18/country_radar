# app/services/indicator_service.py — v2 builder + matrix indicators
from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, Literal
import math
from datetime import date

from app.services.indicator_matrix import INDICATOR_MATRIX

try:
    from app.services.debt_service import compute_debt_payload as _compute_debt_payload
except Exception:  # keep the module import non-fatal
    _compute_debt_payload = None


# -----------------------------------------------------------------------------
# utils: imports & coercion
# -----------------------------------------------------------------------------

def _safe_import(path: str):
    try:
        module = __import__(path, fromlist=["*"])
        return module
    except Exception:
        return None


def _safe_get_attr(mod: Any, name: str):
    try:
        return getattr(mod, name)
    except Exception:
        return None


def _to_float_map(d: Any) -> Dict[str, float]:
    """Coerce a provider result into a {period -> float} mapping, best-effort."""
    if not d:
        return {}
    if isinstance(d, Mapping):
        out: Dict[str, float] = {}
        for k, v in d.items():
            try:
                if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
                    continue
                out[str(k)] = float(v)
            except Exception:
                continue
        return out
    if isinstance(d, Iterable) and not isinstance(d, (str, bytes)):
        out: Dict[str, float] = {}
        for i, v in enumerate(d):
            try:
                if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
                    continue
                out[str(i)] = float(v)
            except Exception:
                continue
        return out
    # Single scalar → pretend it's a one-point series with a dummy key
    try:
        v = float(d)
    except Exception:
        return {}
    if math.isnan(v) or math.isinf(v):
        return {}
    return {"0": v}


def _parse_period_key(p: Any) -> Tuple[int, int, int]:
    """
    Normalise period keys for sorting.

    Supports:
    - "YYYY"
    - "YYYY-MM"
    - "YYYY-Qn"
    - or integer years.
    """
    try:
        if isinstance(p, (int, float)):
            return (int(p), 0, 0)
        s = str(p)
        if "-Q" in s:
            y, q = s.split("-Q", 1)
            return (int(y), 0, int(q))
        if "-" in s:
            y, m = s.split("-", 1)
            return (int(y), int(m), 0)
        return (int(s), 0, 0)
    except Exception:
        return (0, 0, 0)


def _latest(d: Mapping[str, float]) -> Tuple[str, float]:
    if not d:
        raise ValueError("empty series")
    ks = sorted(d.keys(), key=_parse_period_key)
    k = ks[-1]
    return k, d[k]


def _trim_by_keep(series: Dict[str, float], keep: int) -> Dict[str, float]:
    if keep <= 0 or not series:
        return series
    keys = sorted(series.keys(), key=_parse_period_key)
    if len(keys) <= keep:
        return series
    return {k: series[k] for k in keys[-keep:]}


def _apply_series_mode(series: Dict[str, float], mode: Literal["none", "mini", "full"], keep: int) -> Dict[str, float]:
    """Apply mini/full/none mode and keep trimming to a series."""
    if mode == "none":
        return {}
    if mode == "mini":
        # mini: keep just the last 12 points
        return _trim_by_keep(series, keep=12)
    # full: respect the global `keep` parameter
    return _trim_by_keep(series, keep=keep)


# -----------------------------------------------------------------------------
# provider call helper (existing compat / IMF interface)
# -----------------------------------------------------------------------------

def _call_provider(
    module: str,
    candidates: Iterable[str],
    **kwargs: Any,
) -> Tuple[Dict[str, float], Dict[str, Any]]:
    """
    Attempt to call one of the named functions from the given module.

    The provider is expected to return something convertible to {period -> float}
    via `_to_float_map`. We return:
        (series_dict, debug_info)

    debug_info has the shape:
        {
            "used": {"module": ..., "func": ...} or None,
            "tried": [
                {"module": ..., "func": ..., "found": bool, "error": "...?"}
            ]
        }
    """
    mod = _safe_import(module)
    tried = []
    for name in candidates:
        fn = _safe_get_attr(mod, name) if mod is not None else None
        tried.append({"module": module, "func": name, "found": bool(fn)})
        if not fn:
            continue
        try:
            raw = fn(**kwargs)
            series = _to_float_map(raw)
            return series, {"used": {"module": module, "func": name}, "tried": tried}
        except Exception as e:
            tried[-1]["error"] = repr(e)
            continue
    return {}, {"used": None, "tried": tried}


# -----------------------------------------------------------------------------
# ISO resolution
# -----------------------------------------------------------------------------

def _resolve_iso(country: str) -> Dict[str, Any]:
    """
    Resolve ISO codes for a country using the same logic as country_lite / probe.

    Returns a dict suitable for debug + provider calls, e.g.:
      {
        "name": <canonical country name>,
        "iso_alpha_2": "DE",
        "iso_alpha_3": "DEU",
        "iso_numeric": "276"
      }

    If resolution fails, we still return a dict with the input country name and
    null ISO codes plus an "_error" hint for debugging.
    """
    try:
        from app.utils.country_codes import get_country_codes

        codes = get_country_codes(country) or {}
        # When get_country_codes returns nothing, we still preserve the input.
        if not codes:
            return {
                "name": country,
                "iso_alpha_2": None,
                "iso_alpha_3": None,
                "iso_numeric": None,
                "_error": "get_country_codes returned empty result",
            }

        return {
            "name": codes.get("name", country),
            "iso_alpha_2": codes.get("iso_alpha_2"),
            "iso_alpha_3": codes.get("iso_alpha_3"),
            "iso_numeric": codes.get("iso_numeric"),
        }
    except Exception as e:
        # Fallback – at least preserve the country name, and expose the error for _debug.
        return {
            "name": country,
            "iso_alpha_2": None,
            "iso_alpha_3": None,
            "iso_numeric": None,
            "_error": repr(e),
        }

# -----------------------------------------------------------------------------
# payload init
# -----------------------------------------------------------------------------

def _init_payload(
    country: str,
    iso: Dict[str, Any],
    series: Literal["none", "mini", "full"],
    keep: int,
) -> Dict[str, Any]:
    return {
        "country": {
            "input": country,
            "name": iso.get("name"),
            "iso_alpha_2": iso.get("iso_alpha_2"),
            "iso_alpha_3": iso.get("iso_alpha_3"),
            "iso_numeric": iso.get("iso_numeric"),
        },
        "series_mode": series,
        "keep_points": keep,
        # legacy macro indicators (IMF compat)
        "indicators": {
            "cpi_yoy": {
                "series": {},
                "latest_period": None,
                "latest_value": None,
                "source": None,
                "freq": "monthly",
            },
            "unemployment_rate": {
                "series": {},
                "latest_period": None,
                "latest_value": None,
                "source": None,
                "freq": "monthly",
            },
            "fx_rate_usd": {
                "series": {},
                "latest_period": None,
                "latest_value": None,
                "source": None,
                "freq": "monthly",
            },
            "reserves_usd": {
                "series": {},
                "latest_period": None,
                "latest_value": None,
                "source": None,
                "freq": "monthly",
            },
            "policy_rate": {
                "series": {},
                "latest_period": None,
                "latest_value": None,
                "source": None,
                "freq": "monthly",
            },
            "gdp_growth": {
                "series": {},
                "latest_period": None,
                "latest_value": None,
                "source": None,
                "freq": "quarterly",
            },
        },
        # new matrix-based indicator blocks (TE-style KPIs)
        "indicators_matrix": {},
        # debt bundle
        "debt": {
            "government_debt": {
                "latest": {"value": None, "date": None, "source": None},
                "series": {},
            },
            "nominal_gdp": {
                "latest": {"value": None, "date": None, "source": None},
                "series": {},
            },
            "debt_to_gdp": {
                "latest": {"value": None, "date": None, "source": "computed:NA/NA"},
                "series": {},
            },
            "debt_to_gdp_series": {},
        },
        "_debug": {
            "iso": iso,
            "providers": {},
            "matrix": {},
        },
    }


def _attach_series_block(
    payload: Dict[str, Any],
    indicator_key: str,
    series: Dict[str, float],
    source_label: str,
    *,
    series_mode: Literal["none", "mini", "full"],
    keep: int,
) -> None:
    """Helper to attach time series and metadata for a given legacy indicator."""
    ind = payload["indicators"][indicator_key]
    trimmed = _apply_series_mode(series, series_mode, keep)
    if not trimmed:
        return
    latest_key, latest_val = _latest(trimmed)
    ind["series"] = trimmed
    ind["latest_period"] = latest_key
    ind["latest_value"] = latest_val
    ind["source"] = source_label


# -----------------------------------------------------------------------------
# legacy macro population (IMF compat)
# -----------------------------------------------------------------------------

def _populate_macro_blocks(
    payload: Dict[str, Any],
    iso: Dict[str, Any],
    *,
    series_mode: Literal["none", "mini", "full"],
    keep: int,
) -> None:
    iso2 = iso.get("iso_alpha_2")
    dbg_root = payload["_debug"]["providers"]

    # CPI YoY
    cpi_series, cpi_dbg = _call_provider(
        "app.providers.imf_provider",
        ("imf_cpi_yoy_monthly",),
        iso2=iso2,
    )
    cpi_series = _apply_series_mode(cpi_series, series_mode, keep)
    if cpi_series:
        _attach_series_block(
            payload,
            "cpi_yoy",
            cpi_series,
            "IMF (compat)",
            series_mode=series_mode,
            keep=keep,
        )
    dbg_root["cpi_yoy"] = cpi_dbg

    # Unemployment
    unemp_series, unemp_dbg = _call_provider(
        "app.providers.imf_provider",
        ("imf_unemployment_rate_monthly",),
        iso2=iso2,
    )
    unemp_series = _apply_series_mode(unemp_series, series_mode, keep)
    if unemp_series:
        _attach_series_block(
            payload,
            "unemployment_rate",
            unemp_series,
            "IMF (compat)",
            series_mode=series_mode,
            keep=keep,
        )
    dbg_root["unemployment_rate"] = unemp_dbg

    # FX rate vs USD
    fx_series, fx_dbg = _call_provider(
        "app.providers.imf_provider",
        ("imf_fx_to_usd_monthly",),
        iso2=iso2,
    )
    fx_series = _apply_series_mode(fx_series, series_mode, keep)
    if fx_series:
        _attach_series_block(
            payload,
            "fx_rate_usd",
            fx_series,
            "IMF (compat)",
            series_mode=series_mode,
            keep=keep,
        )
    dbg_root["fx_rate_usd"] = fx_dbg

    # Reserves (USD)
    res_series, res_dbg = _call_provider(
        "app.providers.imf_provider",
        ("imf_fx_reserves_usd_monthly",),
        iso2=iso2,
    )
    res_series = _apply_series_mode(res_series, series_mode, keep)
    if res_series:
        _attach_series_block(
            payload,
            "reserves_usd",
            res_series,
            "IMF (compat)",
            series_mode=series_mode,
            keep=keep,
        )
    dbg_root["reserves_usd"] = res_dbg

    # Policy rate
    pol_series, pol_dbg = _call_provider(
        "app.providers.imf_provider",
        ("imf_policy_rate_monthly",),
        iso2=iso2,
    )
    pol_series = _apply_series_mode(pol_series, series_mode, keep)
    if pol_series:
        _attach_series_block(
            payload,
            "policy_rate",
            pol_series,
            "IMF (compat)",
            series_mode=series_mode,
            keep=keep,
        )
    dbg_root["policy_rate"] = pol_dbg

    # GDP growth (quarterly)
    gdp_series, gdp_dbg = _call_provider(
        "app.providers.imf_provider",
        ("imf_gdp_growth_quarterly",),
        iso2=iso2,
    )
    gdp_series = _apply_series_mode(gdp_series, series_mode, keep)
    if gdp_series:
        _attach_series_block(
            payload,
            "gdp_growth",
            gdp_series,
            "IMF (compat)",
            series_mode=series_mode,
            keep=keep,
        )
    dbg_root["gdp_growth"] = gdp_dbg


# -----------------------------------------------------------------------------
# debt block
# -----------------------------------------------------------------------------

def _populate_debt_block(payload: Dict[str, Any], iso: Dict[str, Any]) -> None:
    if _compute_debt_payload is None:
        payload["_debug"]["debt_error"] = "compute_debt_payload not available"
        return

    iso3 = iso.get("iso_alpha_3")
    if not iso3:
        payload["_debug"]["debt_error"] = "iso_alpha_3 not available"
        return

    try:
        debt_payload = _compute_debt_payload(country=country)
        if not isinstance(debt_payload, Mapping):
            raise TypeError("compute_debt_payload returned non-mapping")

        debt_block = payload.get("debt", {})

        if "government_debt" in debt_payload:
            debt_block["government_debt"] = debt_payload["government_debt"]
        if "nominal_gdp" in debt_payload:
            debt_block["nominal_gdp"] = debt_payload["nominal_gdp"]
        if "debt_to_gdp" in debt_payload:
            debt_block["debt_to_gdp"] = debt_payload["debt_to_gdp"]
        if "debt_to_gdp_series" in debt_payload:
            debt_block["debt_to_gdp_series"] = debt_payload["debt_to_gdp_series"]

        payload["debt"] = debt_block

    except Exception as e:
        payload["_debug"]["debt_error"] = repr(e)


# -----------------------------------------------------------------------------
# NEW: indicator matrix integration
# -----------------------------------------------------------------------------

def _apply_transform(series: Dict[str, float], transform: Optional[str]) -> Dict[str, float]:
    """
    Apply simple transforms like yoy, mom, qoq, ratio.

    This is deliberately generic; for monthly/quarterly vs annual we rely on the
    input series ordering, not full calendar logic. Good enough for v1.
    """
    if not series or not transform or transform == "none":
        return series

    keys = sorted(series.keys(), key=_parse_period_key)
    if len(keys) < 2:
        return series

    def _pct_change(prev: float, curr: float) -> float:
        if prev in (0.0, 0):
            return 0.0
        return (curr / prev - 1.0) * 100.0

    if transform in ("yoy", "mom", "qoq"):
        out: Dict[str, float] = {}
        prev_key = None
        prev_val = None
        for k in keys:
            v = series[k]
            if prev_key is None:
                prev_key, prev_val = k, v
                continue
            out[k] = _pct_change(prev_val, v)  # type: ignore[arg-type]
            prev_key, prev_val = k, v
        return out

    # "ratio": assume upstream already computed numerator/denominator
    # and provided final % series, so we just return as-is.
    return series


def _fetch_series_from_matrix_source(iso: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, float]:
    """
    Given an ISO dict and a SourceSpec from INDICATOR_MATRIX, try to
    return a {period -> float} dict from the appropriate provider.

    All branches are defensive: if the provider or function isn't there,
    we just return {} so the next source can be tried.
    """
    provider = src.get("provider")
    dataset = src.get("dataset")
    indicator = src.get("indicator")
    func_name = src.get("func")

    iso2 = iso.get("iso_alpha_2")
    iso3 = iso.get("iso_alpha_3") or iso.get("iso_numeric")

    # IMF
    if provider == "imf":
        mod = _safe_import("app.providers.imf_provider")
        if not mod or not iso2:
            return {}
        # If a specific helper is given (e.g. imf_cpi_yoy_monthly), use it:
        if func_name and hasattr(mod, func_name):
            try:
                fn = getattr(mod, func_name)
                raw = fn(iso2=iso2)
                return _to_float_map(raw)
            except Exception:
                return {}
        # Otherwise, you can later add a generic IMF series fetcher here
        return {}

    # World Bank
    if provider == "world_bank":
        mod = _safe_import("app.providers.wb_provider")
        if not mod or not iso3 or not indicator:
            return {}
        # Guard for function existence
        fetch_raw = _safe_get_attr(mod, "fetch_wb_indicator_raw")
        as_year_dict = _safe_get_attr(mod, "wb_year_dict_from_raw")
        if not fetch_raw or not as_year_dict:
            return {}
        try:
            raw = fetch_raw(iso3, indicator)
            return as_year_dict(raw) or {}
        except Exception:
            return {}

    # Eurostat
    if provider == "eurostat":
        mod = _safe_import("app.providers.eurostat_provider")
        if not mod:
            return {}
        # You can later wire dataset+indicator to a generic eurostat helper
        return {}

    # OECD
    if provider == "oecd":
        mod = _safe_import("app.providers.oecd_provider")
        if not mod or not iso3:
            return {}
        series = _safe_get_attr(mod, "oecd_series")
        if not series:
            return {}
        try:
            raw = series(iso3=iso3, indicator=indicator or "")
            return _to_float_map(raw)
        except Exception:
            return {}

    # GMD
    if provider == "gmd":
        mod = _safe_import("app.providers.gmd_provider")
        if not mod or not iso3:
            return {}
        gmd_series = _safe_get_attr(mod, "gmd_series")
        if not gmd_series:
            return {}
        try:
            raw = gmd_series(iso3=iso3, indicator=indicator or "")
            return _to_float_map(raw)
        except Exception:
            return {}

    # ECB
    if provider == "ecb":
        mod = _safe_import("app.providers.ecb_provider")
        if not mod:
            return {}
        # TODO: call your existing ECB helpers for FX/policy rate
        return {}

    # DBnomics
    if provider == "dbnomics":
        mod = _safe_import("app.providers.dbnomics_provider")
        if not mod or not iso3:
            return {}
        db_series = _safe_get_attr(mod, "dbnomics_series")
        if not db_series:
            return {}
        try:
            raw = db_series(
                provider_code=str(dataset or ""),
                dataset=str(dataset or ""),
                indicator=str(indicator or ""),
                iso3=iso3,
            )
            return _to_float_map(raw)
        except Exception:
            return {}

    return {}


def _build_indicator_block_from_matrix(
    iso: Dict[str, Any],
    key: str,
    *,
    series_mode: Literal["none", "mini", "full"],
    keep: int,
) -> Dict[str, Any]:
    """
    Use INDICATOR_MATRIX to build a normalized indicator block:

    {
      "latest": {"value": ..., "date": "...", "source": "..."},
      "series": {"YYYY-..": value, ...}
    }
    """
    spec = INDICATOR_MATRIX[key]
    max_age_years = spec.get("max_age_years")
    sources = spec.get("sources") or []

    debug_tried = []

    for src in sources:
        series = _fetch_series_from_matrix_source(iso, src)
        if not series:
            debug_tried.append(
                {"provider": src.get("provider"), "dataset": src.get("dataset"), "indicator": src.get("indicator"), "status": "empty"}
            )
            continue

        # apply transform if any
        series = _apply_transform(series, src.get("transform"))
        if not series:
            debug_tried.append(
                {"provider": src.get("provider"), "dataset": src.get("dataset"), "indicator": src.get("indicator"), "status": "after_transform_empty"}
            )
            continue

        # sort & find latest
        keys_sorted = sorted(series.keys(), key=_parse_period_key)
        latest_key = keys_sorted[-1]
        latest_val = series[latest_key]

        # recency by years (for annual-data-heavy indicators)
        if max_age_years and len(str(latest_key)) == 4:
            try:
                yr = int(latest_key)
                if date.today().year - yr > max_age_years:
                    debug_tried.append(
                        {
                            "provider": src.get("provider"),
                            "dataset": src.get("dataset"),
                            "indicator": src.get("indicator"),
                            "status": f"too_old:{latest_key}",
                        }
                    )
                    continue
            except Exception:
                pass

        # apply keep to matrix history (use same semantics as series_mode)
        trimmed = _apply_series_mode(series, series_mode, keep)

        return {
            "latest": {
                "value": latest_val,
                "date": latest_key,
                "source": f"{src.get('provider')}::{src.get('dataset') or src.get('indicator') or 'series'}",
            },
            "series": trimmed,
            "_debug": {"tried": debug_tried},
        }

    # if none returned anything usable
    return {
        "latest": {"value": None, "date": None, "source": "unavailable"},
        "series": {},
        "_debug": {"tried": debug_tried},
    }


def _populate_indicator_matrix(
    payload: Dict[str, Any],
    iso: Dict[str, Any],
    *,
    series_mode: Literal["none", "mini", "full"],
    keep: int,
) -> None:
    """
    Populate payload['indicators_matrix'] using INDICATOR_MATRIX.

    This does NOT affect the existing 'indicators' block; it's additive.
    """
    out: Dict[str, Any] = {}
    matrix_debug: Dict[str, Any] = {}

    for key in INDICATOR_MATRIX.keys():
        block = _build_indicator_block_from_matrix(iso, key, series_mode=series_mode, keep=keep)
        out[key] = {
            "latest": block["latest"],
            "series": block["series"],
        }
        matrix_debug[key] = block.get("_debug", {})

    payload["indicators_matrix"] = out
    payload["_debug"]["matrix"] = matrix_debug


# -----------------------------------------------------------------------------
# builder v2 core
# -----------------------------------------------------------------------------

def _build_country_payload_v2_core(
    country: str,
    series: Literal["none", "mini", "full"] = "mini",
    keep: int = 60,
) -> Dict[str, Any]:
    """
    Modern builder for Country Radar:
    - Compat-first IMF monthly/quarterly indicators (legacy 'indicators' block)
    - Debt bundle via app.services.debt_service.compute_debt_payload
    - Matrix-based indicators via INDICATOR_MATRIX ('indicators_matrix')
    """
    iso = _resolve_iso(country)
    payload = _init_payload(country, iso, series, keep)

    # Populate legacy macro indicators
    _populate_macro_blocks(payload, iso, series_mode=series, keep=keep)

    # Populate matrix indicators (IMF + WB + Eurostat + OECD + GMD + DBnomics)
    _populate_indicator_matrix(payload, iso, series_mode=series, keep=keep)

    # Enrich with debt data if available (uses compute_debt_payload under the hood)
    _populate_debt_block(payload, iso)

    # Make sure additional_indicators.gov_debt_pct_gdp is wired
    # from the normalized debt_to_gdp bundle (IMF/Eurostat/WB/derived).
    _populate_debt_fiscal_additional_indicators(payload)

    return payload


def _populate_debt_fiscal_additional_indicators(payload: Dict[str, Any]) -> None:
    """
    Ensure that the normalized Debt-to-GDP series coming from debt_service
    is surfaced in additional_indicators.gov_debt_pct_gdp so that clients
    (and /v1/country-lite) see a consistent debt % of GDP field.

    - Reads from top-level payload["debt_to_gdp"] and/or payload["debt_to_gdp_series"]
      which are populated by _populate_debt_block (compute_debt_payload).
    - Only touches additional_indicators["gov_debt_pct_gdp"].
    - If that block already exists, it will only backfill missing latest_value /
      series; it will not overwrite non-null values.
    """
    # Ensure we have an additional_indicators dict
    addl = payload.setdefault("additional_indicators", {})

    # ---- Extract debt_to_gdp info from the top-level bundle ----
    debt_block = payload.get("debt_to_gdp")
    series_from_block: Dict[str, float] = {}
    latest_value: Optional[float] = None
    latest_period: Optional[str] = None
    latest_source: Optional[str] = None

    if isinstance(debt_block, Mapping):
        latest = debt_block.get("latest") or {}
        series_from_block = debt_block.get("series") or {}
        latest_value = latest.get("value")
        latest_period = latest.get("date")
        latest_source = latest.get("source")
    else:
        # Fallback: compute latest from debt_to_gdp_series if present
        raw_series = payload.get("debt_to_gdp_series") or {}
        if isinstance(raw_series, Mapping) and raw_series:
            series_from_block = raw_series
            try:
                years_sorted = sorted(series_from_block.keys(), key=lambda y: int(str(y)))
                latest_period = years_sorted[-1]
                latest_value = series_from_block[latest_period]
            except Exception:
                latest_period = None
                latest_value = None

    # If we still have nothing, there's nothing to map
    if latest_value is None and not series_from_block:
        return

    # ---- Wire into additional_indicators.gov_debt_pct_gdp ----
    gov_block = addl.get("gov_debt_pct_gdp")

    if not isinstance(gov_block, Mapping):
        # Create a fresh block
        addl["gov_debt_pct_gdp"] = {
            "latest_value": latest_value,
            "latest_period": latest_period,
            "source": latest_source or "debt_service",
            "series": series_from_block,
        }
    else:
        # Backfill missing fields without clobbering existing good data
        if gov_block.get("latest_value") is None and latest_value is not None:
            gov_block["latest_value"] = latest_value
            gov_block["latest_period"] = latest_period
            if latest_source:
                gov_block["source"] = latest_source

        if not gov_block.get("series") and series_from_block:
            gov_block["series"] = series_from_block


# -----------------------------------------------------------------------------
# recency handling for legacy 'indicators' block
# -----------------------------------------------------------------------------

INDICATOR_RECENCY_RULES: Dict[str, Dict[str, int]] = {
    # Growth / activity
    "gdp_growth": {
        "max_age_months": 24,  # up to ~2 years
    },
    # Prices
    "cpi_yoy": {
        "max_age_months": 6,
    },
    # Labour
    "unemployment_rate": {
        "max_age_months": 18,
    },
    # Rates / FX / reserves
    "policy_rate": {
        "max_age_months": 12,
    },
    "fx_rate_usd": {
        "max_age_months": 12,
    },
    "reserves_usd": {
        "max_age_months": 24,
    },
}


def _period_to_date_generic(period: Any) -> date:
    """Best-effort conversion of 'YYYY', 'YYYY-MM', 'YYYY-Qn' to a date.

    We only need month-level granularity to compare ages; day is fixed to 1.
    Invalid or missing periods are mapped to a very old date so they fail
    recency checks.
    """
    if period is None:
        return date(1900, 1, 1)
    s = str(period)
    if not s:
        return date(1900, 1, 1)

    if "-Q" in s:
        try:
            year_str, q_str = s.split("-Q", 1)
            y = int(year_str)
            q = int(q_str)
            month = (q - 1) * 3 + 2  # Q1→Feb, Q2→May, etc.
            month = max(1, min(12, month))
            return date(y, month, 1)
        except Exception:
            return date(1900, 1, 1)

    parts = s.split("-")
    try:
        if len(parts) == 2:
            y = int(parts[0])
            m = int(parts[1])
            m = max(1, min(12, m))
            return date(y, m, 1)
        if len(parts) == 1:
            y = int(parts[0])
            return date(y, 1, 1)
    except Exception:
        return date(1900, 1, 1)
    return date(1900, 1, 1)


def _is_fresh_for_indicator(
    name: str,
    latest_period: Any,
    *,
    today: Optional[date] = None,
) -> bool:
    """Return True if the given indicator's latest_period is recent enough.

    If an indicator has no configured recency rule, we treat it as always fresh.
    """
    rules = INDICATOR_RECENCY_RULES.get(name)
    if not rules:
        return True  # no rule → assume OK

    d = _period_to_date_generic(latest_period)
    today = today or date.today()

    total_today = today.year * 12 + today.month
    total_d = d.year * 12 + d.month

    max_age_months = rules.get("max_age_months")
    if max_age_months is not None:
        if (total_today - total_d) > max_age_months:
            return False

    return True


def _apply_recency_to_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Walk the payload produced by _build_country_payload_v2_core and blank out
    "latest" values for legacy indicators whose latest_period is too old
    according to INDICATOR_RECENCY_RULES.

    This is a non-destructive transformation: we keep the original series and
    latest_* values under a _debug namespace so we can inspect them later.
    """
    indicators = payload.get("indicators")
    if isinstance(indicators, Mapping):
        for name, block in indicators.items():
            if not isinstance(block, Mapping):
                continue
            latest_period = block.get("latest_period")
            if latest_period is None:
                continue
            if not _is_fresh_for_indicator(name, latest_period):
                if "_debug" not in block:
                    block["_debug"] = {}
                block["_debug"]["stale_latest_period"] = latest_period
                block["_debug"]["stale_latest_value"] = block.get("latest_value")

                block["latest_period"] = None
                block["latest_value"] = None

    # Debt recency could be enforced here if desired. For now we only operate
    # on the legacy indicators; the matrix indicators enforce max_age_years
    # directly in _build_indicator_block_from_matrix.
    return payload


# -----------------------------------------------------------------------------
# public entrypoints
# -----------------------------------------------------------------------------

def build_country_payload_v2(
    country: str,
    series: Literal["none", "mini", "full"] = "mini",
    keep: int = 60,
) -> Dict[str, Any]:
    """
    Public entrypoint for the v2 builder.

    - Delegates to _build_country_payload_v2_core
    - Applies recency filtering on legacy 'indicators'
    - Leaves 'indicators_matrix' as-is (it already enforces max_age_years)
    """
    core_payload = _build_country_payload_v2_core(
        country=country,
        series=series,
        keep=keep,
    )
    if not isinstance(core_payload, Mapping):
        return core_payload  # type: ignore[return-value]
    return _apply_recency_to_payload(dict(core_payload))


def build_country_payload(
    country: str,
    series: str = "mini",
    keep: int = 60,
) -> Dict[str, Any]:
    """Compatibility wrapper for legacy callers—delegate to v2 with same signature."""
    mode: Literal["none", "mini", "full"]
    if series not in ("none", "mini", "full"):
        mode = "mini"
    else:
        mode = series  # type: ignore[assignment]
    return build_country_payload_v2(country=country, series=mode, keep=keep)
