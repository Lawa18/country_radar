# app/services/indicator_service.py — v2 builder using provider compat shim
from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, Literal
import math
from datetime import date
from app.services.indicator_matrix import INDICATOR_MATRIX

# add near top of indicator_service.py
try:
    from app.services.debt_service import compute_debt_payload as _compute_debt_payload
except Exception:  # keep the module import non-fatal
    _compute_debt_payload = None

# ----------------------- utils: imports & coercion ----------------------------

def _safe_import(path: str):
    try:
        module = __import__(path, fromlist=['*'])
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
        out = {}
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

# -------------------- provider call helper (fixed interface) ------------------

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

# --------------------------- builder v2 (core) --------------------------------

def _resolve_iso(country: str) -> Dict[str, Any]:
    """
    Resolve a country identifier into ISO codes via compat provider.

    We keep this logic inside the service to decouple routes from provider
    details. The compat provider is responsible for fuzzy matching etc.
    """
    series, dbg = _call_provider("app.providers.compat", ("resolve_iso",), query=country)
    # compat.resolve_iso returns a dict; but _call_provider normalises outputs
    # to a dict-of-floats shape, which is not what we want here. Instead, we
    # call compat directly under a try/except.
    try:
        mod = _safe_import("app.providers.compat")
        fn = _safe_get_attr(mod, "resolve_iso")
        if fn is None:
            raise RuntimeError("compat.resolve_iso not found")
        iso = fn(country)
        if not isinstance(iso, Mapping):
            raise TypeError("compat.resolve_iso returned non-mapping")
        return dict(iso)
    except Exception as e:
        # In a failure case, we still build a payload but mark the error.
        return {
            "name": country,
            "iso_alpha_2": None,
            "iso_alpha_3": None,
            "iso_numeric": None,
            "_error": repr(e),
        }


def _init_payload(country: str, iso: Dict[str, Any], series: Literal["none", "mini", "full"], keep: int) -> Dict[str, Any]:
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
        "indicators": {
            "cpi_yoy":          {"series": {}, "latest_period": None, "latest_value": None, "source": None, "freq": "monthly"},
            "unemployment_rate":{"series": {}, "latest_period": None, "latest_value": None, "source": None, "freq": "monthly"},
            "fx_rate_usd":      {"series": {}, "latest_period": None, "latest_value": None, "source": None, "freq": "monthly"},
            "reserves_usd":     {"series": {}, "latest_period": None, "latest_value": None, "source": None, "freq": "monthly"},
            "policy_rate":      {"series": {}, "latest_period": None, "latest_value": None, "source": None, "freq": "monthly"},
            "gdp_growth":       {"series": {}, "latest_period": None, "latest_value": None, "source": None, "freq": "quarterly"},
        },
        "debt": {
            "government_debt": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
            "nominal_gdp":     {"latest": {"value": None, "date": None, "source": None}, "series": {}},
            "debt_to_gdp":     {"latest": {"value": None, "date": None, "source": "computed:NA/NA"}, "series": {}},
            "debt_to_gdp_series": {},
        },
        "_debug": {
            "iso": iso,
            "providers": {},
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
    """Helper to attach time series and metadata for a given indicator."""
    ind = payload["indicators"][indicator_key]
    trimmed = _apply_series_mode(series, series_mode, keep)
    if not trimmed:
        return
    latest_key, latest_val = _latest(trimmed)
    ind["series"] = trimmed
    ind["latest_period"] = latest_key
    ind["latest_value"] = latest_val
    ind["source"] = source_label


def _populate_macro_blocks(
    payload: Dict[str, Any],
    iso: Dict[str, Any],
    *,
    series_mode: Literal["none", "mini", "full"],
    keep: int,
) -> None:
    iso2 = iso.get("iso_alpha_2")
    iso3 = iso.get("iso_alpha_3")
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
            payload, "cpi_yoy", cpi_series, "IMF (compat)",
            series_mode=series_mode, keep=keep,
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
            payload, "unemployment_rate", unemp_series, "IMF (compat)",
            series_mode=series_mode, keep=keep,
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
            payload, "fx_rate_usd", fx_series, "IMF (compat)",
            series_mode=series_mode, keep=keep,
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
            payload, "reserves_usd", res_series, "IMF (compat)",
            series_mode=series_mode, keep=keep,
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
            payload, "policy_rate", pol_series, "IMF (compat)",
            series_mode=series_mode, keep=keep,
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
            payload, "gdp_growth", gdp_series, "IMF (compat)",
            series_mode=series_mode, keep=keep,
        )
    dbg_root["gdp_growth"] = gdp_dbg


def _populate_debt_block(payload: Dict[str, Any], iso: Dict[str, Any]) -> None:
    if _compute_debt_payload is None:
        payload["_debug"]["debt_error"] = "compute_debt_payload not available"
        return

    iso3 = iso.get("iso_alpha_3")
    if not iso3:
        payload["_debug"]["debt_error"] = "iso_alpha_3 not available"
        return

    try:
        debt_payload = _compute_debt_payload(iso3=iso3)
        if not isinstance(debt_payload, Mapping):
            raise TypeError("compute_debt_payload returned non-mapping")

        # Expect shape: {"government_debt": {...}, "nominal_gdp": {...}, "debt_to_gdp": {...}}
        debt_block = payload.get("debt", {})
        # Government debt
        if "government_debt" in debt_payload:
            debt_block["government_debt"] = debt_payload["government_debt"]
        # Nominal GDP
        if "nominal_gdp" in debt_payload:
            debt_block["nominal_gdp"] = debt_payload["nominal_gdp"]
        # Debt-to-GDP (latest + series)
        if "debt_to_gdp" in debt_payload:
            debt_block["debt_to_gdp"] = debt_payload["debt_to_gdp"]
        if "debt_to_gdp_series" in debt_payload:
            debt_block["debt_to_gdp_series"] = debt_payload["debt_to_gdp_series"]

        payload["debt"] = debt_block

    except Exception as e:
        payload["_debug"]["debt_error"] = repr(e)

# --------------------------- builder v2 (core) continued ----------------------

def _build_country_payload_v2_core(
    country: str,
    series: Literal["none", "mini", "full"] = "mini",
    keep: int = 60,
) -> Dict[str, Any]:
    """
    Modern builder for Country Radar:
    - Compat-first IMF monthly/quarterly indicators (via app.providers.compat)
    - Optional debt bundle via app.services.debt_service.compute_debt_payload
    - Normalised output schema for use by GPT / UI layers
    """
    iso = _resolve_iso(country)
    payload = _init_payload(country, iso, series, keep)

    # Populate macro indicators
    _populate_macro_blocks(payload, iso, series_mode=series, keep=keep)

    # Attempt to enrich with debt data if available
    _populate_debt_block(payload, iso)

    return payload

# --------------------------- recency handling wrapper -------------------------

# Soft recency rules per-indicator. These are intentionally conservative and can
# be tuned without touching the core builder implementation.
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
    # Debt-related metrics will be handled separately by debt_service;
    # we may still introduce a soft rule for their latest year:
    # e.g. ignore debt older than 7-10 years.
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

    # Quarterly form: 'YYYY-Qn'
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


def _is_fresh_for_indicator(name: str, latest_period: Any, *, today: Optional[date] = None) -> bool:
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
    "latest" values for indicators whose latest_period is too old according to
    INDICATOR_RECENCY_RULES.

    This is a non-destructive transformation: we keep the original series and
    latest_* values under a _debug namespace so we can inspect them later.
    """
    # Handle top-level indicators
    indicators = payload.get("indicators")
    if isinstance(indicators, Mapping):
        for name, block in indicators.items():
            if not isinstance(block, Mapping):
                continue
            latest_period = block.get("latest_period")
            if latest_period is None:
                continue
            if not _is_fresh_for_indicator(name, latest_period):
                # Preserve the original values under a debug namespace, but
                # clear the public latest_* fields so they surface as N/A.
                if "_debug" not in block:
                    block["_debug"] = {}
                block["_debug"]["stale_latest_period"] = latest_period
                block["_debug"]["stale_latest_value"] = block.get("latest_value")

                block["latest_period"] = None
                block["latest_value"] = None

    # Debt recency could be enforced here if desired. For now we only operate
    # on the indicators; debt_service is expected to provide reasonably recent
    # metrics and will be enhanced separately.

    return payload


def build_country_payload_v2(
    country: str,
    series: Literal["none", "mini", "full"] = "mini",
    keep: int = 60,
) -> Dict[str, Any]:
    """
    Public entrypoint for the v2 builder.

    The original implementation is preserved as _build_country_payload_v2_core;
    this thin wrapper delegates to it and then applies recency filtering on the
    resulting payload. This ensures we do not lose any behaviour while still
    preventing very old series (e.g. 1990 debt, 1998 FX) from surfacing as
    current values.
    """
    core_payload = _build_country_payload_v2_core(*args, **kwargs)
    # Be defensive: if core returns something unexpected, just pass it through.
    if not isinstance(core_payload, Mapping):
        return core_payload  # type: ignore[return-value]
    return _apply_recency_to_payload(dict(core_payload))


# --------------------------- legacy fallback ----------------------------------

def build_country_payload(country: str, series: str = "mini", keep: int = 60) -> Dict[str, Any]:
    """Compatibility wrapper for legacy callers—delegate to v2 with same signature."""
    # keep type-safety for `series` by constraining to allowed literals
    mode: Literal["none", "mini", "full"]
    if series not in ("none", "mini", "full"):
        mode = "mini"
    else:
        mode = series  # type: ignore[assignment]
    return build_country_payload_v2(country=country, series=mode, keep=keep)
