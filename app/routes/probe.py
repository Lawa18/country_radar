# app/routes/probe.py — diagnostics + lightweight country info (stable + cached)
from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional, Tuple
import inspect
import time as _time
import logging
import concurrent.futures as _futures

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse, Response

logger = logging.getLogger("country-radar")

router = APIRouter(tags=["probe"])

# -----------------------------------------------------------------------------
# Small helpers and shared utilities
# -----------------------------------------------------------------------------


def _safe_import(module: str):
    try:
        return __import__(module, fromlist=["*"])
    except Exception:
        return None


def _iter_public_callables(mod: Any) -> Iterable[Tuple[str, Any]]:
    for name in dir(mod):
        if name.startswith("_"):
            continue
        obj = getattr(mod, name, None)
        if callable(obj):
            yield name, obj


def _iso_codes(country: str) -> Dict[str, Optional[str]]:
    try:
        from app.utils.country_codes import get_country_codes

        codes = get_country_codes(country) or {}
        return {
            "name": codes.get("name"),
            "iso_alpha_2": codes.get("iso_alpha_2"),
            "iso_alpha_3": codes.get("iso_alpha_3"),
            "iso_numeric": codes.get("iso_numeric"),
        }
    except Exception:
        return {
            "name": country,
            "iso_alpha_2": None,
            "iso_alpha_3": None,
            "iso_numeric": None,
        }


def _coerce_numeric_series(d: Optional[Mapping[str, Any]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not d or not isinstance(d, Mapping):
        return out
    for k, v in d.items():
        try:
            out[str(k)] = float(v)
        except Exception:
            # ignore non-numeric
            pass
    return out


def _latest(series: Mapping[str, float]) -> Tuple[Optional[str], Optional[float]]:
    if not series:
        return None, None
    # sort by key; this assumes period-like keys but is fine for diagnostics
    keys = sorted(series.keys())
    k = keys[-1]
    return k, series[k]


def _freq_of_key(k: str) -> str:
    """
    Very rough frequency detection from a period key.
    - YYYY       -> A
    - YYYY-Qn    -> Q
    - YYYY-MM    -> M
    """
    s = str(k)
    if "-Q" in s:
        return "Q"
    if "-" in s:
        # if second part looks like MM, treat as monthly
        parts = s.split("-")
        if len(parts) >= 2 and parts[0].isdigit():
            return "M"
    return "A"


def _trim_series_policy(series: Mapping[str, float], policy: Dict[str, int]) -> Dict[str, float]:
    """
    Trim a mixed or single-freq series to the policy windows by freq.
    For mixed keys (rare), we group by freq and trim each group.
    """
    if not series:
        return {}
    buckets: Dict[str, Dict[str, float]] = {"A": {}, "Q": {}, "M": {}}
    for k, v in series.items():
        freq = _freq_of_key(k)
        if freq not in buckets:
            freq = "A"
        try:
            buckets[freq][k] = float(v)
        except Exception:
            pass

    out: Dict[str, float] = {}
    for freq, ser in buckets.items():
        keep = policy.get(freq, len(ser))
        keys = sorted(ser.keys())
        if len(keys) <= keep:
            out.update({k: ser[k] for k in keys})
        else:
            out.update({k: ser[k] for k in keys[-keep:]})
    return out


# Global history policy (years/quarters/months)
HIST_POLICY: Dict[str, int] = {"A": 20, "Q": 12, "M": 48}

# -----------------------------------------------------------------------------
# Tiny response cache for /v1/country-lite
# -----------------------------------------------------------------------------
_COUNTRY_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_COUNTRY_TTL = 600.0  # 10 minutes


def _cache_get(country: str) -> Optional[Dict[str, Any]]:
    row = _COUNTRY_CACHE.get(country.lower())
    if not row:
        return None
    ts, payload = row
    if _time.time() - ts > _COUNTRY_TTL:
        return None
    return payload


def _cache_set(country: str, payload: Dict[str, Any]) -> None:
    # Cache key normalized to avoid "Mexico" vs "mexico" misses.
    _COUNTRY_CACHE[country.lower()] = (_time.time(), payload)

# -----------------------------------------------------------------------------
# Compat provider wrapper (with retries + basic timeouts)
# -----------------------------------------------------------------------------
_EXECUTOR = _futures.ThreadPoolExecutor(max_workers=8)


def _with_timeout(timeout_s: float, fn, *args, **kwargs):
    """
    Run fn(*args, **kwargs) in a thread pool with a hard timeout.
    Used to keep country-lite from blocking on heavy calls.
    """
    fut = _EXECUTOR.submit(fn, *args, **kwargs)
    try:
        return fut.result(timeout=timeout_s)
    except Exception:
        try:
            fut.cancel()
        except Exception:
            pass
        return None


def _compat_fetch_series(func_name: str, country: str, want_freq: str, keep_hint: int) -> Dict[str, float]:
    """
    Call app.providers.compat.<func_name>(country=...) and coerce numeric series.
    """
    mod = _safe_import("app.providers.compat")
    if not mod:
        return {}
    fn = getattr(mod, func_name, None)
    if not callable(fn):
        return {}
    try:
        raw = fn(country, keep=keep_hint)
    except TypeError:
        # older compat signatures might not accept keep=
        raw = fn(country)
    except Exception:
        return {}
    series = _coerce_numeric_series(raw)
    return _trim_series_policy(series, HIST_POLICY)


def _compat_fetch_series_retry(
    func_name: str,
    country: str,
    want_freq: str,
    keep_hint: int,
    retries: int = 1,
) -> Dict[str, float]:
    """
    Retry compat fetch once on failure (simple heuristic).
    """
    series = _compat_fetch_series(func_name, country, want_freq, keep_hint)
    if series:
        return series
    if retries <= 0:
        return series
    # brief pause and try once more
    _time.sleep(0.1)
    return _compat_fetch_series(func_name, country, want_freq, keep_hint)


def _get_iso3(country: str) -> Optional[str]:
    try:
        codes = _iso_codes(country)
        return codes.get("iso_alpha_3")
    except Exception:
        return None


def _wb_series_from_helpers(country: str, helper_name: str) -> Dict[str, float]:
    """
    Call a World Bank *helper* (e.g., wb_current_account_balance_pct_gdp_annual)
    exposed on app.providers.wb_provider if present.
    """
    mod = _safe_import("app.providers.wb_provider")
    if not mod:
        return {}
    helper = getattr(mod, helper_name, None)
    if not callable(helper):
        return {}
    try:
        iso3 = _get_iso3(country)
        if not iso3:
            return {}
        raw = helper(iso3)
        series = _coerce_numeric_series(raw)
        return _trim_series_policy(series, HIST_POLICY)
    except Exception:
        return {}


def _wb_series_generic(country: str, indicator_code: str) -> Dict[str, float]:
    """
    Generic call to World Bank WDI using country ISO3 and an indicator code.
    """
    mod = _safe_import("app.providers.wb_provider")
    if not mod:
        return {}
    fetch = getattr(mod, "fetch_wb_indicator_raw", None)
    to_year = getattr(mod, "wb_year_dict_from_raw", None)
    if not callable(fetch) or not callable(to_year):
        return {}
    try:
        iso3 = _get_iso3(country)
        if not iso3:
            return {}
        raw = fetch(iso3, indicator_code)
        series = _coerce_numeric_series(to_year(raw))
        return _trim_series_policy(series, HIST_POLICY)
    except Exception:
        return {}


# -----------------------------------------------------------------------------
# Provider probe endpoint (for diagnostics)
# -----------------------------------------------------------------------------

@router.get("/v1/provider-probe")
def provider_probe() -> JSONResponse:
    """
    Introspect compat/imf/wb providers and list public callables.
    """
    modules = {
        "compat": _safe_import("app.providers.compat"),
        "imf": _safe_import("app.providers.imf_provider"),
        "wb": _safe_import("app.providers.wb_provider"),
    }

    info: Dict[str, Any] = {}
    for name, mod in modules.items():
        if not mod:
            info[name] = {"available": False}
            continue
        info[name] = {
            "available": True,
            "public_callables": [],
        }
        for fn_name, fn in _iter_public_callables(mod):
            try:
                sig = str(inspect.signature(fn))
            except Exception:
                sig = "(unknown)"
            info[name]["public_callables"].append(
                {
                    "name": fn_name,
                    "signature": sig,
                }
            )

    return JSONResponse(content={"modules": info})

# -----------------------------------------------------------------------------
# Country-lite: compat-first, WB-backed, bounded windows
# -----------------------------------------------------------------------------


@router.get(
    "/v1/country-lite",
    summary="Country Lite",
    operation_id="country_lite_get",
    tags=["probe"],
    description=(
        "Compat-first snapshot with bounded history windows:\n"
        "  - Debt-to-GDP (annual, last 20y) — hard timeout to avoid route blocking\n"
        "  - GDP growth (quarterly, last 4q)\n"
        "  - Monthly set (CPI YoY, Unemployment, FX, Reserves, Policy Rate)\n"
        "  - Annual set (Current Account % GDP, Government Effectiveness)\n"
        "This is a lighter-weight alternative to the full /country-data route."
    ),
)
def country_lite(
    country: str = Query(..., description="Full country name, e.g., Mexico"),
    fresh: bool = Query(False, description="Bypass cache if true"),
) -> JSONResponse:
    started = _time.time()

    # ----------------------------
    # 0) Cache
    # ----------------------------
    if not fresh:
        cached = _cache_get(country)
        if cached:
            logger.info("country_lite cache hit | country=%s", country)
            return JSONResponse(content=cached)

    iso = _iso_codes(country)

    # ----------------------------
    # Helper to safely fetch compat series without killing the route
    # ----------------------------
    def _safe_compat(fn_name: str, freq: str, keep_hint: int) -> Dict[str, float]:
        try:
            return _compat_fetch_series_retry(fn_name, country, freq, keep_hint=keep_hint) or {}
        except Exception:
            return {}

    def _safe_wb(indicator_code: str) -> Dict[str, float]:
        try:
            return _wb_series_generic(country, indicator_code) or {}
        except Exception:
            return {}

    # ----------------------------
    # 1) Debt (hard timeout)
    # ----------------------------
    debt_series_full: Dict[str, float] = {}
    debt_latest_summary: Dict[str, Any] = {"year": None, "value": None, "source": "computed:NA/Timeout"}
    debt_bundle: Dict[str, Any] = {}

    try:
        from app.services.debt_service import compute_debt_payload
        bundle = _with_timeout(1.8, compute_debt_payload, country) or {}
    except Exception as e:
        logger.warning("country_lite debt import/call error for %s: %r", country, e)
        bundle = {}

    if isinstance(bundle, Mapping):
        debt_bundle = dict(bundle)
        debt_block = debt_bundle.get("debt_to_gdp") or {}
        series = debt_block.get("series") or debt_bundle.get("debt_to_gdp_series") or {}

        if isinstance(series, Mapping) and series:
            debt_series_full = dict(series)
            try:
                years_sorted = sorted(debt_series_full.keys(), key=lambda y: int(str(y)))
                latest_year = years_sorted[-1]
                latest_val = debt_series_full[latest_year]
            except Exception:
                latest_year = None
                latest_val = None

            latest_meta = debt_block.get("latest") or {}
            debt_latest_summary = {
                "year": latest_year,
                "value": latest_val,
                "source": latest_meta.get("source") or "debt_service",
            }

    debt_series = _trim_series_policy(debt_series_full, HIST_POLICY)

    # ----------------------------
    # 2) Parallel fetches (compat monthly+quarterly + WB annual)
    #    This prevents serial 20–60s behavior.
    # ----------------------------
    futures: Dict[str, Any] = {}
    with _futures.ThreadPoolExecutor(max_workers=10) as ex:
        # Quarterly (compat)
        futures["gdp_growth_q"] = ex.submit(_safe_compat, "get_gdp_growth_quarterly", "Q", 12)

        # Monthly (compat)
        futures["cpi_m"] = ex.submit(_safe_compat, "get_cpi_yoy_monthly", "M", 36)
        futures["une_m"] = ex.submit(_safe_compat, "get_unemployment_rate_monthly", "M", 36)
        futures["fx_m"] = ex.submit(_safe_compat, "get_fx_rate_usd_monthly", "M", 36)
        futures["res_m"] = ex.submit(_safe_compat, "get_reserves_usd_monthly", "M", 36)
        futures["policy_m"] = ex.submit(_safe_compat, "get_policy_rate_monthly", "M", 48)

        # Annual (World Bank)
        futures["cab_pct_a"] = ex.submit(_safe_wb, "BN.CAB.XOKA.GD.ZS")      # CA % GDP
        futures["ge_a"] = ex.submit(_safe_wb, "GE.EST")                     # governance
        futures["gdp_growth_a"] = ex.submit(_safe_wb, "NY.GDP.MKTP.KD.ZG")   # GDP growth annual %
        futures["ca_level_a"] = ex.submit(_safe_wb, "BN.CAB.XOKA.CD")        # CA USD
        futures["fiscal_a"] = ex.submit(_safe_wb, "GC.BAL.CASH.GD.ZS")       # fiscal balance % GDP (often sparse)

        # If your debt service timed out, at least try WB gov debt % GDP quickly:
        # (keeps Nigeria/Mexico from returning empty debt too often)
        if not debt_series:
            futures["wb_debt_ratio"] = ex.submit(_safe_wb, "GC.DOD.TOTL.GD.ZS")

        # Collect with a hard ceiling so route doesn't block forever
        def _get(name: str, timeout: float = 4.0) -> Dict[str, float]:
            fut = futures.get(name)
            if not fut:
                return {}
            try:
                return fut.result(timeout=timeout) or {}
            except Exception:
                return {}

        gdp_growth_q = _get("gdp_growth_q", 4.0)

        cpi_m = _get("cpi_m", 4.0)
        une_m = _get("une_m", 4.0)
        fx_m = _get("fx_m", 4.0)
        res_m = _get("res_m", 4.0)
        policy_m = _get("policy_m", 4.0)

        cab_a = _get("cab_pct_a", 4.0)
        ge_a = _get("ge_a", 4.0)
        gdp_growth_a = _get("gdp_growth_a", 4.0)
        ca_level_a = _get("ca_level_a", 4.0)
        fiscal_a = _get("fiscal_a", 4.0)

        # Apply WB debt fallback if needed
        if not debt_series:
            wb_debt = _get("wb_debt_ratio", 4.0)
            if wb_debt:
                debt_series = wb_debt
                period, value = _latest(wb_debt)
                debt_latest_summary = {
                    "year": (str(period).split("-")[0] if period else None),
                    "value": value,
                    "source": "World Bank (ratio)",
                }

    # ----------------------------
    # 3) Latest extraction helper
    # ----------------------------
    def _kvl(d: Mapping[str, float]) -> Tuple[Optional[str], Optional[float]]:
        return _latest(d)

    cpi_p, cpi_v = _kvl(cpi_m)
    une_p, une_v = _kvl(une_m)
    fx_p, fx_v = _kvl(fx_m)
    res_p, res_v = _kvl(res_m)
    pol_p, pol_v = _kvl(policy_m)
    gdpq_p, gdpq_v = _kvl(gdp_growth_q)

    cab_p, cab_v = _kvl(cab_a)
    ge_p, ge_v = _kvl(ge_a)
    gdpya_p, gdpya_v = _kvl(gdp_growth_a)
    ca_lvl_p, ca_lvl_v = _kvl(ca_level_a)
    fiscal_p, fiscal_v = _kvl(fiscal_a)

    # ----------------------------
    # 4) indicators_matrix (optional, off by default for speed)
    # ----------------------------
    indicators_matrix: Dict[str, Any] = {}
    matrix_debug: Dict[str, Any] = {}
    # Keep this OFF until the matrix builder is proven fast.
    ENABLE_MATRIX = False

    if ENABLE_MATRIX:
        try:
            from app.services.indicator_service import build_country_payload_v2
            matrix_payload = _with_timeout(2.0, build_country_payload_v2, country, series="mini", keep=60) or {}
            if isinstance(matrix_payload, dict):
                indicators_matrix = matrix_payload.get("indicators_matrix") or {}
                matrix_debug = matrix_payload.get("_debug") or {}
        except Exception as e:
            matrix_debug = {"error": repr(e)}

    # ----------------------------
    # 5) Build response (ALWAYS)
    # ----------------------------
    resp: Dict[str, Any] = {
        "country": country,
        "iso_codes": iso,

        # Debt block (annual, trimmed)
        "latest": {
            "year": debt_latest_summary.get("year"),
            "value": debt_latest_summary.get("value"),
            "source": debt_latest_summary.get("source"),
        },
        "series": debt_series,
        "source": debt_latest_summary.get("source"),

        # Legacy top-levels retained (compatibility with older clients)
        "imf_data": {},
        "government_debt": debt_bundle.get("government_debt")
            or {"latest": {"value": None, "date": None, "source": None}, "series": {}},
        "nominal_gdp": debt_bundle.get("nominal_gdp")
            or {"latest": {"value": None, "date": None, "source": None}, "series": {}},
        "debt_to_gdp": debt_bundle.get("debt_to_gdp")
            or {"latest": {"value": None, "date": None, "source": None}, "series": {}},
        "debt_to_gdp_series": debt_bundle.get("debt_to_gdp_series") or debt_series,

        "indicators_matrix": indicators_matrix,

        "additional_indicators": {
            "cpi_yoy": {"latest_value": cpi_v, "latest_period": cpi_p, "source": "compat/IMF", "series": cpi_m},
            "unemployment_rate": {"latest_value": une_v, "latest_period": une_p, "source": "compat/IMF", "series": une_m},
            "fx_rate_usd": {"latest_value": fx_v, "latest_period": fx_p, "source": "compat/IMF", "series": fx_m},
            "reserves_usd": {"latest_value": res_v, "latest_period": res_p, "source": "compat/IMF", "series": res_m},
            "policy_rate": {"latest_value": pol_v, "latest_period": pol_p, "source": "compat/IMF/ECB", "series": policy_m},
            "gdp_growth": {"latest_value": gdpq_v, "latest_period": gdpq_p, "source": "compat/IMF", "series": gdp_growth_q},

            "gdp_growth_annual": {"latest_value": gdpya_v, "latest_period": gdpya_p, "source": "WB(helper/generic)", "series": gdp_growth_a},
            "current_account_balance_pct_gdp": {"latest_value": cab_v, "latest_period": cab_p, "source": "WB(helper/generic)", "series": cab_a},
            "current_account_level_usd": {"latest_value": ca_lvl_v, "latest_period": ca_lvl_p, "source": "WB(helper/generic)", "series": ca_level_a},
            "fiscal_balance_pct_gdp": {"latest_value": fiscal_v, "latest_period": fiscal_p, "source": "WB(helper/generic)", "series": fiscal_a},
            "government_effectiveness": {"latest_value": ge_v, "latest_period": ge_p, "source": "WB(helper/generic)", "series": ge_a},
        },

        "_debug": {
            "builder": "country_lite v3 (probe + parallel bounded fetches)",
            "history_policy": HIST_POLICY,
            "matrix_from_indicator_service": matrix_debug,
            "elapsed_seconds": round((_time.time() - started), 2),
        },
    }

    # ----------------------------
    # 6) Cache + return
    # ----------------------------
    try:
        _cache_set(country, resp)
    except Exception:
        pass

    logger.info("country_lite done | country=%s | elapsed=%.2fs", country, (_time.time() - started))
    return JSONResponse(content=resp)

