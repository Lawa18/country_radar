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

    try:
        # --------------------------------------------------------------
        # 0) Cache
        # --------------------------------------------------------------
        if not fresh:
            cached = _cache_get(country)
            if cached:
                logger.info("country_lite cache hit | country=%s", country)
                return JSONResponse(content=cached)

        iso = _iso_codes(country)

        # --------------------------------------------------------------
        # 1) Containers (default-safe)
        # --------------------------------------------------------------
        debt_series_full: Dict[str, float] = {}
        debt_latest_summary: Dict[str, Any] = {
            "year": None,
            "value": None,
            "source": "computed:NA/Timeout",
        }
        debt_bundle: Dict[str, Any] = {}

        # compat series defaults
        gdp_growth_q: Dict[str, float] = {}
        cpi_m: Dict[str, float] = {}
        une_m: Dict[str, float] = {}
        fx_m: Dict[str, float] = {}
        res_m: Dict[str, float] = {}
        policy_m: Dict[str, float] = {}

        # wb annual defaults
        cab_a: Dict[str, float] = {}
        ge_a: Dict[str, float] = {}
        gdp_growth_a: Dict[str, float] = {}
        ca_level_a: Dict[str, float] = {}
        fiscal_a: Dict[str, float] = {}

        indicators_matrix: Dict[str, Any] = {}
        matrix_debug: Dict[str, Any] = {}

        # --------------------------------------------------------------
        # 2) Parallel work (bounded)
        # --------------------------------------------------------------
        def _run_debt() -> Dict[str, Any]:
            try:
                from app.services.debt_service import compute_debt_payload
                return _with_timeout(2.0, compute_debt_payload, country) or {}
            except Exception as e:
                logger.warning("country_lite debt block error for %s: %r", country, e)
                return {}

        def _run_matrix() -> Dict[str, Any]:
            try:
                from app.services.indicator_service import build_country_payload_v2
                return _with_timeout(
                    3.0,
                    build_country_payload_v2,
                    country,
                    series="mini",
                    keep=60,
                ) or {}
            except Exception as e:
                return {"_debug": {"error": repr(e)}}

        def _run_compat() -> Dict[str, Any]:
            # NOTE: These functions already do retries; keep hints bound the history.
            return {
                "gdp_growth_q": _compat_fetch_series_retry("get_gdp_growth_quarterly", country, "Q", keep_hint=12),
                "cpi_m": _compat_fetch_series_retry("get_cpi_yoy_monthly", country, "M", keep_hint=36),
                "une_m": _compat_fetch_series_retry("get_unemployment_rate_monthly", country, "M", keep_hint=36),
                "fx_m": _compat_fetch_series_retry("get_fx_rate_usd_monthly", country, "M", keep_hint=36),
                "res_m": _compat_fetch_series_retry("get_reserves_usd_monthly", country, "M", keep_hint=36),
                "policy_m": _compat_fetch_series_retry("get_policy_rate_monthly", country, "M", keep_hint=48),
            }

        def _run_wb() -> Dict[str, Any]:
            cab = _wb_series_from_helpers(country, "wb_current_account_balance_pct_gdp_annual") or _wb_series_generic(country, "BN.CAB.XOKA.GD.ZS")
            ge = _wb_series_from_helpers(country, "wb_government_effectiveness_annual") or _wb_series_generic(country, "GE.EST")
            return {
                "cab_a": cab,
                "ge_a": ge,
                "gdp_growth_a": _wb_series_generic(country, "NY.GDP.MKTP.KD.ZG"),
                "ca_level_a": _wb_series_generic(country, "BN.CAB.XOKA.CD"),
                "fiscal_a": _wb_series_generic(country, "GC.BAL.CASH.GD.ZS"),
            }

        with _futures.ThreadPoolExecutor(max_workers=4) as ex:
            fut_debt = ex.submit(_run_debt)
            fut_matrix = ex.submit(_run_matrix)
            fut_compat = ex.submit(_run_compat)
            fut_wb = ex.submit(_run_wb)

            # Each section has its own cap. If it times out, we keep defaults.
            try:
                bundle = fut_debt.result(timeout=2.2)
            except Exception:
                bundle = {}

            try:
                matrix_payload = fut_matrix.result(timeout=3.2)
            except Exception:
                matrix_payload = {}

            try:
                compat_payload = fut_compat.result(timeout=5.0)
            except Exception:
                compat_payload = {}

            try:
                wb_payload = fut_wb.result(timeout=5.0)
            except Exception:
                wb_payload = {}

        # --------------------------------------------------------------
        # 3) Debt normalization (same logic you had)
        # --------------------------------------------------------------
        if isinstance(bundle, Mapping):
            debt_bundle = dict(bundle)

            debt_block = bundle.get("debt_to_gdp") or {}
            series = debt_block.get("series") or bundle.get("debt_to_gdp_series") or {}

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

        # --------------------------------------------------------------
        # 4) Unpack compat + wb payloads
        # --------------------------------------------------------------
        if isinstance(compat_payload, dict):
            gdp_growth_q = compat_payload.get("gdp_growth_q") or {}
            cpi_m = compat_payload.get("cpi_m") or {}
            une_m = compat_payload.get("une_m") or {}
            fx_m = compat_payload.get("fx_m") or {}
            res_m = compat_payload.get("res_m") or {}
            policy_m = compat_payload.get("policy_m") or {}

        if isinstance(wb_payload, dict):
            cab_a = wb_payload.get("cab_a") or {}
            ge_a = wb_payload.get("ge_a") or {}
            gdp_growth_a = wb_payload.get("gdp_growth_a") or {}
            ca_level_a = wb_payload.get("ca_level_a") or {}
            fiscal_a = wb_payload.get("fiscal_a") or {}

        # --------------------------------------------------------------
        # 5) Latest extraction (same as yours)
        # --------------------------------------------------------------
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

        # --------------------------------------------------------------
        # 6) Matrix extraction (same)
        # --------------------------------------------------------------
        if isinstance(matrix_payload, dict):
            indicators_matrix = matrix_payload.get("indicators_matrix") or {}
            matrix_debug = matrix_payload.get("_debug") or {}

        # --------------------------------------------------------------
        # 7) Response (same schema as your current one, plus your extra annuals)
        # --------------------------------------------------------------
        resp: Dict[str, Any] = {
            "country": country,
            "iso_codes": iso,

            "latest": {
                "year": debt_latest_summary.get("year"),
                "value": debt_latest_summary.get("value"),
                "source": debt_latest_summary.get("source"),
            },
            "series": debt_series,
            "source": debt_latest_summary.get("source"),

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

                "current_account_balance_pct_gdp": {"latest_value": cab_v, "latest_period": cab_p, "source": "WB(helper/generic)", "series": cab_a},
                "government_effectiveness": {"latest_value": ge_v, "latest_period": ge_p, "source": "WB(helper/generic)", "series": ge_a},

                # The three annuals you added:
                "gdp_growth_annual": {"latest_value": gdpya_v, "latest_period": gdpya_p, "source": "WB(helper/generic)", "series": gdp_growth_a},
                "current_account_level_usd": {"latest_value": ca_lvl_v, "latest_period": ca_lvl_p, "source": "WB(helper/generic)", "series": ca_level_a},
                "fiscal_balance_pct_gdp": {"latest_value": fiscal_v, "latest_period": fiscal_p, "source": "WB(helper/generic)", "series": fiscal_a},
            },

            "_debug": {
                "builder": "country_lite v3 (probe + parallel bounded fetches)",
                "history_policy": HIST_POLICY,
                "matrix_from_indicator_service": matrix_debug,
                "elapsed_seconds": round(_time.time() - started, 2),
            },
        }

        # cache best-effort
        try:
            _cache_set(country, resp)
        except Exception:
            pass

        return JSONResponse(content=resp)

    finally:
        elapsed = _time.time() - started
        logger.info("country_lite done | country=%s | elapsed=%.2fs", country, elapsed)
