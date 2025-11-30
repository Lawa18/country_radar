# app/routes/country_lite.py
from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Tuple
import time as _time
import threading
import concurrent.futures as _fut
from datetime import date as _date

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

# History policy for trimming series
HIST_POLICY = {"A": 20, "Q": 4, "M": 12}

router = APIRouter(tags=["country-lite"])

# -----------------------------------------------------------------------------
# Simple in-memory cache
# -----------------------------------------------------------------------------
_COUNTRY_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_COUNTRY_TTL = 300.0  # seconds
_LOCKS: Dict[str, threading.Lock] = {}
_LOCKS_GLOBAL = threading.Lock()


def _get_lock(key: str) -> threading.Lock:
    with _LOCKS_GLOBAL:
        lk = _LOCKS.get(key)
        if lk is None:
            lk = threading.Lock()
            _LOCKS[key] = lk
        return lk


def _cache_get(country: str) -> Optional[Dict[str, Any]]:
    lk = _get_lock(country)
    with lk:
        row = _COUNTRY_CACHE.get(country)
        if not row:
            return None
        ts, payload = row
        if _time.time() - ts > _COUNTRY_TTL:
            try:
                del _COUNTRY_CACHE[country]
            except Exception:
                pass
            return None
        return payload


def _cache_set(country: str, payload: Dict[str, Any]) -> None:
    lk = _get_lock(country)
    with lk:
        _COUNTRY_CACHE[country] = (_time.time(), payload)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _safe_import(module: str):
    try:
        return __import__(module, fromlist=["*"])
    except Exception:
        return None


def _coerce_numeric_series(d: Optional[Mapping[str, Any]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not d or not isinstance(d, Mapping):
        return out
    for k, v in d.items():
        try:
            out[str(k)] = float(v)
        except Exception:
            pass
    return out


def _parse_period_key(p: str) -> Tuple[int, int, int]:
    try:
        if isinstance(p, (int, float)):
            return (int(p), 0, 0)
        s = str(p)
        if "-Q" in s:
            year_str, q_str = s.split("-Q", 1)
            return (int(year_str), 0, int(q_str))
        if "-" in s:
            year_str, month_str = s.split("-", 1)
            return (int(year_str), int(month_str), 0)
        return (int(s), 0, 0)
    except Exception:
        return (0, 0, 0)


def _latest(d: Mapping[str, float]) -> Tuple[Optional[str], Optional[float]]:
    if not d:
        return None, None
    keys = sorted(d.keys(), key=_parse_period_key)
    k = keys[-1]
    return k, d[k]


def _freq_of_key(k: str) -> str:
    if "-Q" in k:
        return "Q"
    if "-" in k:
        return "M"
    return "A"


def _trim_series_policy(series: Mapping[str, float], policy: Dict[str, int]) -> Dict[str, float]:
    """Trim annual / quarterly / monthly series according to HIST_POLICY."""
    if not series:
        return {}
    keys = sorted(series.keys(), key=_parse_period_key)

    buckets: Dict[str, Dict[str, float]] = {"A": {}, "Q": {}, "M": {}}
    for k in keys:
        freq = _freq_of_key(k)
        if freq not in buckets:
            freq = "A"
        buckets[freq][k] = float(series[k])

    trimmed: Dict[str, float] = {}
    for freq, ser in buckets.items():
        keep = policy.get(freq, len(ser))
        ks = sorted(ser.keys(), key=_parse_period_key)
        if len(ks) <= keep:
            trimmed.update({k: ser[k] for k in ks})
        else:
            trimmed.update({k: ser[k] for k in ks[-keep:]})

    return trimmed


def _period_to_date(period: Optional[str]) -> _date:
    if not period:
        return _date(1900, 1, 1)
    s = str(period)
    if "-Q" in s:
        try:
            year_str, q_str = s.split("-Q", 1)
            y = int(year_str)
            q = int(q_str)
            m = (q - 1) * 3 + 2  # mid-quarter
            m = max(1, min(12, m))
            return _date(y, m, 1)
        except Exception:
            return _date(1900, 1, 1)
    parts = s.split("-")
    try:
        if len(parts) == 2:
            y = int(parts[0])
            m = int(parts[1])
            m = max(1, min(12, m))
            return _date(y, m, 1)
        if len(parts) == 1:
            y = int(parts[0])
            return _date(y, 1, 1)
    except Exception:
        return _date(1900, 1, 1)
    return _date(1900, 1, 1)


def _is_recent_period(
    period: Optional[str],
    *,
    max_age_months: Optional[int] = None,
    max_age_years: Optional[int] = None,
    today: Optional[_date] = None,
) -> bool:
    if period is None:
        return False
    d = _period_to_date(period)
    today = today or _date.today()
    total_today = today.year * 12 + today.month
    total_d = d.year * 12 + d.month

    if max_age_months is not None and (total_today - total_d) > max_age_months:
        return False
    if max_age_years is not None and (total_today - total_d) > (max_age_years * 12):
        return False
    return True


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
        return {"name": country, "iso_alpha_2": None, "iso_alpha_3": None, "iso_numeric": None}


def _compat_fetch_series(func_name: str, country: str, keep_hint: int) -> Dict[str, float]:
    try:
        mod = _safe_import("app.providers.compat")
        if not mod:
            return {}
        fn = getattr(mod, func_name, None)
        if not callable(fn):
            return {}
        raw = fn(country, keep=keep_hint)
        series = _coerce_numeric_series(raw)
        return _trim_series_policy(series, HIST_POLICY)
    except Exception:
        return {}


def _imf_fetch_series(func_name: str, country: str) -> Dict[str, float]:
    try:
        mod = _safe_import("app.providers.imf_provider")
        if not mod:
            return {}
        fn = getattr(mod, func_name, None)
        if not callable(fn):
            return {}
        from app.utils.country_codes import get_country_codes

        codes = get_country_codes(country) or {}
        iso2 = codes.get("iso_alpha_2")
        if not iso2:
            return {}
        raw = fn(iso2=iso2)
        series = _coerce_numeric_series(raw)
        return _trim_series_policy(series, HIST_POLICY)
    except Exception:
        return {}


def _wb_fallback_series(country: str, indicator_code: str) -> Dict[str, float]:
    """
    Fallback path for some annual indicators via WB WDI helpers.
    """
    try:
        mod = _safe_import("app.providers.wb_provider")
        if not mod:
            return {}
        fetch = getattr(mod, "fetch_wb_indicator_raw", None)
        to_year = getattr(mod, "wb_year_dict_from_raw", None)
        if not callable(fetch) or not callable(to_year):
            return {}
        from app.utils.country_codes import get_country_codes

        codes = get_country_codes(country) or {}
        iso3 = codes.get("iso_alpha_3")
        if not iso3:
            return {}
        raw = fetch(iso3, indicator_code)
        series = _coerce_numeric_series(to_year(raw))
        return _trim_series_policy(series, HIST_POLICY)
    except Exception:
        return {}


# Thread pool for parallel compat/IMF/WB fetch
_EXEC = _fut.ThreadPoolExecutor(max_workers=8)
_PER_TASK_TIMEOUT = 3.0  # seconds


def _fetch_all_parallel(country: str, timing: Dict[str, int]) -> Dict[str, Dict[str, float]]:
    def timed(label: str, fn):
        t0 = _time.time()
        res = fn()
        timing[label] = int((_time.time() - t0) * 1000)
        return res

    tasks = {
        # Monthly (12)
        "cpi_m": ("get_cpi_yoy_monthly", 24),
        "une_m": ("get_unemployment_rate_monthly", 24),
        "fx_m": ("get_fx_to_usd_monthly", 24),
        "res_m": ("get_fx_reserves_usd_monthly", 36),
        "policy_m": ("get_policy_rate_monthly", 24),
        # Quarterly (4)
        "gdp_q": ("get_gdp_growth_quarterly", 8),
        # Annual (20)
        "cab_a": ("get_current_account_balance_pct_gdp_annual", 20),
        "ge_a": ("get_government_effectiveness_annual", 20),
    }

    results: Dict[str, Dict[str, float]] = {k: {} for k in tasks.keys()}
    futs: Dict[str, _fut.Future] = {}

    for key, (func_name, keep_hint) in tasks.items():
        futs[key] = _EXEC.submit(
            timed,
            key,
            lambda fn=func_name, kh=keep_hint: _compat_fetch_series(fn, country, keep_hint=kh),
        )

    # IMF fallbacks if compat empty
    imf_fallbacks = {
        "cpi_m": "imf_cpi_yoy_monthly",
        "une_m": "imf_unemployment_rate_monthly",
        "fx_m": "imf_fx_to_usd_monthly",
        "res_m": "imf_fx_reserves_usd_monthly",
        "policy_m": "imf_policy_rate_monthly",
        "gdp_q": "imf_gdp_growth_quarterly",
    }

    for key, fut in futs.items():
        try:
            _ = fut.result(timeout=_PER_TASK_TIMEOUT)
        except Exception:
            pass

    for key in tasks.keys():
        try:
            series = futs[key].result(timeout=0.0)
        except Exception:
            series = {}
        if not series:
            fb_name = imf_fallbacks.get(key)
            if fb_name:
                series = _imf_fetch_series(fb_name, country)
        results[key] = series

    # WB fallbacks for CA%GDP and gov effectiveness
    if not results["cab_a"]:
        wb_cab = _wb_fallback_series(country, "BN.CAB.XOKA.GD.ZS")
        if wb_cab:
            results["cab_a"] = wb_cab

    if not results["ge_a"]:
        wb_ge = _wb_fallback_series(country, "GE.EST")
        if wb_ge:
            results["ge_a"] = wb_ge

    for k in list(results.keys()):
        if not isinstance(results[k], Mapping):
            results[k] = {}

    return results


# -----------------------------------------------------------------------------
# Route: /v1/country-lite
# -----------------------------------------------------------------------------
@router.get("/v1/country-lite")
def country_lite(
    country: str = Query(..., description="Full country name, e.g., Mexico"),
    fresh: bool = Query(False, description="Bypass cache if true"),
) -> JSONResponse:
    t0 = _time.time()

    if not fresh:
        cached = _cache_get(country)
        if cached:
            resp = JSONResponse(content=cached)
            resp.headers["Cache-Control"] = "public, max-age=300"
            return resp

    iso = _iso_codes(country)

    # -------------------------------------------------------------------------
    # Debt block (sync)
    # -------------------------------------------------------------------------
    t_debt0 = _time.time()
    try:
        from app.services.debt_service import compute_debt_payload

        debt = compute_debt_payload(country) or {}
    except Exception:
        debt = {}
    debt_series_full = debt.get("series") or {}
    debt_series = _trim_series_policy(debt_series_full, HIST_POLICY)
    debt_latest = debt.get("latest") or {"year": None, "value": None, "source": "unavailable"}

    debt_year = debt_latest.get("year")
    try:
        debt_year_str = str(debt_year) if debt_year is not None else None
    except Exception:
        debt_year_str = None
    if debt_year_str and not _is_recent_period(debt_year_str, max_age_years=5):
        debt_series = {}
        debt_latest = {"year": None, "value": None, "source": debt_latest.get("source")}

    t_debt1 = _time.time()

    # -------------------------------------------------------------------------
    # Parallel macro fetch (compat + IMF + WB helpers)
    # -------------------------------------------------------------------------
    t_par0 = _time.time()
    timing_by_key: Dict[str, int] = {}
    series = _fetch_all_parallel(country, timing_by_key)
    t_par1 = _time.time()

    def _kvl(d: Mapping[str, float]) -> Tuple[Optional[str], Optional[float]]:
        return _latest(d)

    cpi_p, cpi_v = _kvl(series["cpi_m"])
    une_p, une_v = _kvl(series["une_m"])
    fx_p, fx_v = _kvl(series["fx_m"])
    res_p, res_v = _kvl(series["res_m"])
    pol_p, pol_v = _kvl(series["policy_m"])
    gdpq_p, gdpq_v = _kvl(series["gdp_q"])
    cab_p, cab_v = _kvl(series["cab_a"])
    ge_p, ge_v = _kvl(series["ge_a"])

    _now = _date.today()
    if cpi_p is not None and not _is_recent_period(cpi_p, max_age_months=6, today=_now):
        series["cpi_m"] = {}
        cpi_p, cpi_v = None, None
    if une_p is not None and not _is_recent_period(une_p, max_age_months=12, today=_now):
        series["une_m"] = {}
        une_p, une_v = None, None
    if fx_p is not None and not _is_recent_period(fx_p, max_age_months=3, today=_now):
        series["fx_m"] = {}
        fx_p, fx_v = None, None
    if res_p is not None and not _is_recent_period(res_p, max_age_months=12, today=_now):
        series["res_m"] = {}
        res_p, res_v = None, None
    if pol_p is not None and not _is_recent_period(pol_p, max_age_months=6, today=_now):
        series["policy_m"] = {}
        pol_p, pol_v = None, None
    if gdpq_p is not None and not _is_recent_period(gdpq_p, max_age_months=6, today=_now):
        series["gdp_q"] = {}
        gdpq_p, gdpq_v = None, None
    if cab_p is not None and not _is_recent_period(cab_p, max_age_years=3, today=_now):
        series["cab_a"] = {}
        cab_p, cab_v = None, None
    if ge_p is not None and not _is_recent_period(ge_p, max_age_years=4, today=_now):
        series["ge_a"] = {}
        ge_p, ge_v = None, None

    # -------------------------------------------------------------------------
    # NEW: indicators_matrix from indicator_service (non-fatal)
    # -------------------------------------------------------------------------
    indicators_matrix: Dict[str, Any] = {}
    matrix_debug: Dict[str, Any] = {}
    try:
        from app.services.indicator_service import build_country_payload_v2

        matrix_payload = build_country_payload_v2(country=country, series="mini", keep=60)
        if isinstance(matrix_payload, dict):
            indicators_matrix = matrix_payload.get("indicators_matrix") or {}
            matrix_debug = matrix_payload.get("_debug") or {}
    except Exception as e:
        matrix_debug = {"error": repr(e)}

    # -------------------------------------------------------------------------
    # Final payload
    # -------------------------------------------------------------------------
    payload: Dict[str, Any] = {
        "country": country,
        "iso_codes": iso,

        "latest": {
            "year": debt_latest.get("year"),
            "value": debt_latest.get("value"),
            "source": debt_latest.get("source"),
        },
        "series": debt_series,
        "source": debt_latest.get("source"),

        # Keep legacy debt-related keys (may not be fully populated here)
        "imf_data": {},
        "government_debt": {
            "latest": {"value": None, "date": None, "source": None},
            "series": {},
        },
        "nominal_gdp": {
            "latest": {"value": None, "date": None, "source": None},
            "series": {},
        },
        "debt_to_gdp": {
            "latest": {"value": None, "date": None, "source": None},
            "series": {},
        },
        "debt_to_gdp_series": {},

        # NEW: matrix-based indicators block
        "indicators_matrix": indicators_matrix,

        # Existing macro indicators
        "additional_indicators": {
            "cpi_yoy": {
                "latest_value": cpi_v,
                "latest_period": cpi_p,
                "source": "compat/IMF",
                "series": series["cpi_m"],
            },
            "unemployment_rate": {
                "latest_value": une_v,
                "latest_period": une_p,
                "source": "compat/IMF",
                "series": series["une_m"],
            },
            "fx_rate_usd": {
                "latest_value": fx_v,
                "latest_period": fx_p,
                "source": "compat/IMF",
                "series": series["fx_m"],
            },
            "reserves_usd": {
                "latest_value": res_v,
                "latest_period": res_p,
                "source": "compat/IMF",
                "series": series["res_m"],
            },
            "policy_rate": {
                "latest_value": pol_v,
                "latest_period": pol_p,
                "source": "compat/IMF/ECB",
                "series": series["policy_m"],
            },
            "gdp_growth": {
                "latest_value": gdpq_v,
                "latest_period": gdpq_p,
                "source": "compat/IMF",
                "series": series["gdp_q"],
            },
            "current_account_balance_pct_gdp": {
                "latest_value": cab_v,
                "latest_period": cab_p,
                "source": "compat/WB",
                "series": series["cab_a"],
            },
            "government_effectiveness": {
                "latest_value": ge_v,
                "latest_period": ge_p,
                "source": "compat/WB WGI",
                "series": series["ge_a"],
            },
        },

        "_debug": {
            "builder": "country_lite v3 (sync + cache + parallel + imf_fallbacks + matrix)",
            "code_version": "clite_v3_matrix_2025-11-29",
            "history_policy": HIST_POLICY,
            "timing_ms": {
                "total": int((_time.time() - t0) * 1000),
                "debt": int((t_debt1 - t_debt0) * 1000),
                "parallel_fetch": int((t_par1 - t_par0) * 1000),
            },
            "timing_ms_by_key": timing_by_key,
            "matrix_from_indicator_service": matrix_debug,
            "fresh": bool(fresh),
            "timeouts": {"per_task_seconds": _PER_TASK_TIMEOUT},
        },
    }

    try:
        _cache_set(country, payload)
    except Exception:
        pass

    resp = JSONResponse(content=payload)
    resp.headers["Cache-Control"] = "public, max-age=300"
    return resp
