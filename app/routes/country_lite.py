# app/routes/country_lite.py
from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Tuple
import time as _time
import threading
import concurrent.futures as _fut
from datetime import date as _date

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

HIST_POLICY = {"A": 20, "Q": 4, "M": 12}

router = APIRouter(tags=["country-lite"])

# -------------------- tiny cache w/ stampede guard --------------------
_COUNTRY_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_COUNTRY_TTL = 600.0  # 10 minutes
_LOCKS: Dict[str, threading.Lock] = {}
_GLOBAL_LOCK = threading.Lock()


def _get_lock(key: str) -> threading.Lock:
    with _GLOBAL_LOCK:
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


def _safe_import(module: str):
    try:
        return __import__(module, fromlist=["*"])
    except Exception:
        return None


def _coerce_numeric_series(d: Optional[Mapping[str, Any]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not isinstance(d, Mapping):
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
            y, q = s.split("-Q", 1)
            return (int(y), 0, int(q))
        if "-" in s:
            y, m = s.split("-", 1)
            return (int(y), int(m), 0)
        return (int(s), 0, 0)
    except Exception:
        return (0, 0, 0)


def _latest(d: Mapping[str, float]) -> Tuple[Optional[str], Optional[float]]:
    if not d:
        return None, None
    ks = sorted(d.keys(), key=_parse_period_key)
    k = ks[-1]
    return k, d[k]


def _freq_of_key(k: str) -> str:
    s = str(k)
    if "-Q" in s:
        return "Q"
    if "-" in s:
        parts = s.split("-")
        if len(parts) >= 2 and parts[0].isdigit():
            return "M"
    return "A"


def _trim_series_policy(series: Mapping[str, float], policy: Dict[str, int]) -> Dict[str, float]:
    if not series:
        return {}
    buckets: Dict[str, Dict[str, float]] = {"A": {}, "Q": {}, "M": {}}
    for k, v in series.items():
        try:
            buckets[_freq_of_key(k)][str(k)] = float(v)
        except Exception:
            continue
    out: Dict[str, float] = {}
    for f, sub in buckets.items():
        if not sub:
            continue
        keep = policy.get(f, 0)
        ordered = sorted(sub.items(), key=lambda kv: _parse_period_key(kv[0]))
        take = ordered[-keep:] if keep > 0 else ordered
        out.update(dict(take))
    return dict(sorted(out.items(), key=lambda kv: _parse_period_key(kv[0])))

# -------------------- recency helpers --------------------
def _period_to_date(period: Optional[str]) -> _date:
    """Convert a period like "YYYY", "YYYY-MM" or "YYYY-Qn" into a date.

    We only care about relative age in months, not exact day.
    """
    if not period:
        # Something obviously old so it fails freshness checks
        return _date(1900, 1, 1)
    s = str(period)

    # Quarterly form: 'YYYY-Qn'
    if "-Q" in s:
        try:
            year_str, q_str = s.split("-Q", 1)
            y = int(year_str)
            q = int(q_str)
            month = (q - 1) * 3 + 2  # Q1->Feb, Q2->May, etc.
            month = max(1, min(12, month))
            return _date(y, month, 1)
        except Exception:
            return _date(1900, 1, 1)

    parts = s.split("-")
    try:
        if len(parts) == 2:
            y, m = map(int, parts)
            m = max(1, min(12, m))
            return _date(y, m, 1)
        # plain 'YYYY'
        y = int(s)
        return _date(y, 1, 1)
    except Exception:
        return _date(1900, 1, 1)


def _is_recent_period(
    period: Optional[str],
    *,
    max_age_months: Optional[int] = None,
    max_age_years: Optional[int] = None,
    today: Optional[_date] = None,
) -> bool:
    """Return True if the given period is recent enough.

    If neither max_age_months nor max_age_years is provided, the period is
    treated as always fresh.
    """
    if period is None:
        return False

    d = _period_to_date(period)
    today = today or _date.today()

    total_today = today.year * 12 + today.month
    total_d = d.year * 12 + d.month
    diff_months = total_today - total_d

    if max_age_months is not None:
        return diff_months <= max_age_months
    if max_age_years is not None:
        return diff_months <= max_age_years * 12

    return True


def _iso_codes(country: str) -> Dict[str, Optional[str]]:
    try:
        cc_mod = _safe_import("app.utils.country_codes")
        if cc_mod and hasattr(cc_mod, "get_country_codes"):
            codes = cc_mod.get_country_codes(country)
            if isinstance(codes, Mapping):
                return {
                    "name": str(codes.get("name") or country),
                    "iso_alpha_2": codes.get("iso_alpha_2") or codes.get("alpha2") or codes.get("iso2"),
                    "iso_alpha_3": codes.get("iso_alpha_3") or codes.get("alpha3") or codes.get("iso3"),
                    "iso_numeric": codes.get("iso_numeric") or codes.get("numeric"),
                }
    except Exception:
        pass
    return {"name": country, "iso_alpha_2": None, "iso_alpha_3": None, "iso_numeric": None}

# -------------------- compat + IMF + WB fetchers --------------------
def _compat_fetch_series(func_name: str, country: str, keep_hint: int) -> Dict[str, float]:
    mod = _safe_import("app.providers.compat")
    raw: Mapping[str, Any] = {}
    if mod:
        fn = getattr(mod, func_name, None)
        if callable(fn):
            for kwargs in (
                {"country": country, "series": "mini", "keep": max(keep_hint, 24)},
                {"country": country, "series": "full"},
                {"country": country},
            ):
                try:
                    raw = fn(**kwargs) or {}
                    if raw:
                        break
                except Exception:
                    continue
    return _coerce_numeric_series(raw)


def _imf_fetch_series(func_name: str, country: str) -> Dict[str, float]:
    mod = _safe_import("app.providers.imf_provider")
    if not mod:
        return {}
    fn = getattr(mod, func_name, None)
    if not callable(fn):
        return {}
    try:
        raw = fn(country)
        return _coerce_numeric_series(raw)
    except Exception:
        return {}


def _wb_fallback_series(country: str, indicator_code: str) -> Dict[str, float]:
    try:
        wb = _safe_import("app.providers.wb_provider")
        if not wb:
            return {}
        fetch = getattr(wb, "fetch_wb_indicator_raw", None)
        to_year = getattr(wb, "wb_year_dict_from_raw", None)
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


# Thread pool for parallel fetch
_EXEC = _fut.ThreadPoolExecutor(max_workers=8)
_PER_TASK_TIMEOUT = 3.0  # seconds, keep low to avoid 17s total waits


def _fetch_all_parallel(country: str, timing: Dict[str, int]) -> Dict[str, Dict[str, float]]:
    def timed(label: str, fn):
        t0 = _time.time()
        res = fn()
        timing[label] = int((_time.time() - t0) * 1000)
        return res

    # 1) primary compat submits
    tasks = {
        # Monthly (12)
        "cpi_m":    ("get_cpi_yoy_monthly", 24),
        "une_m":    ("get_unemployment_rate_monthly", 24),
        "fx_m":     ("get_fx_rate_usd_monthly", 24),
        "res_m":    ("get_reserves_usd_monthly", 24),
        "policy_m": ("get_policy_rate_monthly", 36),
        # Quarterly (4)
        "gdp_q":    ("get_gdp_growth_quarterly", 8),
        # Annual (20)
        "cab_a":    ("get_current_account_balance_pct_gdp", 40),
        "ge_a":     ("get_government_effectiveness", 40),
    }
    futures = {
        key: _EXEC.submit(_compat_fetch_series, func, country, keep)
        for key, (func, keep) in tasks.items()
    }

    out: Dict[str, Dict[str, float]] = {}
    for key, fut in futures.items():
        try:
            out[key] = fut.result(timeout=_PER_TASK_TIMEOUT) or {}
        except Exception:
            out[key] = {}

    # 2) IMF direct fallbacks for key gaps
    # CPI YoY fallback: direct IMF yoy, else IMF index → compute yoy
    if not out.get("cpi_m"):
        # try direct IMF YoY
        imf_yoy = timed("imf_cpi_yoy", lambda: _imf_fetch_series("get_cpi_yoy_monthly", country))
        if imf_yoy:
            out["cpi_m"] = imf_yoy
        else:
            # try IMF CPI index and compute YoY
            imf_idx = timed("imf_cpi_index", lambda: _imf_fetch_series("get_cpi_index_monthly", country))
            if imf_idx:
                out["cpi_m"] = _yoy_from_index(imf_idx)

    # Unemployment fallback: IMF direct monthly
    if not out.get("une_m"):
        out["une_m"] = timed("imf_unemployment", lambda: _imf_fetch_series("get_unemployment_rate_monthly", country))

    # Quarterly GDP growth fallback: IMF direct
    if not out.get("gdp_q"):
        out["gdp_q"] = timed("imf_gdp_q", lambda: _imf_fetch_series("get_gdp_growth_quarterly", country))

    # WB fallbacks for annuals
    if not out.get("cab_a"):
        out["cab_a"] = timed("wb_cab", lambda: _wb_fallback_series(country, "BN.CAB.XOKA.GD.ZS"))
    if not out.get("ge_a"):
        out["ge_a"] = timed("wb_ge", lambda: _wb_fallback_series(country, "GE.EST"))

    # Ensure all keys exist
    for k in ("cpi_m","une_m","fx_m","res_m","policy_m","gdp_q","cab_a","ge_a"):
        out.setdefault(k, {})

    return out


def _yoy_from_index(index_series: Mapping[str, float]) -> Dict[str, float]:
    """Compute YoY % change from an index-like series."""
    if not index_series:
        return {}
    keys = sorted(index_series.keys(), key=_parse_period_key)
    out: Dict[str, float] = {}
    for i, k in enumerate(keys):
        if i < 12:
            continue
        k_prev = keys[i - 12]
        try:
            v = (index_series[k] / index_series[k_prev] - 1.0) * 100.0
            out[str(k)] = float(v)
        except Exception:
            continue
    return out

# -------------------- route --------------------
@router.get("/v1/country-lite", summary="Country Lite")
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

    # debt (sync)
    t_debt0 = _time.time()
    try:
        from app.services.debt_service import compute_debt_payload
        debt = compute_debt_payload(country) or {}
    except Exception:
        debt = {}
    debt_series_full = debt.get("series") or {}
    debt_series = _trim_series_policy(debt_series_full, HIST_POLICY)
    debt_latest = debt.get("latest") or {"year": None, "value": None, "source": "unavailable"}
    # Recency filter for debt: drop very old debt-to-GDP points
    debt_year = debt_latest.get("year")
    try:
        debt_year_str = str(debt_year) if debt_year is not None else None
    except Exception:
        debt_year_str = None
    if debt_year_str and not _is_recent_period(debt_year_str, max_age_years=5):
        # Treat as unavailable if older than the recency window
        debt_series = {}
        debt_latest = {"year": None, "value": None, "source": debt_latest.get("source")}

    t_debt1 = _time.time()

    # parallel compat + fallbacks
    t_par0 = _time.time()
    timing_by_key: Dict[str, int] = {}
    series = _fetch_all_parallel(country, timing_by_key)
    t_par1 = _time.time()

    def _kvl(d: Mapping[str, float]) -> Tuple[Optional[str], Optional[float]]:
        return _latest(d)

    cpi_p, cpi_v   = _kvl(series["cpi_m"])
    une_p, une_v   = _kvl(series["une_m"])
    fx_p, fx_v     = _kvl(series["fx_m"])
    res_p, res_v   = _kvl(series["res_m"])
    pol_p, pol_v   = _kvl(series["policy_m"])
    gdpq_p, gdpq_v = _kvl(series["gdp_q"])
    cab_p, cab_v   = _kvl(series["cab_a"])
    ge_p, ge_v     = _kvl(series["ge_a"])

    # Recency filters for macro indicators: avoid surfacing very old data
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

    payload: Dict[str, Any] = {
        "country": country,
        "iso_codes": iso,

        "latest": {"year": debt_latest.get("year"), "value": debt_latest.get("value"), "source": debt_latest.get("source")},
        "series": debt_series,
        "source": debt_latest.get("source"),

        # legacy blocks
        "imf_data": {},
        "government_debt": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
        "nominal_gdp": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
        "debt_to_gdp": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
        "debt_to_gdp_series": {},

        "additional_indicators": {
            "cpi_yoy": {"latest_value": cpi_v, "latest_period": cpi_p, "source": "compat/IMF", "series": series["cpi_m"]},
            "unemployment_rate": {"latest_value": une_v, "latest_period": une_p, "source": "compat/IMF", "series": series["une_m"]},
            "fx_rate_usd": {"latest_value": fx_v, "latest_period": fx_p, "source": "compat/IMF", "series": series["fx_m"]},
            "reserves_usd": {"latest_value": res_v, "latest_period": res_p, "source": "compat/IMF", "series": series["res_m"]},
            "policy_rate": {"latest_value": pol_v, "latest_period": pol_p, "source": "compat/IMF/ECB", "series": series["policy_m"]},
            "gdp_growth": {"latest_value": gdpq_v, "latest_period": gdpq_p, "source": "compat/IMF", "series": series["gdp_q"]},
            "current_account_balance_pct_gdp": {"latest_value": cab_v, "latest_period": cab_p, "source": "compat/WB", "series": series["cab_a"]},
            "government_effectiveness": {"latest_value": ge_v, "latest_period": ge_p, "source": "compat/WB WGI", "series": series["ge_a"]},
        },

        "_debug": {
            "builder": "country_lite (sync + cache + parallel + imf_fallbacks)",
            "history_policy": HIST_POLICY,
            "timing_ms": {
                "total": int((_time.time() - t0) * 1000),
                "debt": int((t_debt1 - t_debt0) * 1000),
                "parallel_fetch": int((t_par1 - t_par0) * 1000),
            },
            "timing_ms_by_key": timing_by_key,
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
