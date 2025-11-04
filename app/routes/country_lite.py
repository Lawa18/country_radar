# app/routes/country_lite.py
from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Tuple
import time as _time
import threading
import concurrent.futures as _fut

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

# Keep history windows identical to probe
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
    row = _COUNTRY_CACHE.get(country.lower())
    if not row:
        return None
    ts, payload = row
    if _time.time() - ts > _COUNTRY_TTL:
        return None
    return payload

def _cache_set(country: str, payload: Dict[str, Any]) -> None:
    _COUNTRY_CACHE[country.lower()] = (_time.time(), payload)

# -------------------- helpers --------------------
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

# -------------------- compat + WB fetchers --------------------
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
                except TypeError:
                    continue
                except Exception:
                    continue
    return _trim_series_policy(_coerce_numeric_series(raw), HIST_POLICY)

def _compat_fetch_retry(func_name: str, country: str, keep_hint: int) -> Dict[str, float]:
    s = _compat_fetch_series(func_name, country, keep_hint)
    if s:
        return s
    _time.sleep(0.1)
    return _compat_fetch_series(func_name, country, keep_hint)

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

def _fetch_all_parallel(country: str) -> Dict[str, Dict[str, float]]:
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
        key: _EXEC.submit(_compat_fetch_retry, func, country, keep)
        for key, (func, keep) in tasks.items()
    }
    out: Dict[str, Dict[str, float]] = {}
    for key, fut in futures.items():
        try:
            out[key] = fut.result(timeout=8.0) or {}
        except Exception:
            out[key] = {}
    # WB fallbacks if needed
    if not out.get("cab_a"):
        out["cab_a"] = _wb_fallback_series(country, "BN.CAB.XOKA.GD.ZS")
    if not out.get("ge_a"):
        out["ge_a"] = _wb_fallback_series(country, "GE.EST")
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

    # debt (sync, but usually cached in its own service)
    t_debt0 = _time.time()
    try:
        from app.services.debt_service import compute_debt_payload
        debt = compute_debt_payload(country) or {}
    except Exception:
        debt = {}
    debt_series_full = debt.get("series") or {}
    debt_series = _trim_series_policy(debt_series_full, HIST_POLICY)
    debt_latest = debt.get("latest") or {"year": None, "value": None, "source": "unavailable"}
    t_debt1 = _time.time()

    # parallel compat fetch
    t_par0 = _time.time()
    series = _fetch_all_parallel(country)
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

    payload: Dict[str, Any] = {
        "country": country,
        "iso_codes": iso,

        "latest": {"year": debt_latest.get("year"), "value": debt_latest.get("value"), "source": debt_latest.get("source")},
        "series": debt_series,
        "source": debt_latest.get("source"),

        # legacy blocks (kept empty for compatibility)
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
            "builder": "country_lite (sync + cache + parallel)",
            "history_policy": HIST_POLICY,
            "timing_ms": {
                "total": int((_time.time() - t0) * 1000),
                "debt": int((t_debt1 - t_debt0) * 1000),
                "parallel_fetch": int((t_par1 - t_par0) * 1000),
            },
            "fresh": bool(fresh),
        },
    }

    # store cache
    try:
        _cache_set(country, payload)
    except Exception:
        pass

    resp = JSONResponse(content=payload)
    resp.headers["Cache-Control"] = "public, max-age=300"
    return resp
