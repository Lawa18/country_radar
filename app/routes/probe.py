# app/routes/probe.py — diagnostics + lightweight country info (stable + cached)
from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple
import inspect
import time as _time

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse, Response

router = APIRouter(tags=["probe"])

# -----------------------------------------------------------------------------
# History policy + compat helpers
# -----------------------------------------------------------------------------
HIST_POLICY = {"A": 20, "Q": 4, "M": 12}  # Annual, Quarterly, Monthly window sizes

# --- tiny response cache for /v1/country-lite --------------------------------
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
# Low-level utilities (defensive: never raise in probes)
# -----------------------------------------------------------------------------
def _safe_import(module: str):
    try:
        return __import__(module, fromlist=["*"])
    except Exception:
        return None

def _coerce_numeric_series(d: Optional[Mapping[str, Any]]) -> Dict[str, float]:
    """Keep only numeric values; keys as strings; ignore junk."""
    out: Dict[str, float] = {}
    if not isinstance(d, Mapping):
        return out
    for k, v in d.items():
        try:
            out[str(k)] = float(v)
        except Exception:
            # ignore non-numeric
            pass
    return out

def _parse_period_key(p: str) -> Tuple[int, int, int]:
    """
    Sort keys like 'YYYY', 'YYYY-MM', 'YYYY-Qn' robustly.
    Returns (year, month, quarter) for sorting.
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

def _latest(d: Mapping[str, float]) -> Tuple[Optional[str], Optional[float]]:
    if not d:
        return None, None
    ks = sorted(d.keys(), key=_parse_period_key)
    k = ks[-1]
    return k, d[k]

def _to_annual_latest(d: Mapping[str, float]) -> Dict[str, float]:
    """Collapse to annual by taking latest period per year (for display/length)."""
    if not d:
        return {}
    by_year: Dict[str, Tuple[str, float]] = {}
    for k, v in d.items():
        y = str(k).split("-")[0]
        prev = by_year.get(y)
        if prev is None or _parse_period_key(k) > _parse_period_key(prev[0]):
            by_year[y] = (str(k), float(v))
    return {y: v for y, (_, v) in sorted(by_year.items(), key=lambda kv: int(kv[0]))}

def _freq_of_key(k: str) -> str:
    """Crude freq detector: 'YYYY-Qn' -> Q ; 'YYYY-MM' -> M ; else 'A'."""
    s = str(k)
    if "-Q" in s:
        return "Q"
    if "-" in s:
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
        try:
            freq = _freq_of_key(k)
            buckets[freq][str(k)] = float(v)
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

# -----------------------------------------------------------------------------
# ISO + provider probes
# -----------------------------------------------------------------------------
def _iso_codes(country: str) -> Dict[str, Optional[str]]:
    """
    Resolve ISO codes defensively; never raise.
    Expects app.utils.country_codes.get_country_codes(name) → dict or similar.
    """
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

def _probe_provider(module_name: str, fns: Iterable[str], **kwargs) -> Tuple[Dict[str, float], Dict[str, Any]]:
    """
    Try calling a list of function names in a provider; return coerced numeric series and a small debug trace.
    Accepts {'country': 'Germany'} or legacy alias {'name': 'Germany'}.
    """
    mod = _safe_import(module_name)
    dbg: Dict[str, Any] = {"module": module_name, "tried": []}
    if mod is None:
        dbg["error"] = "import_failed"
        return {}, dbg

    kw_variants = [kwargs]
    if "country" in kwargs:
        kv = dict(kwargs)
        kv["name"] = kv.pop("country")
        kw_variants.append(kv)

    for fn in fns:
        f = getattr(mod, fn, None)
        if not callable(f):
            dbg["tried"].append({fn: "missing"})
            continue
        for kv in kw_variants:
            try:
                data = f(**kv)
                dbg["tried"].append({fn: {"ok": True}})
                return _coerce_numeric_series(data), dbg
            except Exception as e:
                dbg["tried"].append({fn: {"error": str(e)}})
    return {}, dbg

# -----------------------------------------------------------------------------
# Compat fetchers (primary) + WB fallback
# -----------------------------------------------------------------------------
def _compat_fetch_series(func_name: str, country: str, want_freq: str, keep_hint: int) -> Dict[str, float]:
    """
    Fetch from compat with hints; fall back to plain call; coerce + trim.
    keep_hint should be >= policy window (e.g., 24 for safety), we then trim strictly.
    """
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
    data = _coerce_numeric_series(raw)
    return _trim_series_policy(data, HIST_POLICY)

def _compat_fetch_series_retry(func_name: str, country: str, want_freq: str, keep_hint: int) -> Dict[str, float]:
    s = _compat_fetch_series(func_name, country, want_freq, keep_hint)
    if s:
        return s
    # tiny backoff and try once more
    _time.sleep(0.15)
    return _compat_fetch_series(func_name, country, want_freq, keep_hint)

def _wb_fallback_series(country: str, indicator_code: str) -> Dict[str, float]:
    """
    Direct WB fallback when compat function is missing/unimplemented.
    """
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

# -----------------------------------------------------------------------------
# Parallel compat fetch helpers (for faster first response)
# -----------------------------------------------------------------------------
_EXEC = ThreadPoolExecutor(max_workers=8)  # adjust if your instance has more CPU

def _compat_fetch_series_blocking(func_name: str, country: str, keep_hint: int) -> Dict[str, float]:
    """
    Blocking compat fetch + trim used inside the thread pool (parallel).
    Uses the same logic as _compat_fetch_series but drops want_freq (unused here).
    """
    mod = _safe_import("app.providers.compat")
    if not mod:
        return {}
    fn = getattr(mod, func_name, None)
    if not callable(fn):
        return {}
    for kwargs in (
        {"country": country, "series": "mini", "keep": max(keep_hint, 24)},
        {"country": country, "series": "full"},
        {"country": country},
    ):
        try:
            raw = fn(**kwargs) or {}
            if raw:
                return _trim_series_policy(_coerce_numeric_series(raw), HIST_POLICY)
        except TypeError:
            continue
        except Exception:
            continue
    return {}

async def _gather_series_parallel(country: str) -> Dict[str, Dict[str, float]]:
    """
    Run all compat fetches concurrently using a thread pool.
    Returns a dict keyed by our short names → trimmed series dicts.
    """
    loop = asyncio.get_event_loop()
    futs = {
        # Monthly (12m)
        "cpi_m":    loop.run_in_executor(_EXEC, _compat_fetch_series_blocking, "get_cpi_yoy_monthly", country, 24),
        "une_m":    loop.run_in_executor(_EXEC, _compat_fetch_series_blocking, "get_unemployment_rate_monthly", country, 24),
        "fx_m":     loop.run_in_executor(_EXEC, _compat_fetch_series_blocking, "get_fx_rate_usd_monthly", country, 24),
        "res_m":    loop.run_in_executor(_EXEC, _compat_fetch_series_blocking, "get_reserves_usd_monthly", country, 24),
        "policy_m": loop.run_in_executor(_EXEC, _compat_fetch_series_blocking, "get_policy_rate_monthly", country, 36),
        # Quarterly (4q)
        "gdp_q":    loop.run_in_executor(_EXEC, _compat_fetch_series_blocking, "get_gdp_growth_quarterly", country, 8),
        # Annual (20y)
        "cab_a":    loop.run_in_executor(_EXEC, _compat_fetch_series_blocking, "get_current_account_balance_pct_gdp", country, 40),
        "ge_a":     loop.run_in_executor(_EXEC, _compat_fetch_series_blocking, "get_government_effectiveness", country, 40),
    }
    results = await asyncio.gather(*futs.values(), return_exceptions=True)
    out = {}
    for key, res in zip(futs.keys(), results):
        out[key] = res if isinstance(res, dict) else {}
    return out

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------

# ——— Reachability ------------------------------------------------------------
@router.get("/__action_probe", summary="Connectivity probe")
def action_probe_get() -> Dict[str, Any]:
    return {"ok": True, "path": "/__action_probe"}

@router.options("/__action_probe", include_in_schema=False)
def action_probe_options() -> Response:
    # Allow preflights to succeed fast
    return Response(status_code=204)

# ——— Series probe ------------------------------------------------------------
@router.get("/__probe_series", summary="Probe Series")
def probe_series(
    country: str = Query(..., description="Full country name, e.g., Germany"),
) -> Dict[str, Any]:
    """
    Quick availability check for core indicators across providers.
    Returns length and latest period per source; never raises.
    """
    iso = _iso_codes(country)

    # CPI (YoY or equivalent)
    imf_cpi, dbg_imf_cpi = _probe_provider(
        "app.providers.imf_provider",
        ("get_cpi_yoy_monthly", "cpi_yoy_monthly", "get_cpi_yoy", "cpi_yoy"),
        country=country,
    )
    eu_cpi, dbg_eu_cpi = _probe_provider(
        "app.providers.eurostat_provider",
        ("get_hicp_yoy", "hicp_yoy", "get_cpi_yoy", "cpi_yoy"),
        country=country,
    )
    wb_cpi, dbg_wb_cpi = _probe_provider(
        "app.providers.wb_provider",
        ("get_cpi_annual", "cpi_annual"),
        country=country,
    )

    # Unemployment
    imf_une, dbg_imf_une = _probe_provider(
        "app.providers.imf_provider",
        ("get_unemployment_rate_monthly", "unemployment_rate_monthly", "get_unemployment_rate", "unemployment_rate"),
        country=country,
    )
    eu_une, dbg_eu_une = _probe_provider(
        "app.providers.eurostat_provider",
        ("get_unemployment_rate_monthly", "unemployment_rate_monthly", "get_unemployment_rate", "unemployment_rate"),
        country=country,
    )
    wb_une, dbg_wb_une = _probe_provider(
        "app.providers.wb_provider",
        ("get_unemployment_rate_annual", "unemployment_rate_annual"),
        country=country,
    )

    # FX vs USD
    imf_fx, dbg_imf_fx = _probe_provider(
        "app.providers.imf_provider",
        ("get_fx_rate_usd_monthly", "fx_rate_usd_monthly", "get_fx_rate_usd", "fx_rate_usd"),
        country=country,
    )

    # Reserves (USD)
    imf_res, dbg_imf_res = _probe_provider(
        "app.providers.imf_provider",
        ("get_reserves_usd_monthly", "reserves_usd_monthly", "get_reserves_usd", "reserves_usd"),
        country=country,
    )

    # Policy rate
    imf_pr, dbg_imf_pr = _probe_provider(
        "app.providers.imf_provider",
        ("get_policy_rate_monthly", "policy_rate_monthly", "get_policy_rate", "policy_rate"),
        country=country,
    )

    # GDP growth (quarterly preferred)
    imf_gdpq, dbg_imf_gdpq = _probe_provider(
        "app.providers.imf_provider",
        ("get_gdp_growth_quarterly", "gdp_growth_quarterly"),
        country=country,
    )
    wb_gdpa, dbg_wb_gdpa = _probe_provider(
        "app.providers.wb_provider",
        ("get_gdp_growth_annual", "gdp_growth_annual"),
        country=country,
    )

    def _brief(series: Mapping[str, float]) -> Dict[str, Any]:
        if not series:
            return {"len": 0, "latest": None}
        period, _ = _latest(series)
        return {"len": len(series), "latest": period}

    cpi_wb_annual = _to_annual_latest(wb_cpi)
    une_wb_annual = _to_annual_latest(wb_une)

    resp = {
        "ok": True,
        "country": country,
        "iso2": iso.get("iso_alpha_2"),
        "iso3": iso.get("iso_alpha_3"),
        "series": {
            "cpi": {
                "IMF": _brief(imf_cpi),
                "Eurostat": _brief(eu_cpi),
                "WB_annual": _brief(cpi_wb_annual),
            },
            "unemployment": {
                "IMF": _brief(imf_une),
                "Eurostat": _brief(eu_une),
                "WB_annual": _brief(une_wb_annual),
            },
            "fx": {
                "IMF": _brief(imf_fx),
            },
            "reserves": {
                "IMF": _brief(imf_res),
            },
            "policy_rate": {
                "IMF": _brief(imf_pr),
            },
            "gdp_growth": {
                "IMF_quarterly": _brief(imf_gdpq),
                "WB_annual": _brief(wb_gdpa),
            },
        },
        "_debug": {
            "cpi": {"IMF": dbg_imf_cpi, "Eurostat": dbg_eu_cpi, "WB": dbg_wb_cpi},
            "unemployment": {"IMF": dbg_imf_une, "Eurostat": dbg_eu_une, "WB": dbg_wb_une},
            "fx": {"IMF": dbg_imf_fx},
            "reserves": {"IMF": dbg_imf_res},
            "policy_rate": {"IMF": dbg_imf_pr},
            "gdp_growth": {"IMF_q": dbg_imf_gdpq, "WB_a": dbg_wb_gdpa},
        },
    }
    return resp

@router.options("/__probe_series", include_in_schema=False)
def probe_series_options() -> Response:
    return Response(status_code=204)

# ——— Compat probe ------------------------------------------------------------
@router.get("/__compat_probe", summary="Inspect compat normalization for one indicator")
def compat_probe(
    indicator: str,
    country: str = "Mexico",
    freq: str = "auto",  # monthly/annual/quarterly/auto
):
    import app.providers.compat as compat

    name_map = {
        ("cpi_yoy","monthly"): "get_cpi_yoy_monthly",
        ("cpi_yoy","annual"):  "get_cpi_annual",
        ("unemployment_rate","monthly"): "get_unemployment_rate_monthly",
        ("unemployment_rate","annual"):  "get_unemployment_rate_annual",
        ("fx_rate_usd","monthly"): "get_fx_rate_usd_monthly",
        ("fx_rate_usd","annual"):  "get_fx_official_annual",
        ("reserves_usd","monthly"): "get_reserves_usd_monthly",
        ("reserves_usd","annual"):  "get_reserves_annual",
        ("policy_rate","monthly"):  "get_policy_rate_monthly",
        ("gdp_growth","quarterly"): "get_gdp_growth_quarterly",
        ("gdp_growth","annual"):   "get_gdp_growth_annual",
    }
    if indicator == "gdp_growth":
        key = (indicator, "quarterly" if freq in ("auto","quarterly") else "annual")
    else:
        key = (indicator, "monthly" if freq in ("auto","monthly") else "annual")
    fn_name = name_map.get(key)
    fn = getattr(compat, fn_name, None) if fn_name else None
    if not callable(fn):
        return {"error": f"compat function not found: {fn_name}"}

    series = fn(country=country)
    head = dict(list(series.items())[:10])
    return {
        "indicator": indicator,
        "compat_fn": fn_name,
        "country": country,
        "normalized_len": len(series),
        "normalized_head": head,
    }

# --- Provider introspection helpers ------------------------------------------
@router.get("/__provider_fns", summary="List callables exported by a provider module")
def provider_fns(module: str):
    """
    List top-level callables in a provider module so we can see what's actually deployed.
    Example: /__provider_fns?module=app.providers.wb_provider_cr
    """
    mod = _safe_import(module)
    if not mod:
        return {"ok": False, "module": module, "error": "import_failed"}
    fns = []
    for name, obj in vars(mod).items():
        if name.startswith("_"):
            continue
        if callable(obj):
            try:
                sig = str(inspect.signature(obj))
            except Exception:
                sig = "(?)"
            fns.append({"name": name, "signature": sig})
    return {"ok": True, "module": module, "count": len(fns), "functions": sorted(fns, key=lambda x: x["name"])}

@router.get("/__codes", summary="Show resolved ISO codes for a country")
def show_codes(country: str = "Mexico"):
    from app.utils.country_codes import get_country_codes
    codes = get_country_codes(country)
    return {"country": country, "codes": codes}

@router.get("/__provider_raw", summary="Call a provider function directly and preview result")
def provider_raw(
    module: str,
    fn: str,
    country: str = "Mexico",
):
    """
    Call a specific function in a provider with various arg shapes (country/name/iso2/iso3/code)
    and return a short preview of the raw payload.
    """
    mod = _safe_import(module)
    if not mod:
        return {"ok": False, "module": module, "fn": fn, "error": "import_failed"}
    f = getattr(mod, fn, None)
    if not callable(f):
        return {"ok": False, "module": module, "fn": fn, "error": "fn_missing"}
    # variants
    from app.utils import country_codes as cc
    codes = (cc.get_country_codes(country) or {}) if hasattr(cc, "get_country_codes") else {}
    trials = [
        {"country": country},
        {"name": country},
        {"iso2": codes.get("iso_alpha_2")},
        {"iso3": codes.get("iso_alpha_3")},
        {"code": codes.get("iso_alpha_3") or codes.get("iso_alpha_2")},
    ]
    tried = []
    res = err = None
    for kv in trials:
        if any(v is None for v in kv.values()):
            continue
        try:
            res = f(**kv)
            tried.append({"kwargs": kv, "ok": True})
            break
        except TypeError as e:
            tried.append({"kwargs": kv, "error": str(e)})
        except Exception as e:
            tried.append({"kwargs": kv, "error": f"{type(e).__name__}: {e}"})
    if res is None:
        # final positional attempt
        try:
            res = f(country)
            tried.append({"args": [country], "ok": True})
        except Exception as e:
            tried.append({"args": [country], "error": f"{type(e).__name__}: {e}"})
    def _preview(obj: Any, limit: int = 12):
        if obj is None:
            return None
        if isinstance(obj, dict):
            keys = list(obj.keys())
            head = keys[:limit]
            small = {str(k): obj[k] for k in head}
            return {"type": "mapping", "len": len(keys), "head_keys": head, "head_values": small}
        if isinstance(obj, (list, tuple)):
            return {"type": "sequence", "len": len(obj), "head": list(obj)[:limit]}
        return {"type": type(obj).__name__, "repr": repr(obj)[:400]}
    return {
        "ok": res is not None,
        "module": module,
        "fn": fn,
        "tried": tried,
        "result_type": None if res is None else type(res).__name__,
        "result_preview": _preview(res),
    }

# -----------------------------------------------------------------------------
# Parallel compat fetch helpers (for faster first response)
# -----------------------------------------------------------------------------
_EXEC = ThreadPoolExecutor(max_workers=8)  # adjust if your instance has more CPU

def _compat_fetch_series_blocking(func_name: str, country: str, keep_hint: int) -> Dict[str, float]:
    """
    Blocking compat fetch + trim used inside the thread pool (parallel).
    Uses the same logic as _compat_fetch_series but drops want_freq (unused here).
    """
    mod = _safe_import("app.providers.compat")
    if not mod:
        return {}
    fn = getattr(mod, func_name, None)
    if not callable(fn):
        return {}
    for kwargs in (
        {"country": country, "series": "mini", "keep": max(keep_hint, 24)},
        {"country": country, "series": "full"},
        {"country": country},
    ):
        try:
            raw = fn(**kwargs) or {}
            if raw:
                return _trim_series_policy(_coerce_numeric_series(raw), HIST_POLICY)
        except TypeError:
            continue
        except Exception:
            continue
    return {}

async def _gather_series_parallel(country: str) -> Dict[str, Dict[str, float]]:
    """
    Run all compat fetches concurrently using a thread pool.
    Returns a dict keyed by our short names → trimmed series dicts.
    """
    loop = asyncio.get_event_loop()
    futs = {
        # Monthly (12m)
        "cpi_m":    loop.run_in_executor(_EXEC, _compat_fetch_series_blocking, "get_cpi_yoy_monthly", country, 24),
        "une_m":    loop.run_in_executor(_EXEC, _compat_fetch_series_blocking, "get_unemployment_rate_monthly", country, 24),
        "fx_m":     loop.run_in_executor(_EXEC, _compat_fetch_series_blocking, "get_fx_rate_usd_monthly", country, 24),
        "res_m":    loop.run_in_executor(_EXEC, _compat_fetch_series_blocking, "get_reserves_usd_monthly", country, 24),
        "policy_m": loop.run_in_executor(_EXEC, _compat_fetch_series_blocking, "get_policy_rate_monthly", country, 36),
        # Quarterly (4q)
        "gdp_q":    loop.run_in_executor(_EXEC, _compat_fetch_series_blocking, "get_gdp_growth_quarterly", country, 8),
        # Annual (20y)
        "cab_a":    loop.run_in_executor(_EXEC, _compat_fetch_series_blocking, "get_current_account_balance_pct_gdp", country, 40),
        "ge_a":     loop.run_in_executor(_EXEC, _compat_fetch_series_blocking, "get_government_effectiveness", country, 40),
    }
    results = await asyncio.gather(*futs.values(), return_exceptions=True)
    out = {}
    for key, res in zip(futs.keys(), results):
        out[key] = res if isinstance(res, dict) else {}
    return out

@router.options("/v1/country-lite", include_in_schema=False)
def country_lite_options() -> Response:
    return Response(status_code=204)
