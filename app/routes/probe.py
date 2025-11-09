# app/routes/probe.py — diagnostics + lightweight country info (stable + cached)
from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional, Tuple
import inspect
import time as _time
from concurrent.futures import ThreadPoolExecutor

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
# Compat fetchers (primary) + WB fallback (sync, no threads)
# -----------------------------------------------------------------------------
def _compat_fetch_series(func_name: str, country: str, want_freq: str, keep_hint: int) -> Dict[str, float]:
    """
    Fetch from compat with hints; fall back to plain call; coerce + trim.
    keep_hint should be >= policy window (e.g., 24/36/48 for safety), we then trim strictly.
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
    _time.sleep(0.1)
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
# Debt fast-fail executor
# -----------------------------------------------------------------------------
_EXEC_DEBT = ThreadPoolExecutor(max_workers=2)
DEBT_TIMEOUT_S = 2.0  # don’t let debt stall the request

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
    res = None
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

# --- Country Lite (compat-first, bounded history, cached) — with fast-fail debt
@router.get(
    "/v1/country-lite",
    summary="Country Lite",
    operation_id="country_lite_get",
)
def country_lite(
    country: str = Query(..., description="Full country name, e.g., Mexico"),
    fresh: bool = Query(False, description="Bypass cache if true"),
) -> JSONResponse:
    """
    Compat-first snapshot with bounded windows (M:12, Q:4, A:20).
    Uses fast-fail for debt (≤2s) so slow providers don't block responses.
    """
    # 0) Cache
    if not fresh:
        cached = _cache_get(country)
        if cached:
            return JSONResponse(content=cached)

    iso = _iso_codes(country)

    # ---- Debt-to-GDP (annual, trimmed) — fast-fail wrapper
    debt: Dict[str, Any] = {}
    try:
        from app.services.debt_service import compute_debt_payload  # type: ignore
        fut = _EXEC_DEBT.submit(compute_debt_payload, country)
        try:
            debt = fut.result(timeout=DEBT_TIMEOUT_S) or {}
        except Exception:
            try:
                fut.cancel()
            except Exception:
                pass
            debt = {}
    except Exception:
        debt = {}

    debt_series_full = (debt.get("series") or {}) if isinstance(debt, dict) else {}
    debt_series = _trim_series_policy(debt_series_full, HIST_POLICY)  # A:20
    debt_latest = (debt.get("latest") or {}) if isinstance(debt, dict) else {}
    if not debt_latest or debt_latest.get("value") is None:
        debt_latest = {"year": None, "value": None, "source": "computed:NA/NA"}

    # ---- GDP growth (quarterly, 4) — compat + retry
    gdp_growth_q = _compat_fetch_series_retry("get_gdp_growth_quarterly", country, "Q", keep_hint=12)

    # ---- Monthly (12): CPI YoY, Unemployment, FX, Reserves, Policy rate — compat + retry
    # bump keep hints so late-arriving months survive trimming
    cpi_m    = _compat_fetch_series_retry("get_cpi_yoy_monthly",           country, "M", keep_hint=36)
    une_m    = _compat_fetch_series_retry("get_unemployment_rate_monthly",  country, "M", keep_hint=36)
    fx_m     = _compat_fetch_series_retry("get_fx_rate_usd_monthly",        country, "M", keep_hint=36)
    res_m    = _compat_fetch_series_retry("get_reserves_usd_monthly",       country, "M", keep_hint=36)
    policy_m = _compat_fetch_series_retry("get_policy_rate_monthly",        country, "M", keep_hint=48)

    # ---- Annual (20): Current Account % GDP, Government Effectiveness (WB fallback)
    cab_a = _compat_fetch_series_retry("get_current_account_balance_pct_gdp", country, "A", keep_hint=40)
    if not cab_a:
        cab_a = _wb_fallback_series(country, "BN.CAB.XOKA.GD.ZS")
    ge_a  = _compat_fetch_series_retry("get_government_effectiveness",        country, "A", keep_hint=40)
    if not ge_a:
        ge_a = _wb_fallback_series(country, "GE.EST")

    # ---- Latest extraction
    def _kvl(d: Mapping[str, float]) -> Tuple[Optional[str], Optional[float]]:
        return _latest(d)

    cpi_p, cpi_v   = _kvl(cpi_m)
    une_p, une_v   = _kvl(une_m)
    fx_p, fx_v     = _kvl(fx_m)
    res_p, res_v   = _kvl(res_m)
    pol_p, pol_v   = _kvl(policy_m)
    gdpq_p, gdpq_v = _kvl(gdp_growth_q)
    cab_p, cab_v   = _kvl(cab_a)
    ge_p, ge_v     = _kvl(ge_a)

    resp: Dict[str, Any] = {
        "country": country,
        "iso_codes": iso,
        # Debt block (annual, trimmed)
        "latest": {
            "year":  debt_latest.get("year"),
            "value": debt_latest.get("value"),
            "source": debt_latest.get("source"),
        },
        "series": debt_series,
        "source": debt_latest.get("source"),
        # Legacy top-levels retained (compatibility with older clients)
        "imf_data": {},
        "government_debt": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
        "nominal_gdp": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
        "debt_to_gdp": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
        "debt_to_gdp_series": {},
        # Indicators with required frequency and **trimmed history**
        "additional_indicators": {
            # Monthly — 12m
            "cpi_yoy": {
                "latest_value": cpi_v, "latest_period": cpi_p,
                "source": "compat/IMF", "series": cpi_m,
            },
            "unemployment_rate": {
                "latest_value": une_v, "latest_period": une_p,
                "source": "compat/IMF", "series": une_m,
            },
            "fx_rate_usd": {
                "latest_value": fx_v, "latest_period": fx_p,
                "source": "compat/IMF", "series": fx_m,
            },
            "reserves_usd": {
                "latest_value": res_v, "latest_period": res_p,
                "source": "compat/IMF", "series": res_m,
            },
            "policy_rate": {
                "latest_value": pol_v, "latest_period": pol_p,
                "source": "compat/IMF/ECB", "series": policy_m,
            },
            # Quarterly — 4q
            "gdp_growth": {
                "latest_value": gdpq_v, "latest_period": gdpq_p,
                "source": "compat/IMF", "series": gdp_growth_q,
            },
            # Annual — 20y (with WB fallback)
            "current_account_balance_pct_gdp": {
                "latest_value": cab_v, "latest_period": cab_p,
                "source": "compat/WB" if cab_a else "unavailable",
                "series": cab_a,
            },
            "government_effectiveness": {
                "latest_value": ge_v, "latest_period": ge_p,
                "source": "compat/WB WGI" if ge_a else "unavailable",
                "series": ge_a,
            },
        },
        "_debug": {
            "builder": "country_lite (sync + cache + retry)",
            "history_policy": HIST_POLICY,
        },
    }

    # Save to cache (best-effort)
    try:
        _cache_set(country, resp)
    except Exception:
        pass

    return JSONResponse(content=resp)

@router.options("/v1/country-lite", include_in_schema=False)
def country_lite_options() -> Response:
    return Response(status_code=204)
