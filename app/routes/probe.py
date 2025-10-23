# app/routes/probe.py — diagnostics + lightweight country info
from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple
import os
import time
import asyncio
import inspect
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse, Response

router = APIRouter(tags=["probe"])

# -----------------------------------------------------------------------------
# Config / timeouts / cache
# -----------------------------------------------------------------------------
HIST_POLICY = {"A": 20, "Q": 4, "M": 12}  # Annual, Quarterly, Monthly window sizes

IND_TIMEOUT = float(os.getenv("CR_INDICATOR_TIMEOUT", "3.0"))   # seconds per indicator
ROUTE_DEADLINE = float(os.getenv("CR_COUNTRY_TIMEOUT", "6.0"))  # seconds for whole route
_THREADPOOL = int(os.getenv("CR_THREADPOOL", "8"))
_TTL_SECS = int(os.getenv("CR_TTL_SECS", "900"))

_EXEC = ThreadPoolExecutor(max_workers=_THREADPOOL)

class _TTLCache:
    def __init__(self, ttl_sec: int = 900):
        self.ttl = ttl_sec
        self._d: Dict[str, Tuple[float, Any]] = {}

    def get(self, key: str) -> Any:
        row = self._d.get(key)
        if not row:
            return None
        ts, val = row
        if time.time() - ts > self.ttl:
            try:
                del self._d[key]
            except Exception:
                pass
            return None
        return val

    def set(self, key: str, val: Any) -> None:
        self._d[key] = (time.time(), val)

_cache = _TTLCache(ttl_sec=_TTL_SECS)

# -----------------------------------------------------------------------------
# Utilities (defensive: never raise in probes)
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
    """Trim mixed/single-freq series to policy windows by freq."""
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

def _latest_pair(series: Mapping[str, float]) -> Tuple[Optional[str], Optional[float]]:
    if not series:
        return None, None
    return _latest(series)

def _probe_provider(module_name: str, fns: Iterable[str], **kwargs) -> Tuple[Dict[str, float], Dict[str, Any]]:
    """Try a list of function names in a provider; return coerced numeric series + debug trace."""
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

def _iso_codes(country: str) -> Dict[str, Optional[str]]:
    """Resolve ISO codes defensively; never raise."""
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

# -----------------------------------------------------------------------------
# Blocking→async bridge & compat/WB async helpers (timeouts + small caches)
# -----------------------------------------------------------------------------
async def _run_blocking(fn, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_EXEC, lambda: fn(**kwargs))

async def _compat_fetch_series_async(
    func_name: str,
    country: str,
    keep_hint: int,
    prefer_sdmx: bool = False,
    fresh: bool = False,
) -> Dict[str, float]:
    """
    Async wrapper that calls compat.<func_name>(country=..., series/keep/prefer hints),
    coerces numbers, trims by HIST_POLICY, and respects IND_TIMEOUT.
    Uses a per-indicator TTL cache unless fresh=True.
    """
    mod = _safe_import("app.providers.compat")
    if not mod:
        return {}

    fn = getattr(mod, func_name, None)
    if not callable(fn):
        return {}

    cache_key = f"compat:{func_name}:{country}:k{keep_hint}:sdmx{int(prefer_sdmx)}"
    if not fresh:
        cached = _cache.get(cache_key)
        if isinstance(cached, dict) and cached:
            return cached

    kwargs_variants = [
        {"country": country, "series": "mini", "keep": max(keep_hint, 24)},
        {"country": country},  # fallback
    ]
    if prefer_sdmx:
        for kv in kwargs_variants:
            kv.setdefault("prefer", "sdmxcentral")  # ignored if fn doesn't accept

    for kwargs in kwargs_variants:
        try:
            raw = await asyncio.wait_for(_run_blocking(fn, **kwargs), timeout=IND_TIMEOUT)
            data = _coerce_numeric_series(raw or {})
            if data:
                out = _trim_series_policy(data, HIST_POLICY)
                if not fresh:
                    _cache.set(cache_key, out)
                return out
        except asyncio.TimeoutError:
            continue
        except TypeError:
            continue
        except Exception:
            continue
    return {}

def _compat_or_provider_series(country: str,
                               compat_candidates: Iterable[str],
                               provider_fallbacks: Iterable[Tuple[str, Iterable[str]]],
                               keep_hint: int = 24) -> Dict[str, float]:
    """
    Try a list of compat functions first; if none return data, try provider fallbacks
    using the existing _probe_provider utility. Trim to policy window at the end.
    """
    # 1) compat candidates
    mod = _safe_import("app.providers.compat")
    if mod:
        for name in compat_candidates:
            fn = getattr(mod, name, None)
            if callable(fn):
                for kv in (
                    {"country": country, "series": "mini", "keep": max(keep_hint, 24)},
                    {"country": country},
                ):
                    try:
                        raw = fn(**kv) or {}
                        if raw:
                            return _trim_series_policy(_coerce_numeric_series(raw), HIST_POLICY)
                    except Exception:
                        continue

    # 2) provider fallbacks [(module, [fn1, fn2, ...]), ...]
    for module_name, fn_list in provider_fallbacks:
        series, _dbg = _probe_provider(module_name, tuple(fn_list), country=country)
        if series:
            return _trim_series_policy(series, HIST_POLICY)

    return {}

async def _wb_fallback_series_async(country: str, indicator_code: str) -> Dict[str, float]:
    """WB fallback via wb_provider raw helpers, under IND_TIMEOUT."""
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
        raw = await asyncio.wait_for(
            _run_blocking(fetch, iso3=iso3, indicator=indicator_code),
            timeout=IND_TIMEOUT
        )
        series = _coerce_numeric_series(to_year(raw))
        return _trim_series_policy(series, HIST_POLICY)
    except Exception:
        return {}

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------

# ——— Reachability ------------------------------------------------------------
@router.get("/__action_probe", summary="Connectivity probe")
def action_probe_get() -> Dict[str, Any]:
    return {"ok": True, "path": "/__action_probe"}

@router.options("/__action_probe", include_in_schema=False)
def action_probe_options() -> Response:
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
            "fx": {"IMF": _brief(imf_fx)},
            "reserves": {"IMF": _brief(imf_res)},
            "policy_rate": {"IMF": _brief(imf_pr)},
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

# --- BEGIN: provider introspection helpers -----------------------------------
@router.get("/__provider_fns", summary="List callables exported by a provider module")
def provider_fns(module: str):
    """List top-level callables in a provider module."""
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
    """Call a specific function in a provider with various arg shapes and preview result."""
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
# --- END: provider introspection helpers -------------------------------------

# === Country Lite (mode-aware) ===============================================

@router.get("/v1/country-lite", summary="Country Lite")
def country_lite(
    country: str = Query(..., description="Full country name, e.g., Mexico"),
    mode: str = Query("auto", description="auto|sync|async|dry"),
    prefer: str = Query("default", description="provider preference hint"),
    fresh: bool = Query(False, description="hint upstream caches to refresh"),
) -> Dict[str, Any]:
    """
    Compat-first, frequency-aware snapshot with bounded history windows:
      - Debt-to-GDP (annual, last 20y)
      - GDP growth (quarterly, last 4q)
      - Monthly set (CPI YoY, Unemployment, FX, Reserves, Policy rate) last 12m
      - Current Account % GDP (annual, last 20y) — WB fallback
      - Government Effectiveness (annual, last 20y) — WB fallback

    Modes:
      - dry  : wiring-only (no upstream)
      - sync : single-threaded, no gather/async, most stable
      - async: parallel fetch (honors timeouts)
      - auto : sync if CR_USE_ASYNC_PROBE is "0", else async
    """
    # ---- config / env --------------------------------------------------------
    use_async_env = os.getenv("CR_USE_ASYNC_PROBE", "1").lower() not in ("0", "false", "no")
    prefer_sdmx  = os.getenv("CR_PREFER_SDMXCENTRAL", "0").lower() in ("1", "true", "yes")
    IND_TIMEOUT  = float(os.getenv("CR_INDICATOR_TIMEOUT", "8.0"))
    ROUTE_DEADLINE = float(os.getenv("CR_COUNTRY_TIMEOUT", "18.0"))

    # normalize mode
    mode = (mode or "auto").lower().strip()
    if mode not in ("auto", "sync", "async", "dry"):
        mode = "auto"

    # resolve ISO (safe)
    iso = _iso_codes(country)

    # ---- dry mode: structure only -------------------------------------------
    if mode == "dry":
        resp = {
            "country": country,
            "iso_codes": iso,
            "latest": {"year": None, "value": None, "source": "unavailable"},
            "series": {},
            "source": None,
            "imf_data": {},
            "government_debt": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
            "nominal_gdp":    {"latest": {"value": None, "date": None, "source": None}, "series": {}},
            "debt_to_gdp":    {"latest": {"value": None, "date": None, "source": None}, "series": {}},
            "debt_to_gdp_series": {},
            "additional_indicators": {
                "cpi_yoy":  {"latest_value": None, "latest_period": None, "source": "compat/IMF", "series": {}},
                "unemployment_rate": {"latest_value": None, "latest_period": None, "source": "compat/IMF", "series": {}},
                "fx_rate_usd": {"latest_value": None, "latest_period": None, "source": "compat/IMF", "series": {}},
                "reserves_usd": {"latest_value": None, "latest_period": None, "source": "compat/IMF", "series": {}},
                "policy_rate": {"latest_value": None, "latest_period": None, "source": "compat/IMF/ECB", "series": {}},
                "gdp_growth": {"latest_value": None, "latest_period": None, "source": "compat/IMF", "series": {}},
                "current_account_balance_pct_gdp": {"latest_value": None, "latest_period": None, "source": "compat/WB", "series": {}},
                "government_effectiveness": {"latest_value": None, "latest_period": None, "source": "compat/WB WGI", "series": {}},
            },
            "_debug": {
                "builder": "probe.country_lite (dry)",
                "history_policy": HIST_POLICY,
                "prefer_sdmxcentral": prefer_sdmx,
                "fresh": fresh,
                "timeouts": {"per_indicator": IND_TIMEOUT, "route_deadline": ROUTE_DEADLINE},
            },
        }
        return JSONResponse(content=resp)

    # ---- shared sync builder -------------------------------------------------
    def _build_sync() -> Dict[str, Any]:
        # Debt-to-GDP (uses your debt_service cascade + cache)
        try:
            from app.services.debt_service import compute_debt_payload
            debt = compute_debt_payload(country) or {}
        except Exception:
            debt = {}
        debt_series_full = debt.get("series") or {}
        debt_series = _trim_series_policy(debt_series_full, HIST_POLICY)  # A:20
        debt_latest = debt.get("latest") or {"year": None, "value": None, "source": "unavailable"}

        # Quarterly — last 4
        gdp_growth_q = _compat_fetch_series("get_gdp_growth_quarterly", country, "Q", keep_hint=12)

        # Monthly — last 12 (CPI & Unemp use tolerant helper)
        cpi_m = _compat_or_provider_series(
            country,
            compat_candidates=("get_cpi_yoy_monthly", "get_inflation_yoy_monthly", "get_cpi_monthly_yoy", "get_cpi_yoy"),
            provider_fallbacks=(
                ("app.providers.imf_provider", ("get_cpi_yoy_monthly","cpi_yoy_monthly","get_cpi_yoy","cpi_yoy")),
                ("app.providers.eurostat_provider", ("get_hicp_yoy","hicp_yoy")),
                ("app.providers.wb_provider", ("get_cpi_annual","cpi_annual")),
            ),
            keep_hint=24
        )
        une_m = _compat_or_provider_series(
            country,
            compat_candidates=("get_unemployment_rate_monthly","get_unemployment_monthly","get_unemployment_rate"),
            provider_fallbacks=(
                ("app.providers.imf_provider", ("get_unemployment_rate_monthly","unemployment_rate_monthly","get_unemployment_rate","unemployment_rate")),
                ("app.providers.eurostat_provider", ("get_unemployment_rate_monthly","unemployment_rate_monthly")),
                ("app.providers.wb_provider", ("get_unemployment_rate_annual","unemployment_rate_annual")),
            ),
            keep_hint=24
        )
        fx_m     = _compat_fetch_series("get_fx_rate_usd_monthly", country, "M", keep_hint=24)
        res_m    = _compat_fetch_series("get_reserves_usd_monthly", country, "M", keep_hint=24)
        policy_m = _compat_fetch_series("get_policy_rate_monthly", country, "M", keep_hint=36)

        # Annual — last 20 (WB fallback codes)
        cab_a = _compat_fetch_series("get_current_account_balance_pct_gdp", country, "A", keep_hint=40)
        if not cab_a:
            cab_a = _wb_fallback_series(country, "BN.CAB.XOKA.GD.ZS")
        ge_a  = _compat_fetch_series("get_government_effectiveness", country, "A", keep_hint=40)
        if not ge_a:
            ge_a = _wb_fallback_series(country, "GE.EST")

        # latests
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

        return {
            "country": country,
            "iso_codes": iso,

            # Debt block (annual, trimmed)
            "latest": {"year": debt_latest.get("year"), "value": debt_latest.get("value"), "source": debt_latest.get("source")},
            "series": debt_series,
            "source": debt_latest.get("source"),

            # Legacy top-levels retained
            "imf_data": {},
            "government_debt": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
            "nominal_gdp":    {"latest": {"value": None, "date": None, "source": None}, "series": {}},
            "debt_to_gdp":    {"latest": {"value": None, "date": None, "source": None}, "series": {}},
            "debt_to_gdp_series": {},

            # Indicators (trimmed history)
            "additional_indicators": {
                # Monthly — 12m
                "cpi_yoy":  {"latest_value": cpi_v,  "latest_period": cpi_p,  "source": "compat/IMF",     "series": cpi_m},
                "unemployment_rate": {"latest_value": une_v, "latest_period": une_p, "source": "compat/IMF", "series": une_m},
                "fx_rate_usd": {"latest_value": fx_v, "latest_period": fx_p, "source": "compat/IMF",       "series": fx_m},
                "reserves_usd": {"latest_value": res_v, "latest_period": res_p, "source": "compat/IMF",     "series": res_m},
                "policy_rate": {"latest_value": pol_v, "latest_period": pol_p, "source": "compat/IMF/ECB",  "series": policy_m},

                # Quarterly — 4q
                "gdp_growth": {"latest_value": gdpq_v, "latest_period": gdpq_p, "source": "compat/IMF",     "series": gdp_growth_q},

                # Annual — 20y
                "current_account_balance_pct_gdp": {
                    "latest_value": cab_v, "latest_period": cab_p, "source": "compat/WB", "series": cab_a
                },
                "government_effectiveness": {
                    "latest_value": ge_v, "latest_period": ge_p, "source": "compat/WB WGI", "series": ge_a
                },
            },

            "_debug": {
                "builder": "probe.country_lite (sync)",
                "history_policy": HIST_POLICY,
                "prefer_sdmxcentral": prefer_sdmx,
                "fresh": fresh,
            },
        }

    # ---- async (temporarily: run sync under a deadline) ----------------------
    async def _build_async_with_deadline():
        loop = asyncio.get_event_loop()
        try:
            return await asyncio.wait_for(loop.run_in_executor(None, _build_sync), timeout=ROUTE_DEADLINE)
        except asyncio.TimeoutError:
            return {
                "country": country,
                "iso_codes": iso,
                "latest": {"year": None, "value": None, "source": "timeout"},
                "series": {},
                "source": "timeout",
                "imf_data": {},
                "government_debt": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
                "nominal_gdp":    {"latest": {"value": None, "date": None, "source": None}, "series": {}},
                "debt_to_gdp":    {"latest": {"value": None, "date": None, "source": None}, "series": {}},
                "debt_to_gdp_series": {},
                "additional_indicators": {},
                "_debug": {
                    "builder": "timeout",
                    "history_policy": HIST_POLICY,
                    "deadline": ROUTE_DEADLINE,
                },
            }

    # choose mode
    if mode == "sync" or (mode == "auto" and not use_async_env):
        return JSONResponse(content=_build_sync())
    else:
        # async (for now it runs the sync builder under a deadline; we can parallelize later)
        return JSONResponse(content=asyncio.run(_build_async_with_deadline()))


@router.options("/v1/country-lite", include_in_schema=False)
def country_lite_options() -> Response:
    return Response(status_code=204)
