# app/routes/probe.py — diagnostics + lightweight country info
from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple
import inspect

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse, Response

router = APIRouter(tags=["probe"])

# -----------------------------------------------------------------------------
# Utilities (defensive: never raise in probes)
# -----------------------------------------------------------------------------

def _compat_series(func_name: str, country: str) -> Dict[str, float]:
    mod = _safe_import("app.providers.compat")
    if not mod:
        return {}
    fn = getattr(mod, func_name, None)
    if not callable(fn):
        return {}
    try:
        data = fn(country=country)
        return _coerce_numeric_series(data)
    except Exception:
        return {}

def _latest_pair(series: Mapping[str, float]) -> Tuple[Optional[str], Optional[float]]:
    if not series:
        return None, None
    k, v = _latest(series)
    return k, v

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

# --- BEGIN: provider introspection helpers -----------------------------------

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

# --- END: provider introspection helpers -------------------------------------

# ——— Country Lite ------------------------------------------------------------
@router.get("/v1/country-lite", summary="Country Lite")
def country_lite(
    country: str = Query(..., description="Full country name, e.g., Mexico"),
) -> Dict[str, Any]:
    """
    Lightweight snapshot built off the compat layer (which routes to the actual
    deployed provider functions on Render). Soft-fails and returns what it can.
    """
    iso = _iso_codes(country)

    # ---- Debt-to-GDP via unified service (uses compat/Eurostat/IMF/WB as needed)
    try:
        from app.services.debt_service import compute_debt_payload
        debt = compute_debt_payload(country) or {}
    except Exception:
        debt = {}
    debt_latest = debt.get("latest") or {"year": None, "value": None, "source": "computed:NA/NA"}
    debt_series = debt.get("series") or {}

    # ---- Latest-only indicators via compat (monthly/quarterly where possible)
    cpi_s  = _compat_series("get_cpi_yoy_monthly", country)
    une_s  = _compat_series("get_unemployment_rate_monthly", country)
    fx_s   = _compat_series("get_fx_rate_usd_monthly", country)
    res_s  = _compat_series("get_reserves_usd_monthly", country)
    pol_s  = _compat_series("get_policy_rate_monthly", country)
    gdpq_s = _compat_series("get_gdp_growth_quarterly", country)

    # Optional WB-style metrics (only if you have compat wrappers deployed)
    cab_s  = _compat_series("get_current_account_balance_pct_gdp", country)   # optional
    ge_s   = _compat_series("get_government_effectiveness", country)          # optional

    cpi_p,  cpi_v  = _latest_pair(cpi_s)
    une_p,  une_v  = _latest_pair(une_s)
    fx_p,   fx_v   = _latest_pair(fx_s)
    res_p,  res_v  = _latest_pair(res_s)
    pol_p,  pol_v  = _latest_pair(pol_s)
    gdpq_p, gdpq_v = _latest_pair(gdpq_s)
    cab_p,  cab_v  = _latest_pair(cab_s)
    ge_p,   ge_v   = _latest_pair(ge_s)

    resp = {
        "country": country,
        "iso_codes": iso,

        # Debt block (your clients expect these fields)
        "latest": {"year": debt_latest.get("year"), "value": debt_latest.get("value"), "source": debt_latest.get("source")},
        "series": debt_series,
        "source": debt_latest.get("source"),

        # Legacy fields kept for display-parity
        "imf_data": {},
        "government_debt": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
        "nominal_gdp":    {"latest": {"value": None, "date": None, "source": None}, "series": {}},
        "debt_to_gdp":    {"latest": {"value": None, "date": None, "source": None}, "series": {}},
        "debt_to_gdp_series": {},

        # Latest-only indicators
        "additional_indicators": {
            "cpi_yoy":  {"latest_value": cpi_v,  "latest_period": cpi_p,  "source": "compat/IMF",  "series": {}},
            "unemployment_rate": {"latest_value": une_v, "latest_period": une_p, "source": "compat/IMF", "series": {}},
            "fx_rate_usd": {"latest_value": fx_v, "latest_period": fx_p, "source": "compat/IMF", "series": {}},
            "reserves_usd": {"latest_value": res_v, "latest_period": res_p, "source": "compat/IMF", "series": {}},
            "policy_rate": {"latest_value": pol_v, "latest_period": pol_p, "source": "compat/IMF/ECB", "series": {}},
            "gdp_growth": {"latest_value": gdpq_v, "latest_period": gdpq_p, "source": "compat/IMF", "series": {}},
            "current_account_balance_pct_gdp": {
                "latest_value": cab_v, "latest_period": cab_p, "source": "compat/WB", "series": {}
            },
            "government_effectiveness": {
                "latest_value": ge_v, "latest_period": ge_p, "source": "compat/WB WGI", "series": {}
            },
        },

        "_debug": {
            "builder": "probe.country_lite (compat)",
            "sources": {
                "debt": debt.get("latest", {}),
                "cpi": {"len": len(cpi_s)}, "unemployment": {"len": len(une_s)}, "fx": {"len": len(fx_s)},
                "reserves": {"len": len(res_s)}, "policy_rate": {"len": len(pol_s)},
                "gdp_growth_q": {"len": len(gdpq_s)}, "current_account_balance": {"len": len(cab_s)},
                "government_effectiveness": {"len": len(ge_s)},
            },
        },
    }
    return JSONResponse(content=resp)

@router.options("/v1/country-lite", include_in_schema=False)
def country_lite_options() -> Response:
    return Response(status_code=204)
