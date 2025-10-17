# app/routes/probe.py — diagnostics + lightweight country info
from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional, Tuple

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

router = APIRouter()


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
                # normalize keys we care about
                return {
                    "name": str(codes.get("name") or country),
                    "iso_alpha_2": codes.get("iso_alpha_2") or codes.get("alpha2") or codes.get("iso2"),
                    "iso_alpha_3": codes.get("iso_alpha_3") or codes.get("alpha3") or codes.get("iso3"),
                    "iso_numeric": codes.get("iso_numeric") or codes.get("numeric"),
                }
    except Exception:
        pass
    # fallback: unknown codes, but keep name
    return {"name": country, "iso_alpha_2": None, "iso_alpha_3": None, "iso_numeric": None}


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------

@router.get("/__action_probe", summary="Connectivity probe", tags=["probe"])
def __action_probe() -> Dict[str, Any]:
    return {"ok": True, "path": "/__action_probe"}

@router.get("/__probe_series", summary="Probe Series", tags=["probe"])
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

    # Annualize some for consistent “len”
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
                # WB’s FX is typically not monthly; omit unless you have one
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
        # Minimal debug so it’s usable but compact
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

@router.get("/__compat_probe", tags=["probe"], summary="Inspect compat normalization for one indicator")
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
    key = None
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
from typing import Any, Dict
import inspect

def _safe_import(path: str):
    try:
        return __import__(path, fromlist=["*"])
    except Exception:
        return None

@router.get("/__provider_fns", tags=["probe"], summary="List callables exported by a provider module")
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

@router.get("/__codes", tags=["probe"], summary="Show resolved ISO codes for a country")
def show_codes(country: str = "Mexico"):
    from app.utils.country_codes import get_country_codes
    codes = get_country_codes(country)
    return {"country": country, "codes": codes}

@router.get("/__provider_raw", tags=["probe"], summary="Call a provider function directly and preview result")
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

from typing import Any, Dict, Iterable, Mapping, Sequence
import inspect

def _safe_import(path: str):
    try:
        return __import__(path, fromlist=["*"])
    except Exception:
        return None

def _iso_variants(country: str) -> Dict[str, Any]:
    try:
        cc_mod = _safe_import("app.utils.country_codes")
        if cc_mod and hasattr(cc_mod, "get_country_codes"):
            c = cc_mod.get_country_codes(country) or {}
            return {
                "country": country,
                "name": country,
                "iso2": c.get("iso_alpha_2") or c.get("alpha2") or c.get("iso2"),
                "iso3": c.get("iso_alpha_3") or c.get("alpha3") or c.get("iso3"),
                "code": c.get("iso_alpha_3") or c.get("alpha3") or c.get("iso3") or c.get("iso_alpha_2"),
            }
    except Exception:
        pass
    return {"country": country, "name": country, "iso2": None, "iso3": None, "code": None}

def _call_with_variants(fn, country: str):
    variants = _iso_variants(country)
    attempts = [
        {"country": variants["country"]},
        {"name": variants["name"]},
        {"iso2": variants["iso2"]} if variants["iso2"] else None,
        {"iso3": variants["iso3"]} if variants["iso3"] else None,
        {"code": variants["code"]} if variants["code"] else None,
    ]
    for kv in [x for x in attempts if x]:
        try:
            return {"args": kv, "result": fn(**kv), "error": None}
        except TypeError as e:
            err = str(e)
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
        # try positional as a last resort for this variant
        try:
            return {"args": {"positional": list(kv.values())[0]}, "result": fn(list(kv.values())[0]), "error": None}
        except Exception as e2:
            err = f"{type(e2).__name__}: {e2}"
    return {"args": {}, "result": None, "error": err if 'err' in locals() else "no_variant_matched"}

def _preview(obj: Any, limit: int = 12) -> Any:
    # Make a short, JSON-safe preview
    try:
        import pandas as pd  # if present
        if isinstance(obj, (pd.Series, pd.DataFrame)):
            obj = obj.to_dict()
    except Exception:
        pass

    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, Mapping):
        keys = list(obj.keys())
        head = keys[:limit]
        preview_map = {str(k): obj[k] for k in head}
        # If mapping-of-mapping, show nested keys for first few
        nested = {}
        for k in head:
            v = obj[k]
            if isinstance(v, Mapping):
                nested[str(k)] = list(v.keys())[:5]
        out = {"type": "mapping", "len": len(keys), "head_keys": head, "head_values": preview_map}
        if nested:
            out["nested_keys"] = nested
        return out
    if isinstance(obj, Sequence) and not isinstance(obj, (bytes, bytearray, str)):
        head = list(obj)[:limit]
        return {"type": "sequence", "len": len(obj), "head": head}
    # fallback repr
    return {"type": type(obj).__name__, "repr": repr(obj)[:400]}

@router.get("/__provider_raw", tags=["probe"], summary="Call a provider function directly and preview result")
def provider_raw(
    module: str = Query(..., description="e.g., app.providers.imf_provider"),
    fn_candidates: str = Query(..., description="Comma-separated function names to try, in order"),
    country: str = Query("Mexico"),
):
    """
    Tries to import `module`, finds the first callable among `fn_candidates`,
    calls it with multiple argument variants (country/name/iso2/iso3/code or positional),
    and returns a short preview of the raw return value.
    """
    mod = _safe_import(module)
    if not mod:
        return {"ok": False, "error": f"import_failed: {module}"}
    tried = []
    f = None
    for name in [s.strip() for s in fn_candidates.split(",") if s.strip()]:
        cand = getattr(mod, name, None)
        if callable(cand):
            f = cand
            tried.append({name: "callable"})
            break
        tried.append({name: "missing"})
    if not f:
        return {"ok": False, "module": module, "tried": tried, "error": "no_callable_found"}

    call = _call_with_variants(f, country)
    res = call["result"]
    return {
        "ok": True,
        "module": module,
        "fn_used": [k for k,v in tried[-1].items()][0] if tried else None,
        "call_args": call["args"],
        "call_error": call["error"],
        "result_type": type(res).__name__ if res is not None else None,
        "result_preview": _preview(res, 10),
    }

@router.get("/v1/country-lite", summary="Country Lite", tags=["probe"])
def country_lite(
    country: str = Query(..., description="Full country name, e.g., Mexico"),
) -> Dict[str, Any]:
    """
    Lightweight country snapshot:
      - ISO codes
      - Debt-to-GDP (WB ratio preferred)
      - A few latest macro indicators (IMF/WB)
    Always soft-fails and returns what it can.
    """
    iso = _iso_codes(country)

    # Debt-to-GDP ratio (World Bank preferred here for parity with your current outputs)
    wb = _safe_import("app.providers.wb_provider")
    imf = _safe_import("app.providers.imf_provider")

    ratio_series, src_ratio_dbg = {}, {}
    if wb:
        ratio_series, src_ratio_dbg = _probe_provider(
            "app.providers.wb_provider",
            ("get_central_gov_debt_pct_gdp", "central_gov_debt_pct_gdp", "get_debt_to_gdp_annual", "debt_to_gdp_annual"),
            country=country,
        )
    if not ratio_series and imf:
        ratio_series, src_ratio_dbg = _probe_provider(
            "app.providers.imf_provider",
            ("get_debt_to_gdp_annual", "debt_to_gdp_annual", "get_general_gov_debt_pct_gdp", "general_gov_debt_pct_gdp"),
            country=country,
        )
    ratio_series = _to_annual_latest(ratio_series)
    latest_year, latest_value = _latest(ratio_series)
    ratio_source = "World Bank (ratio)" if wb and ratio_series else ("IMF (ratio)" if imf and ratio_series else None)

    # Additional indicators (latest only)
    def _latest_only(module: str, fn_candidates: Iterable[str]) -> Tuple[Optional[str], Optional[float], Dict[str, Any], str]:
        s, dbg = _probe_provider(module, tuple(fn_candidates), country=country)
        period, value = _latest(s)
        src = "IMF" if "imf" in module else ("WorldBank" if "wb" in module else "Eurostat")
        return period, value, dbg, src

    cpi_p, cpi_v, dbg_cpi, cpi_src = _latest_only("app.providers.imf_provider",
        ("get_cpi_yoy_monthly","cpi_yoy_monthly","get_cpi_yoy","cpi_yoy"))
    une_p, une_v, dbg_une, une_src = _latest_only("app.providers.imf_provider",
        ("get_unemployment_rate_monthly","unemployment_rate_monthly","get_unemployment_rate","unemployment_rate"))
    fx_p, fx_v, dbg_fx, fx_src = _latest_only("app.providers.imf_provider",
        ("get_fx_rate_usd_monthly","fx_rate_usd_monthly","get_fx_rate_usd","fx_rate_usd"))
    res_p, res_v, dbg_res, res_src = _latest_only("app.providers.imf_provider",
        ("get_reserves_usd_monthly","reserves_usd_monthly","get_reserves_usd","reserves_usd"))
    pol_p, pol_v, dbg_pol, pol_src = _latest_only("app.providers.imf_provider",
        ("get_policy_rate_monthly","policy_rate_monthly","get_policy_rate","policy_rate"))
    gdpq_p, gdpq_v, dbg_gdpq, gdpq_src = _latest_only("app.providers.imf_provider",
        ("get_gdp_growth_quarterly","gdp_growth_quarterly"))

    cab_p, cab_v, dbg_cab, cab_src = _latest_only("app.providers.wb_provider",
        ("get_current_account_balance_pct_gdp","current_account_balance_pct_gdp"))
    ge_p, ge_v, dbg_ge, ge_src = _latest_only("app.providers.wb_provider",
        ("get_government_effectiveness","government_effectiveness"))

    resp = {
        "country": country,
        "iso_codes": iso,
        "imf_data": {},  # kept for backward display-parity with your current client
        "latest": {
            "year": latest_year,
            "value": latest_value,
            "source": ratio_source,
        },
        "series": ratio_series,   # annual series used by your client
        "source": ratio_source,
        "government_debt": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
        "nominal_gdp":    {"latest": {"value": None, "date": None, "source": None}, "series": {}},
        "debt_to_gdp":    {"latest": {"value": None, "date": None, "source": None}, "series": {}},
        "debt_to_gdp_series": {},

        "additional_indicators": {
            "cpi_yoy":  {"latest_value": cpi_v,  "latest_period": cpi_p,  "source": cpi_src,  "series": {}},
            "unemployment_rate": {"latest_value": une_v, "latest_period": une_p, "source": une_src, "series": {}},
            "fx_rate_usd": {"latest_value": fx_v, "latest_period": fx_p, "source": fx_src, "series": {}},
            "reserves_usd": {"latest_value": res_v, "latest_period": res_p, "source": res_src, "series": {}},
            "policy_rate": {"latest_value": pol_v, "latest_period": pol_p, "source": pol_src, "series": {}},
            "gdp_growth": {"latest_value": gdpq_v, "latest_period": gdpq_p, "source": gdpq_src, "series": {}},
            "current_account_balance_pct_gdp": {
                "latest_value": cab_v, "latest_period": cab_p, "source": cab_src, "series": {}
            },
            "government_effectiveness": {
                "latest_value": ge_v, "latest_period": ge_p, "source": ge_src, "series": {}
            },
        },
        "_debug": {
            "builder": "probe.country_lite",
            "sources": {
                "debt_ratio": src_ratio_dbg,
                "cpi": dbg_cpi, "unemployment": dbg_une, "fx": dbg_fx,
                "reserves": dbg_res, "policy_rate": dbg_pol, "gdp_growth_q": dbg_gdpq,
                "current_account_balance": dbg_cab, "government_effectiveness": dbg_ge,
            },
        },
    }
    return JSONResponse(content=resp)
