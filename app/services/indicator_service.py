# app/services/indicator_service.py — v2 builder using provider compat shim
from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, Literal
import math

# ----------------------- utils: imports & coercion ----------------------------

def _safe_import(path: str):
    try:
        return __import__(path, fromlist=["*"])
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
    """Sort keys like 'YYYY', 'YYYY-MM', 'YYYY-Qn' robustly."""
    try:
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

def _trim_by_keep(series: Dict[str, float], keep: int) -> Dict[str, float]:
    if keep <= 0 or not series:
        return series
    keys = sorted(series.keys(), key=_parse_period_key)
    if len(keys) <= keep:
        return series
    return {k: series[k] for k in keys[-keep:]}

def _apply_series_mode(series: Dict[str, float], mode: Literal["none", "mini", "full"], keep: int) -> Dict[str, float]:
    if mode == "none":
        if not series:
            return {}
        k, v = _latest(series)
        return {k: v} if k is not None else {}
    if mode == "mini":
        return _trim_by_keep(series, keep)
    return series  # full

def _annualize_latest(d: Mapping[str, float]) -> Dict[str, float]:
    """Collapse to annual by taking latest period per year (for WB fallbacks if needed later)."""
    if not d:
        return {}
    by_year: Dict[str, Tuple[str, float]] = {}
    for k, v in d.items():
        y = str(k).split("-")[0]
        prev = by_year.get(y)
        if prev is None or _parse_period_key(k) > _parse_period_key(prev[0]):
            by_year[y] = (str(k), float(v))
    return {y: v for y, (_, v) in sorted(by_year.items(), key=lambda kv: int(kv[0]))}

# -------------------- provider call helper (fixed interface) ------------------

def _call_provider(module: str, candidates: Iterable[str], **kwargs) -> Tuple[Dict[str, float], Dict[str, Any]]:
    """
    Call a function by name in a module; coerce dict-of-numbers back.
    Accepts {'country': 'Mexico'} and also tries legacy {'name': 'Mexico'}.
    Returns (series, debug_trace).
    """
    dbg: Dict[str, Any] = {"module": module, "tried": []}
    mod = _safe_import(module)
    if not mod:
        dbg["error"] = "import_failed"
        return {}, dbg

    variants = [kwargs]
    if "country" in kwargs:
        kv = dict(kwargs)
        kv["name"] = kv.pop("country")
        variants.append(kv)

    for fn in candidates:
        f = getattr(mod, fn, None)
        if not callable(f):
            dbg["tried"].append({fn: "missing"})
            continue
        for kv in variants:
            try:
                data = f(**kv)
                dbg["tried"].append({fn: {"ok": True}})
                return _coerce_numeric_series(data), dbg
            except Exception as e:
                dbg["tried"].append({fn: {"error": str(e)}})
    return {}, dbg

# ------------------------ ISO codes (defensive) -------------------------------

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

# --------------------------- v2 builder core ----------------------------------

def build_country_payload_v2(
    country: str,
    series: Literal["none", "mini", "full"] = "mini",
    keep: int = 60,
) -> Dict[str, Any]:
    """
    Modern builder (compat-first):
      - Uses app.providers.compat for IMF + WB (compat does fuzzy resolution).
      - series: none|mini|full; keep trims length (60 ≈ 5y monthly, ~20 quarterly).
      - Eurostat stays disabled by default (Render DNS quirks).
    """
    keep_m = keep if keep != 60 else 60
    keep_q = max(20, math.ceil(keep / 3))

    out: Dict[str, Any] = {
        "ok": True,
        "country": country,
        "iso_codes": _iso_codes(country),
        "series_mode": series,
        "keep_days": keep,
        "indicators": {},
        "_debug": {
            "builder": {
                "used": "build_country_payload_v2",
                "module": __name__,
            },
            "source_trace": {},
            "eurostat": {"enabled": False, "host": "data-api.ec.europa.eu", "dns": False},
        },
    }

    # -------- CPI YoY (monthly preferred, IMF via compat) --------
    cpi_series, cpi_src = {}, None
    c_imf, dbg_imf_cpi = _call_provider("app.providers.compat", ("get_cpi_yoy_monthly",), country=country)
    if c_imf:
        cpi_series, cpi_src = c_imf, "IMF"
    else:
        c_wb, dbg_wb_cpi = _call_provider("app.providers.compat", ("get_cpi_annual",), country=country)
        if c_wb:
            cpi_series, cpi_src = _annualize_latest(c_wb), "WorldBank"
        else:
            dbg_wb_cpi = _call_provider("app.providers.compat", (), country=country)[1]
    out["_debug"]["source_trace"]["cpi_yoy"] = {"compat_imf": dbg_imf_cpi, "compat_wb": locals().get("dbg_wb_cpi", {})}

    # -------- Unemployment rate --------
    une_series, une_src = {}, None
    u_imf, dbg_imf_une = _call_provider("app.providers.compat", ("get_unemployment_rate_monthly",), country=country)
    if u_imf:
        une_series, une_src = u_imf, "IMF"
    else:
        u_wb, dbg_wb_une = _call_provider("app.providers.compat", ("get_unemployment_rate_annual",), country=country)
        if u_wb:
            une_series, une_src = _annualize_latest(u_wb), "WorldBank"
        else:
            dbg_wb_une = _call_provider("app.providers.compat", (), country=country)[1]
    out["_debug"]["source_trace"]["unemployment_rate"] = {
        "compat_imf": dbg_imf_une, "compat_wb": locals().get("dbg_wb_une", {})
    }

    # -------- FX rate vs USD (monthly) --------
    fx_series, fx_src = {}, None
    fx_imf, dbg_imf_fx = _call_provider("app.providers.compat", ("get_fx_rate_usd_monthly",), country=country)
    if fx_imf:
        fx_series, fx_src = fx_imf, "IMF"
    else:
        fx_wb, dbg_wb_fx = _call_provider("app.providers.compat", ("get_fx_official_annual",), country=country)
        if fx_wb:
            fx_series, fx_src = _annualize_latest(fx_wb), "WorldBank"
        else:
            dbg_wb_fx = _call_provider("app.providers.compat", (), country=country)[1]
    out["_debug"]["source_trace"]["fx_rate_usd"] = {"compat_imf": dbg_imf_fx, "compat_wb": locals().get("dbg_wb_fx", {})}

    # -------- Reserves (USD) --------
    res_series, res_src = {}, None
    r_imf, dbg_imf_res = _call_provider("app.providers.compat", ("get_reserves_usd_monthly",), country=country)
    if r_imf:
        res_series, res_src = r_imf, "IMF"
    else:
        r_wb, dbg_wb_res = _call_provider("app.providers.compat", ("get_reserves_annual",), country=country)
        if r_wb:
            res_series, res_src = _annualize_latest(r_wb), "WorldBank"
        else:
            dbg_wb_res = _call_provider("app.providers.compat", (), country=country)[1]
    out["_debug"]["source_trace"]["reserves_usd"] = {"compat_imf": dbg_imf_res, "compat_wb": locals().get("dbg_wb_res", {})}

    # -------- Policy rate --------
    pol_series, pol_src = {}, None
    p_imf, dbg_imf_pol = _call_provider("app.providers.compat", ("get_policy_rate_monthly",), country=country)
    if p_imf:
        pol_series, pol_src = p_imf, "IMF"
    out["_debug"]["source_trace"]["policy_rate"] = {"compat_imf": dbg_imf_pol}

    # -------- GDP growth (quarterly preferred; WB annual fallback) --------
    gdp_series, gdp_src, gdp_freq = {}, None, "quarterly"
    gq_imf, dbg_imf_gdpq = _call_provider("app.providers.compat", ("get_gdp_growth_quarterly",), country=country)
    if gq_imf:
        gdp_series, gdp_src = gq_imf, "IMF"
    else:
        ga_wb, dbg_wb_gdpa = _call_provider("app.providers.compat", ("get_gdp_growth_annual",), country=country)
        if ga_wb:
            gdp_series, gdp_src, gdp_freq = _annualize_latest(ga_wb), "WorldBank", "annual"
        else:
            dbg_wb_gdpa = _call_provider("app.providers.compat", (), country=country)[1]
    out["_debug"]["source_trace"]["gdp_growth"] = {"compat_imf": dbg_imf_gdpq, "compat_wb": locals().get("dbg_wb_gdpa", {})}

    # ---------------- apply series mode & assemble indicators -----------------

    def _pack(series_dict: Dict[str, float], src: Optional[str], freq: str, mode: str, keepn: int):
        ser = _apply_series_mode(series_dict, mode, keepn)
        period, value = _latest(ser)
        return {"series": ser, "latest_period": period, "latest_value": value, "source": src, "freq": freq}

    out["indicators"]["cpi_yoy"]           = _pack(cpi_series, cpi_src, "monthly",  series, keep_m)
    out["indicators"]["unemployment_rate"] = _pack(une_series, une_src, "monthly",  series, keep_m)
    out["indicators"]["fx_rate_usd"]       = _pack(fx_series,  fx_src,  "monthly",  series, keep_m)
    out["indicators"]["reserves_usd"]      = _pack(res_series, res_src, "monthly",  series, keep_m)
    out["indicators"]["policy_rate"]       = _pack(pol_series, pol_src, "monthly",  series, keep_m)
    out["indicators"]["gdp_growth"]        = _pack(gdp_series, gdp_src, gdp_freq,   series, keep_q)

    # ---------------- debt enrichment (best effort) ---------------------------

    out["government_debt"]    = {"latest": {"value": None, "date": None, "source": None}, "series": {}}
    out["nominal_gdp"]        = {"latest": {"value": None, "date": None, "source": None}, "series": {}}
    out["debt_to_gdp"]        = {"latest": {"value": None, "date": None, "source": "computed:NA/NA"}, "series": {}}
    out["debt_to_gdp_series"] = {}

    debt_mod = _safe_import("app.routes.debt_bundle") or _safe_import("app.routes.debt")
    fn = getattr(debt_mod, "compute_debt_payload", None) if debt_mod else None
    if callable(fn):
        try:
            bundle = fn(country=country)
            if isinstance(bundle, Mapping):
                out["government_debt"]    = bundle.get("government_debt", out["government_debt"])
                out["nominal_gdp"]        = bundle.get("nominal_gdp", out["nominal_gdp"])
                out["debt_to_gdp"]        = bundle.get("debt_to_gdp", out["debt_to_gdp"])
                out["debt_to_gdp_series"] = bundle.get("debt_to_gdp_series", out["debt_to_gdp_series"])
        except Exception as e:
            out["_debug"].setdefault("debt", {})["error"] = str(e)

    return out

# --------------------------- legacy fallback ----------------------------------

def build_country_payload(country: str, series: str = "mini", keep: int = 60) -> Dict[str, Any]:
    """Compatibility wrapper for legacy callers—delegate to v2 with same signature."""
    return build_country_payload_v2(country=country, series=series, keep=keep)
