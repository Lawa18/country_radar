# app/providers/compat.py — robust runtime shim to normalize provider outputs
from __future__ import annotations
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, Callable, Sequence, Union
import types

# -------------------- small helpers --------------------

def _safe_import(path: str):
    try:
        return __import__(path, fromlist=["*"])
    except Exception:
        return None

def _iso_codes(country: str) -> Dict[str, Optional[str]]:
    try:
        cc_mod = _safe_import("app.utils.country_codes")
        if cc_mod and hasattr(cc_mod, "get_country_codes"):
            codes = cc_mod.get_country_codes(country)
            if isinstance(codes, Mapping):
                return {
                    "name": str(codes.get("name") or country),
                    "iso2": codes.get("iso_alpha_2") or codes.get("alpha2") or codes.get("iso2"),
                    "iso3": codes.get("iso_alpha_3") or codes.get("alpha3") or codes.get("iso3"),
                    "numeric": codes.get("iso_numeric") or codes.get("numeric"),
                }
    except Exception:
        pass
    return {"name": country, "iso2": None, "iso3": None, "numeric": None}

def _coerce_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if v != v:  # NaN
            return None
        return v
    except Exception:
        return None

def _norm_period_from_dict(d: Mapping[str, Any]) -> Optional[str]:
    # Look for common date/period fields
    for k in ("date", "period", "time"):
        if k in d and d[k]:
            return str(d[k])
    # Compose year + month or quarter
    y = d.get("year") or d.get("yr") or d.get("y")
    m = d.get("month") or d.get("mn") or d.get("m")
    q = d.get("quarter") or d.get("qtr") or d.get("q")
    if y:
        try:
            yi = int(y)
        except Exception:
            return None
        if q:
            try:
                qi = int(q)
                return f"{yi}-Q{qi}"
            except Exception:
                return None
        if m:
            try:
                mi = int(m)
                return f"{yi}-{mi:02d}"
            except Exception:
                return None
        return str(yi)
    return None

def _normalize_series(data: Any) -> Dict[str, float]:
    """
    Accept many shapes and normalize to {period_str: float_value}.
    Supported shapes:
      - Mapping[str, number]
      - Mapping with "series" or "data" key
      - Sequence of (period, value)
      - Sequence of dicts with keys like date/period/time and value/val/v/y
      - Objects with .to_dict()
    """
    # Pandas objects or similar with to_dict()
    if hasattr(data, "to_dict") and callable(getattr(data, "to_dict")):
        try:
            data = data.to_dict()
        except Exception:
            pass

    out: Dict[str, float] = {}

    # Some providers return {"series": {...}} or {"data": [...]}
    if isinstance(data, Mapping):
        # If explicit "series"
        for key in ("series", "data", "values"):
            if key in data and isinstance(data[key], (Mapping, Sequence)):
                nested = _normalize_series(data[key])
                if nested:
                    return nested
        # direct mapping {period:value}
        ok = False
        for k, v in data.items():
            fv = _coerce_float(v)
            if fv is not None:
                out[str(k)] = fv
                ok = True
        if ok:
            return out
        # mapping with rows (dicts)
        # ex: {"rows": [{"date": "...", "value": ...}, ...]}
        for key in ("rows", "observations", "points"):
            if key in data and isinstance(data[key], Sequence):
                return _normalize_series(data[key])
        # mapping with only latest
        if "latest_period" in data and "latest_value" in data:
            lp = data.get("latest_period")
            lv = _coerce_float(data.get("latest_value"))
            if lp and lv is not None:
                return {str(lp): lv}
        return {}

    # sequence of tuples (period, value)
    if isinstance(data, Sequence) and not isinstance(data, (str, bytes)):
        tmp: Dict[str, float] = {}
        for row in data:
            if isinstance(row, Mapping):
                period = _norm_period_from_dict(row)  # date/period or year+month/quarter
                if period:
                    # value fields
                    for vk in ("value", "val", "v", "y"):
                        if vk in row:
                            fv = _coerce_float(row[vk])
                            if fv is not None:
                                tmp[str(period)] = fv
                                break
                continue
            if isinstance(row, (list, tuple)) and len(row) >= 2:
                period = str(row[0])
                fv = _coerce_float(row[1])
                if fv is not None:
                    tmp[period] = fv
        return tmp

    # anything else: nothing
    return {}

def _pick_fn(mod, name_candidates: Iterable[str], substr_hints: Iterable[str]) -> Optional[Callable[..., Any]]:
    """Find a function: exact candidates first, otherwise fuzzy by substrings."""
    if not mod:
        return None
    # exact
    for nm in name_candidates:
        fn = getattr(mod, nm, None)
        if callable(fn):
            return fn
    # fuzzy
    best = None
    for k, v in vars(mod).items():
        if not callable(v) or k.startswith("_"):
            continue
        name = k.lower()
        if all(s in name for s in substr_hints):
            best = v
            break
    return best

def _call_with_variants(fn: Callable[..., Any], country: str) -> Any:
    iso = _iso_codes(country)
    # Try most common calling conventions
    variants = [
        {"country": country},
        {"name": country},
        {"iso2": iso.get("iso2")},
        {"iso3": iso.get("iso3")},
        {"code": iso.get("iso3") or iso.get("iso2")},
    ]
    for kv in variants:
        # skip None-valued variants
        if any(v is None for v in kv.values()):
            continue
        try:
            return fn(**kv)
        except TypeError:
            continue
        except Exception:
            continue
    # final attempt: positional
    try:
        return fn(country)
    except Exception:
        return None

def _call_series(fn: Callable[..., Any], country: str) -> Dict[str, float]:
    data = _call_with_variants(fn, country)
    return _normalize_series(data)

# ----------------------- IMF-normalized accessors -----------------------------

def get_cpi_yoy_monthly(country: str) -> Dict[str, float]:
    mod = _safe_import("app.providers.imf_provider")
    fn = _pick_fn(mod,
        ["get_cpi_yoy_monthly", "cpi_yoy_monthly", "get_cpi_yoy", "cpi_yoy", "get_inflation_cpi_yoy", "inflation_cpi_yoy"],
        ["cpi", "yoy"]
    )
    return _call_series(fn, country) if fn else {}

def get_unemployment_rate_monthly(country: str) -> Dict[str, float]:
    mod = _safe_import("app.providers.imf_provider")
    fn = _pick_fn(mod,
        ["get_unemployment_rate_monthly","unemployment_rate_monthly","get_unemployment_rate","unemployment_rate"],
        ["unemployment"]
    )
    return _call_series(fn, country) if fn else {}

def get_fx_rate_usd_monthly(country: str) -> Dict[str, float]:
    mod = _safe_import("app.providers.imf_provider")
    fn = _pick_fn(mod,
        ["get_fx_rate_usd_monthly","fx_rate_usd_monthly","get_fx_rate_usd","fx_rate_usd","get_exchange_rate_usd","exchange_rate_usd"],
        ["fx","usd"]
    )
    return _call_series(fn, country) if fn else {}

def get_reserves_usd_monthly(country: str) -> Dict[str, float]:
    mod = _safe_import("app.providers.imf_provider")
    fn = _pick_fn(mod,
        ["get_reserves_usd_monthly","reserves_usd_monthly","get_reserves_usd","reserves_usd"],
        ["reserve"]
    )
    return _call_series(fn, country) if fn else {}

def get_policy_rate_monthly(country: str) -> Dict[str, float]:
    # Allow IMF or ECB providers; pick best available
    mod = _safe_import("app.providers.imf_provider") or _safe_import("app.providers.ecb_provider")
    fn = _pick_fn(mod,
        ["get_policy_rate_monthly","policy_rate_monthly","get_policy_rate","policy_rate","get_interest_rate_policy","interest_rate_policy"],
        ["policy","rate"]
    )
    return _call_series(fn, country) if fn else {}

def get_gdp_growth_quarterly(country: str) -> Dict[str, float]:
    mod = _safe_import("app.providers.imf_provider")
    fn = _pick_fn(mod,
        ["get_gdp_growth_quarterly","gdp_growth_quarterly","get_gdp_qoq_annualized","gdp_qoq_annualized"],
        ["gdp","growth"]
    )
    return _call_series(fn, country) if fn else {}

# ----------------------- WB-normalized accessors ------------------------------

def get_cpi_annual(country: str) -> Dict[str, float]:
    mod = _safe_import("app.providers.wb_provider")
    fn = _pick_fn(mod, ["get_cpi_annual","cpi_annual","get_inflation_annual","inflation_annual"], ["cpi"])
    return _call_series(fn, country) if fn else {}

def get_unemployment_rate_annual(country: str) -> Dict[str, float]:
    mod = _safe_import("app.providers.wb_provider")
    fn = _pick_fn(mod, ["get_unemployment_rate_annual","unemployment_rate_annual"], ["unemployment"])
    return _call_series(fn, country) if fn else {}

def get_fx_official_annual(country: str) -> Dict[str, float]:
    mod = _safe_import("app.providers.wb_provider")
    fn = _pick_fn(mod, ["get_fx_official_annual","fx_official_annual"], ["fx","official"])
    return _call_series(fn, country) if fn else {}

def get_reserves_annual(country: str) -> Dict[str, float]:
    mod = _safe_import("app.providers.wb_provider")
    fn = _pick_fn(mod, ["get_reserves_annual","reserves_annual"], ["reserve"])
    return _call_series(fn, country) if fn else {}

def get_gdp_growth_annual(country: str) -> Dict[str, float]:
    mod = _safe_import("app.providers.wb_provider")
    fn = _pick_fn(mod, ["get_gdp_growth_annual","gdp_growth_annual"], ["gdp","growth"])
    return _call_series(fn, country) if fn else {}

def get_current_account_balance_pct_gdp(country: str) -> Dict[str, float]:
    mod = _safe_import("app.providers.wb_provider")
    fn = _pick_fn(mod,
        ["get_current_account_balance_pct_gdp","current_account_balance_pct_gdp"],
        ["current","account"]
    )
    return _call_series(fn, country) if fn else {}

def get_government_effectiveness(country: str) -> Dict[str, float]:
    mod = _safe_import("app.providers.wb_provider")
    fn = _pick_fn(mod, ["get_government_effectiveness","government_effectiveness"], ["government","effectiveness"])
    return _call_series(fn, country) if fn else {}

def get_debt_to_gdp_annual(country: str) -> Dict[str, float]:
    # Accept either “debt_to_gdp_annual” or “general_gov_debt_pct_gdp”
    mod = _safe_import("app.providers.wb_provider")
    fn = _pick_fn(mod,
        ["get_debt_to_gdp_annual","debt_to_gdp_annual","get_general_gov_debt_pct_gdp","general_gov_debt_pct_gdp"],
        ["debt","gdp"]
    )
    return _call_series(fn, country) if fn else {}
