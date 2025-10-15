# app/providers/compat.py — runtime shim to normalize provider function names
from __future__ import annotations
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, Callable
import types

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

def _pick_fn(mod, name_candidates: Iterable[str], substr_hints: Iterable[str]) -> Optional[Callable[..., Any]]:
    """Find a function: exact match by candidates first, otherwise fuzzy by substrings."""
    if not mod:
        return None
    # exact candidates
    for nm in name_candidates:
        fn = getattr(mod, nm, None)
        if callable(fn):
            return fn
    # fuzzy by substrings (best-effort)
    best = None
    for k, v in vars(mod).items():
        if not callable(v) or k.startswith("_"):
            continue
        name = k.lower()
        if all(s in name for s in substr_hints):
            best = v
            break
    return best

def _call_series(fn: Callable[..., Any], country: str) -> Dict[str, float]:
    # try country= first, then legacy name=
    try:
        data = fn(country=country)
    except TypeError:
        try:
            data = fn(name=country)
        except Exception:
            return {}
    except Exception:
        return {}
    return _coerce_numeric_series(data)

# --- IMF-normalized accessors -------------------------------------------------

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
    mod = _safe_import("app.providers.imf_provider")
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

# --- WB-normalized accessors --------------------------------------------------

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
