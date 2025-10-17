# app/providers/compat.py — robust runtime shim to normalize provider outputs
from __future__ import annotations
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, Callable, Sequence
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

def _val_from_mapping(m: Mapping[str, Any]) -> Optional[float]:
    # Common value keys across providers
    for vk in ("value","val","v","y","OBS_VALUE","obs_value"):
        if vk in m:
            fv = _coerce_float(m[vk])
            if fv is not None:
                return fv
    # Sometimes dict is {"period": "...", "value": ...} and we’ll pick via caller
    return None

def _period_from_mapping(m: Mapping[str, Any]) -> Optional[str]:
    # Common period keys
    for pk in ("date","period","time","TIME_PERIOD","time_period"):
        if pk in m and m[pk] not in (None, ""):
            return str(m[pk])
    # Compose year + month/quarter
    y = m.get("year") or m.get("yr") or m.get("y")
    mth = m.get("month") or m.get("mn") or m.get("m")
    q = m.get("quarter") or m.get("qtr") or m.get("q")
    if y:
        try:
            yi = int(y)
        except Exception:
            return None
        if q:
            try:
                qi = int(q); return f"{yi}-Q{qi}"
            except Exception:
                return None
        if mth:
            try:
                mi = int(mth); return f"{yi}-{mi:02d}"
            except Exception:
                return None
        return str(yi)
    return None

def _normalize_series(data: Any) -> Dict[str, float]:
    """
    Normalize many shapes into {period: float}.
    Handles:
      - Mapping[str, number]
      - Mapping with nested dict values holding {'value': ...} or OBS_VALUE
      - Mapping with 'series'/'data'/'values'/'rows'/'observations'/'points'
      - Dict of parallel arrays: {'periods':[...],'values':[...]} or {'x':[...],'y':[...]}
      - Sequence of (period, value)
      - Sequence of dict rows with date/period/time/TIME_PERIOD and value/val/v/y/OBS_VALUE
      - Nested under country/ISO keys: {'Mexico': {...}}, {'MEX': {...}}
      - Objects with .to_dict()
    """
    # Pandas-like
    if hasattr(data, "to_dict") and callable(getattr(data, "to_dict")):
        try:
            data = data.to_dict()
        except Exception:
            pass

    out: Dict[str, float] = {}

    if data is None:
        return out

    # Mapping branch
    if isinstance(data, Mapping):
        # 1) Nested under country/ISO key
        for k in list(data.keys()):
            v = data[k]
            if isinstance(v, (Mapping, Sequence)) and isinstance(k, str) and (len(k) in (2,3) or k.lower() in ("usa","uk","mexico","germany","sweden")):
                nested = _normalize_series(v)
                if nested:
                    return nested

        # 2) Well-known container keys
        for key in ("series","data","values","rows","observations","points"):
            if key in data and isinstance(data[key], (Mapping, Sequence)):
                nested = _normalize_series(data[key])
                if nested:
                    return nested

        # 3) Parallel arrays
        def _map_parallel(a: Sequence, b: Sequence) -> Dict[str, float]:
            tmp: Dict[str, float] = {}
            try:
                n = min(len(a), len(b))
                for i in range(n):
                    p = str(a[i]); fv = _coerce_float(b[i])
                    if fv is not None: tmp[p] = fv
            except Exception:
                pass
            return tmp
        if "periods" in data and "values" in data and isinstance(data["periods"], Sequence) and isinstance(data["values"], Sequence):
            tmp = _map_parallel(data["periods"], data["values"])
            if tmp: return tmp
        if "x" in data and "y" in data and isinstance(data["x"], Sequence) and isinstance(data["y"], Sequence):
            tmp = _map_parallel(data["x"], data["y"])
            if tmp: return tmp

        # 4) Direct mapping {period: number} OR {period: {'value': number}}
        ok = False
        for k, v in data.items():
            if isinstance(v, Mapping):
                fv = _val_from_mapping(v)
                if fv is not None:
                    out[str(k)] = fv; ok = True
            else:
                fv = _coerce_float(v)
                if fv is not None:
                    out[str(k)] = fv; ok = True
        if ok:
            return out

        # 5) Latest-only mapping
        if any(k in data for k in ("latest_period","TIME_PERIOD","time_period")) and any(k in data for k in ("latest_value","OBS_VALUE","obs_value","value","val","v","y")):
            lp = data.get("latest_period") or data.get("TIME_PERIOD") or data.get("time_period")
            for vk in ("latest_value","OBS_VALUE","obs_value","value","val","v","y"):
                if vk in data:
                    lv = _coerce_float(data[vk]); 
                    if lp and lv is not None: return {str(lp): lv}
            return {}

        return {}

    # Sequence branch
    if isinstance(data, Sequence) and not isinstance(data, (str, bytes)):
        # Pair of sequences ([periods], [values])
        if len(data) == 2 and all(isinstance(x, Sequence) and not isinstance(x, (str, bytes)) for x in data):
            a, b = data
            tmp: Dict[str, float] = {}
            try:
                n = min(len(a), len(b))
                for i in range(n):
                    p = str(a[i]); fv = _coerce_float(b[i])
                    if fv is not None: tmp[p] = fv
                return tmp
            except Exception:
                pass

        # General list of rows
        tmp: Dict[str, float] = {}
        for row in data:
            if isinstance(row, Mapping):
                period = _period_from_mapping(row)
                if period:
                    for vk in ("value","val","v","y","OBS_VALUE","obs_value"):
                        if vk in row:
                            fv = _coerce_float(row[vk])
                            if fv is not None:
                                tmp[str(period)] = fv
                                break
                continue
            if isinstance(row, (list, tuple)) and len(row) >= 2:
                period = str(row[0]); fv = _coerce_float(row[1])
                if fv is not None: tmp[period] = fv
        return tmp

    return {}

def _pick_fn(mod, name_candidates: Iterable[str], substr_hints: Iterable[str]) -> Optional[Callable[..., Any]]:
    if not mod:
        return None
    for nm in name_candidates:
        fn = getattr(mod, nm, None)
        if callable(fn):
            return fn
    best = None
    for k, v in vars(mod).items():
        if not callable(v) or k.startswith("_"): 
            continue
        name = k.lower()
        if all(s in name for s in substr_hints):
            best = v; break
    return best

def _call_with_variants(fn: Callable[..., Any], country: str) -> Any:
    iso = _iso_codes(country)
    variants = [
        {"country": country},
        {"name": country},
        {"iso2": iso.get("iso2")},
        {"iso3": iso.get("iso3")},
        {"code": iso.get("iso3") or iso.get("iso2")},
    ]
    for kv in variants:
        if any(v is None for v in kv.values()):
            continue
        try:
            return fn(**kv)
        except TypeError:
            continue
        except Exception:
            continue
    try:
        return fn(country)  # positional
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
    mod = _safe_import("app.providers.wb_provider")
    fn = _pick_fn(mod,
        ["get_debt_to_gdp_annual","debt_to_gdp_annual","get_general_gov_debt_pct_gdp","general_gov_debt_pct_gdp"],
        ["debt","gdp"]
    )
    return _call_series(fn, country) if fn else {}
