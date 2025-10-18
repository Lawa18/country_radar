# app/services/indicator_service.py — v2 builder using provider compat shim
from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, Literal
import math

# add near top of indicator_service.py
try:
    from app.services.debt_service import compute_debt_payload as _compute_debt_payload
except Exception:  # keep the module import non-fatal
    _compute_debt_payload = None

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
    Modern builder: IMF-first monthly/quarterly indicators + debt bundle if available.
    - `series`: none | mini | full   (affects series trimming)
    - `keep`:   hard cap on returned points (monthly/quarterly/annual)
    """
    # --- base ISO resolution
    try:
        from app.utils.country_codes import get_country_codes
        iso = get_country_codes(country) or {}
    except Exception:
        iso = {"name": country, "iso_alpha_2": None, "iso_alpha_3": None, "iso_numeric": None}

    # --- payload scaffold
    payload: Dict[str, Any] = {
        "ok": True,
        "country": country,
        "iso_codes": {
            "name": iso.get("name") or country,
            "iso_alpha_2": iso.get("iso_alpha_2"),
            "iso_alpha_3": iso.get("iso_alpha_3"),
            "iso_numeric": iso.get("iso_numeric"),
        },
        "series_mode": series,
        "keep_days": keep,
        "indicators": {
            "cpi_yoy":          {"series": {}, "latest_period": None, "latest_value": None, "source": None, "freq": "monthly"},
            "unemployment_rate":{"series": {}, "latest_period": None, "latest_value": None, "source": None, "freq": "monthly"},
            "fx_rate_usd":      {"series": {}, "latest_period": None, "latest_value": None, "source": None, "freq": "monthly"},
            "reserves_usd":     {"series": {}, "latest_period": None, "latest_value": None, "source": None, "freq": "monthly"},
            "policy_rate":      {"series": {}, "latest_period": None, "latest_value": None, "source": None, "freq": "monthly"},
            "gdp_growth":       {"series": {}, "latest_period": None, "latest_value": None, "source": None, "freq": "quarterly"},
        },
        "_debug": {
            "builder": {
                "name": "build_country_payload_v2",
                "module": __name__,
                "file": __file__,
                "signature": "(country: str, series: Literal['none','mini','full']='mini', keep: int=60) -> Dict[str, Any]",
                "mode": "v2",
            },
            "source_trace": {},
            "eurostat": {"enabled": False, "host": "data-api.ec.europa.eu", "dns": False},
            "notes": [],
        },
        # debt placeholders (merged later if available)
        "government_debt": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
        "nominal_gdp":     {"latest": {"value": None, "date": None, "source": None}, "series": {}},
        "debt_to_gdp":     {"latest": {"value": None, "date": None, "source": "computed:NA/NA"}, "series": {}},
        "debt_to_gdp_series": {},
    }

    # --- trim helper
    def _trim_series(d: Dict[str, float]) -> Dict[str, float]:
        if not d:
            return {}
        try:
            keys = sorted(d.keys())
        except Exception:
            keys = list(d.keys())
        if series == "none":
            last = keys[-1]
            return {last: d[last]}
        if series == "mini":
            keys = keys[-min(len(keys), keep):]
        elif keep and series == "full" and len(keys) > keep:
            keys = keys[-keep:]
        return {k: d[k] for k in keys}

    # --- indicators via compat (IMF/WB/ECB bridge)
    try:
        from app.providers import compat as compat
        tried: Dict[str, Any] = {}

        def _fill(key: str, getter_name: str, src_label: str):
            getter = getattr(compat, getter_name, None)
            tried.setdefault(key, []).append({getter_name: bool(callable(getter))})
            if not callable(getter):
                return
            series_map = getter(country) or {}
            series_map = _trim_series(series_map)
            if not series_map:
                return
            latest_key = sorted(series_map.keys())[-1]
            latest_val = series_map[latest_key]
            payload["indicators"][key]["series"] = series_map
            payload["indicators"][key]["latest_period"] = latest_key
            payload["indicators"][key]["latest_value"] = latest_val
            payload["indicators"][key]["source"] = src_label

        _fill("cpi_yoy",           "get_cpi_yoy_monthly",           "IMF")
        _fill("unemployment_rate", "get_unemployment_rate_monthly",  "IMF")
        _fill("fx_rate_usd",       "get_fx_rate_usd_monthly",        "IMF")
        _fill("reserves_usd",      "get_reserves_usd_monthly",       "IMF")
        _fill("policy_rate",       "get_policy_rate_monthly",        "IMF/ECB")
        _fill("gdp_growth",        "get_gdp_growth_quarterly",       "IMF")

        payload["_debug"]["source_trace"].update({
            "cpi_yoy":           {"compat_imf": {"module": compat.__name__, "tried": tried.get("cpi_yoy", [])}},
            "unemployment_rate": {"compat_imf": {"module": compat.__name__, "tried": tried.get("unemployment_rate", [])}},
            "fx_rate_usd":       {"compat_imf": {"module": compat.__name__, "tried": tried.get("fx_rate_usd", [])}},
            "reserves_usd":      {"compat_imf": {"module": compat.__name__, "tried": tried.get("reserves_usd", [])}},
            "policy_rate":       {"compat_imf": {"module": compat.__name__, "tried": tried.get("policy_rate", [])}},
            "gdp_growth":        {"compat_imf": {"module": compat.__name__, "tried": tried.get("gdp_growth", [])}},
        })
    except Exception as e:
        payload["_debug"]["notes"].append(f"indicator_error:{type(e).__name__}:{e}")

    # --- merge debt bundle if available
    try:
        if _compute_debt_payload is None:
            raise RuntimeError("debt_service_unavailable")

        debt = _compute_debt_payload(country)

        # Accept several shapes
        if isinstance(debt, dict):
            # direct keys if present
            for key in ("government_debt", "nominal_gdp", "debt_to_gdp", "debt_to_gdp_series"):
                if key in debt and isinstance(debt[key], dict):
                    payload[key] = debt[key]

            # common simple shape
            if "latest" in debt and "series" in debt and not payload["debt_to_gdp"]["series"]:
                payload["debt_to_gdp"] = {
                    "latest": {
                        "value": (debt.get("latest") or {}).get("value"),
                        "date":  (debt.get("latest") or {}).get("year") or (debt.get("latest") or {}).get("date"),
                        "source": (debt.get("latest") or {}).get("source"),
                    },
                    "series": debt.get("series") or {},
                }
                payload["debt_to_gdp_series"] = debt.get("series") or {}
        else:
            payload["_debug"]["notes"].append("debt_payload_unrecognized_shape")

        # trim annual series in debt blocks
        def _trim_annual(block: Dict[str, Any], field: str):
            if field not in block or not isinstance(block[field], dict):
                return
            d = block[field]
            keys = sorted(d.keys())
            if series == "none":
                keys = keys[-1:]
            elif series == "mini":
                keys = keys[-min(len(keys), keep):]
            elif keep and series == "full" and len(keys) > keep:
                keys = keys[-keep:]
            block[field] = {k: d[k] for k in keys}

        _trim_annual(payload["debt_to_gdp"], "series")
        _trim_annual(payload["government_debt"], "series")
        _trim_annual(payload["nominal_gdp"], "series")

    except Exception as e:
        payload["_debug"]["notes"].append(f"debt_error:{type(e).__name__}:{e}")

    return payload

# --------------------------- legacy fallback ----------------------------------

def build_country_payload(country: str, series: str = "mini", keep: int = 60) -> Dict[str, Any]:
    """Compatibility wrapper for legacy callers—delegate to v2 with same signature."""
    return build_country_payload_v2(country=country, series=series, keep=keep)
