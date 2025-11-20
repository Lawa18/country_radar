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
            from typing import Mapping as _Mapping  # avoid mypy confusion
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
    Modern builder for Country Radar:
    - Compat-first IMF monthly/quarterly indicators (via app.providers.compat)
    - Optional debt bundle via app.services.debt_service.compute_debt_payload
    - Controlled history windows per indicator, clamped by `keep`

    `series`:
      - "none": only latest point per indicator
      - "mini": short history (per-indicator policy, e.g. 36m FX/CPI)
      - "full": up to `keep` points if `keep` > mini policy
    """
    # --- base ISO resolution
    iso = _iso_codes(country)

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
        "keep_points": keep,
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

    # --- per-indicator history policy (mini/full caps), clamped by `keep`
    # These are *soft* caps; the effective limit is min(policy, keep) where applicable.
    history_policy = {
        "cpi_yoy":          {"freq": "M", "mini": 36, "full": keep},
        "unemployment_rate":{"freq": "M", "mini": 36, "full": keep},
        "fx_rate_usd":      {"freq": "M", "mini": 36, "full": keep},
        "reserves_usd":     {"freq": "M", "mini": 36, "full": keep},
        "policy_rate":      {"freq": "M", "mini": 48, "full": keep},
        "gdp_growth":       {"freq": "Q", "mini": 8,  "full": keep},
    }

    def _trim_series_for_indicator(name: str, d: Dict[str, float]) -> Dict[str, float]:
        """Apply series-mode + per-indicator history policy with robust sorting."""
        if not d:
            return {}
        try:
            keys = sorted(d.keys(), key=_parse_period_key)
        except Exception:
            keys = list(d.keys())

        if not keys:
            return {}

        if series == "none":
            last = keys[-1]
            return {last: d[last]}

        policy = history_policy.get(name, {"mini": keep, "full": keep})
        if series == "mini":
            k_limit = min(policy.get("mini", keep), keep) if keep else policy.get("mini", keep)
        else:  # "full"
            k_limit = keep or policy.get("full", keep)

        if k_limit and len(keys) > k_limit:
            keys = keys[-k_limit:]
        return {k: d[k] for k in keys}

    # --- tiny helper for setting indicator blocks
    def _set_indicator_block(key: str, series_map: Dict[str, float], source_label: str) -> None:
        block = payload["indicators"][key]
        trimmed = _trim_series_for_indicator(key, series_map)
        if not trimmed:
            return
        latest_key, latest_val = _latest(trimmed)
        block["series"] = trimmed
        block["latest_period"] = latest_key
        block["latest_value"] = latest_val
        block["source"] = source_label

    # --- indicators via compat (IMF/WB/ECB bridge)
    try:
        from app.providers import compat as compat
        tried: Dict[str, Any] = {}

        def _fill(key: str, getter_name: str, src_label: str):
            getter = getattr(compat, getter_name, None)
            tried.setdefault(key, []).append({getter_name: bool(callable(getter))})
            if not callable(getter):
                return
            try:
                raw_series = getter(country) or {}
            except Exception as e:
                tried[key].append({getter_name: {"error": f"{type(e).__name__}: {e}"}})
                return
            # Compat returns already-normalized series; just trim + set
            numeric_series = _coerce_numeric_series(raw_series)
            if not numeric_series:
                return
            _set_indicator_block(key, numeric_series, src_label)

        _fill("cpi_yoy",           "get_cpi_yoy_monthly",           "compat/IMF")
        _fill("unemployment_rate", "get_unemployment_rate_monthly", "compat/IMF")
        _fill("fx_rate_usd",       "get_fx_rate_usd_monthly",       "compat/IMF")
        _fill("reserves_usd",      "get_reserves_usd_monthly",      "compat/IMF")
        _fill("policy_rate",       "get_policy_rate_monthly",       "compat/IMF/ECB")
        _fill("gdp_growth",        "get_gdp_growth_quarterly",      "compat/IMF")

        payload["_debug"]["source_trace"].update({
            "cpi_yoy":           {"compat": {"module": compat.__name__, "tried": tried.get("cpi_yoy", [])}},
            "unemployment_rate": {"compat": {"module": compat.__name__, "tried": tried.get("unemployment_rate", [])}},
            "fx_rate_usd":       {"compat": {"module": compat.__name__, "tried": tried.get("fx_rate_usd", [])}},
            "reserves_usd":      {"compat": {"module": compat.__name__, "tried": tried.get("reserves_usd", [])}},
            "policy_rate":       {"compat": {"module": compat.__name__, "tried": tried.get("policy_rate", [])}},
            "gdp_growth":        {"compat": {"module": compat.__name__, "tried": tried.get("gdp_growth", [])}},
        })
    except Exception as e:
        payload["_debug"]["notes"].append(f"indicator_error:{type(e).__name__}:{e}")

    # --- merge debt bundle if available
    try:
        if _compute_debt_payload is None:
            raise RuntimeError("debt_service_unavailable")

        debt = _compute_debt_payload(country)

        if isinstance(debt, dict):
            # direct keys if present (newer debt_service versions)
            for key in ("government_debt", "nominal_gdp", "debt_to_gdp", "debt_to_gdp_series"):
                if key in debt and isinstance(debt[key], dict):
                    payload[key] = debt[key]

            # common simple shape: {"latest": {...}, "series": {...}}
            if "latest" in debt and "series" in debt and not payload["debt_to_gdp"]["series"]:
                latest_block = debt.get("latest") or {}
                series_block = debt.get("series") or {}
                payload["debt_to_gdp"] = {
                    "latest": {
                        "value": latest_block.get("value"),
                        "date":  latest_block.get("year") or latest_block.get("date"),
                        "source": latest_block.get("source"),
                    },
                    "series": series_block,
                }
                payload["debt_to_gdp_series"] = series_block
        else:
            payload["_debug"]["notes"].append("debt_payload_unrecognized_shape")

        # trim annual series in debt blocks using same `keep` semantics
        def _trim_annual(block: Dict[str, Any], field: str):
            ser = block.get(field)
            if not isinstance(ser, dict) or not ser:
                return
            keys = sorted(ser.keys(), key=_parse_period_key)
            if not keys:
                return
            if series == "none":
                keys = keys[-1:]
            elif series == "mini":
                # annual mini: up to 20y or keep, whichever is smaller
                annual_keep = min(20, keep) if keep else 20
                if len(keys) > annual_keep:
                    keys = keys[-annual_keep:]
            elif keep and series == "full" and len(keys) > keep:
                keys = keys[-keep:]
            block[field] = {k: ser[k] for k in keys}

        _trim_annual(payload["debt_to_gdp"], "series")
        _trim_annual(payload["government_debt"], "series")
        _trim_annual(payload["nominal_gdp"], "series")

    except Exception as e:
        payload["_debug"]["notes"].append(f"debt_error:{type(e).__name__}:{e}")

    return payload

# --------------------------- legacy fallback ----------------------------------

def build_country_payload(country: str, series: str = "mini", keep: int = 60) -> Dict[str, Any]:
    """Compatibility wrapper for legacy callers—delegate to v2 with same signature."""
    # keep type-safety for `series` by constraining to allowed literals
    mode: Literal["none", "mini", "full"]
    if series not in ("none", "mini", "full"):
        mode = "mini"
    else:
        mode = series  # type: ignore[assignment]
    return build_country_payload_v2(country=country, series=mode, keep=keep)
