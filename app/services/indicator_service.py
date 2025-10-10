# app/services/indicator_service.py — modern builder (IMF→WB, optional Eurostat),
# series=mini|full, keep=N days, with _debug traces and debt integration.
from __future__ import annotations

import os
import socket
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_period_key(p: str) -> Tuple[int, int, int]:
    """Sort 'YYYY', 'YYYY-MM', 'YYYY-Qn' as (Y,M,Q)."""
    try:
        if "-Q" in p:
            y, q = p.split("-Q", 1)
            return (int(y), 0, int(q))
        if "-" in p:
            y, m = p.split("-", 1)
            return (int(y), int(m), 0)
        return (int(p), 0, 0)
    except Exception:
        return (0, 0, 0)


def _latest(series: Optional[Mapping[str, Any]]) -> Tuple[Optional[str], Optional[float]]:
    if not isinstance(series, Mapping) or not series:
        return None, None
    keys = sorted(series.keys(), key=_parse_period_key)
    k = keys[-1]
    try:
        v = float(series[k])
    except Exception:
        v = None
    return k, v


def _trim_points(series: Optional[Mapping[str, Any]], n_points: int) -> Dict[str, Any]:
    if not isinstance(series, Mapping) or n_points <= 0:
        return {}
    items = sorted(series.items(), key=lambda kv: _parse_period_key(kv[0]))
    if n_points < len(items):
        items = items[-n_points:]
    return {k: v for k, v in items}


def _route_env_flag(name: str, default_true: bool = True) -> bool:
    v = os.getenv(name)
    if v is None:
        return default_true
    return str(v).lower() not in {"0", "false", "no", "off"}


def _host_resolves(host: str) -> bool:
    try:
        socket.getaddrinfo(host, 443)
        return True
    except OSError:
        return False


def _resolve_iso(country: str) -> Dict[str, Optional[str]]:
    result = {"name": country, "iso_alpha_2": None, "iso_alpha_3": None, "iso_numeric": None}
    try:
        from app.utils import country_codes as cc  # type: ignore
        for fn in ("resolve_country", "name_to_iso", "get_iso_codes", "lookup_country", "resolve"):
            f = getattr(cc, fn, None)
            if callable(f):
                try:
                    out = f(country)
                    if isinstance(out, Mapping):
                        result.update({
                            "name": out.get("name", country),
                            "iso_alpha_2": out.get("iso_alpha_2") or out.get("iso2"),
                            "iso_alpha_3": out.get("iso_alpha_3") or out.get("iso3"),
                            "iso_numeric": out.get("iso_numeric") or out.get("isonum"),
                        })
                        return result
                except Exception:
                    pass
    except Exception:
        pass
    return result


# ---------------------------------------------------------------------------
# Provider wrapper (try several function names and kw variants)
# ---------------------------------------------------------------------------

def _call_provider(module_name: str, candidates: Iterable[str], **kwargs) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    dbg: Dict[str, Any] = {"module": module_name, "tried": []}
    try:
        mod = __import__(module_name, fromlist=["*"])  # type: ignore
    except Exception as e:
        dbg["error"] = f"import_failed: {e}"
        return None, dbg

    kw_variants = [kwargs]
    if "country" in kwargs:
        kw = dict(kwargs)
        kw["name"] = kw.pop("country")
        kw_variants.append(kw)
    if "iso2" in kwargs:
        kw = dict(kwargs)
        kw["code"] = kw.pop("iso2")
        kw_variants.append(kw)

    for fn in candidates:
        f = getattr(mod, fn, None)
        if not callable(f):
            dbg["tried"].append({fn: "missing"})
            continue
        for kv in kw_variants:
            try:
                series = f(**kv)
                dbg["tried"].append({fn: {"ok": True}})
                if isinstance(series, Mapping):
                    return dict(series), dbg
                if isinstance(series, (list, tuple)):
                    return {str(i): v for i, v in enumerate(series)}, dbg
                return {"value": series}, dbg
            except Exception as e:
                dbg["tried"].append({fn: {"error": str(e)}})
    return None, dbg


# ---------------------------------------------------------------------------
# Indicator config
# ---------------------------------------------------------------------------

_INDICATORS = {
    "cpi_yoy": {
        "imf": ("get_cpi_yoy_monthly", "get_cpi_yoy"),
        "wb": ("get_cpi_inflation_annual",),
        "eurostat": ("get_hicp_yoy_iso2", "hicp_yoy_iso2", "get_hicp_yoy"),
        "freq": "monthly",
    },
    "unemployment_rate": {
        "imf": ("get_unemployment_rate_monthly", "get_unemployment_rate"),
        "wb": ("get_unemployment_annual",),
        "eurostat": ("get_unemployment_rate_iso2", "unemployment_rate_iso2", "get_unemployment_rate"),
        "freq": "monthly",
    },
    "fx_rate_usd": {
        "imf": ("get_fx_rate_usd",),
        "wb": ("get_fx_official_annual",),
        "freq": "monthly",
    },
    "reserves_usd": {
        "imf": ("get_reserves_usd",),
        "wb": ("get_reserves_annual",),
        "freq": "monthly",
    },
    "policy_rate": {
        "imf": ("get_policy_rate",),
        "wb": (),
        "freq": "monthly",
    },
    "gdp_growth": {
        "imf": ("get_gdp_growth_quarterly",),
        "wb": ("get_gdp_growth_annual",),
        "freq": "quarterly",
    },
}

_MINI_SET = ("cpi_yoy", "unemployment_rate", "fx_rate_usd", "reserves_usd", "policy_rate", "gdp_growth")


def _points_for_keep(freq: str, keep_days: int) -> int:
    if keep_days <= 0:
        return 0
    if freq == "monthly":
        # ~30 days each
        return max(1, round(keep_days / 30))
    if freq == "quarterly":
        return max(1, round(keep_days / 90))
    # annual or unknown
    return max(1, round(keep_days / 365))


# ---------------------------------------------------------------------------
# Public API: modern builder
# ---------------------------------------------------------------------------

def build_country_payload_v2(country: str, series: str = "full", keep: int = 180) -> Dict[str, Any]:
    """Modern builder with IMF primary, WB fallback, optional Eurostat.

    * series: 'mini' for a lighter payload, 'full' for all configured indicators
    * keep: trims historical points to the last N days worth (approx by freq)
    """
    iso = _resolve_iso(country)
    eurostat_enabled = _route_env_flag("EUROSTAT_ENABLED", True)
    eurostat_host = os.getenv("EUROSTAT_HOST", "data-api.ec.europa.eu")
    eurostat_ok = eurostat_enabled and _host_resolves(eurostat_host)

    chosen = _MINI_SET if str(series).lower() == "mini" else tuple(_INDICATORS.keys())

    indicators: Dict[str, Any] = {}
    source_trace: Dict[str, Any] = {}

    for key in chosen:
        cfg = _INDICATORS[key]
        freq = cfg.get("freq", "monthly")

        # IMF first
        ser, dbg_imf = _call_provider("app.providers.imf_provider", cfg["imf"], country=country)
        src = None
        if isinstance(ser, Mapping) and ser:
            src = "IMF"
        else:
            # Eurostat (if viable and defined for this indicator)
            ser = None
            dbg_es = {"skipped": True}
            if eurostat_ok and cfg.get("eurostat"):
                ser, dbg_es = _call_provider("app.providers.eurostat_provider", cfg["eurostat"], iso2=iso.get("iso_alpha_2") or "")
                if isinstance(ser, Mapping) and ser:
                    src = "Eurostat"
            # World Bank fallback (annual)
            if not src:
                ser, dbg_wb = _call_provider("app.providers.wb_provider", cfg["wb"], country=country)
                if isinstance(ser, Mapping) and ser:
                    src = "WorldBank"
                source_trace[key] = {"imf": dbg_imf, "eurostat": (dbg_es if eurostat_ok else {"enabled": False}), "wb": (dbg_wb if 'dbg_wb' in locals() else {})}
            else:
                source_trace[key] = {"imf": dbg_imf, "eurostat": (dbg_es if eurostat_ok else {"enabled": False}), "wb": {}}
        if src == "IMF":
            source_trace[key] = {"imf": dbg_imf, "eurostat": {"skipped": not eurostat_ok}, "wb": {}}

        # Normalize & trim
        trimmed: Dict[str, Any] = {}
        if isinstance(ser, Mapping):
            # coerce to float where possible
            _ser = {}
            for k, v in ser.items():
                try:
                    _ser[str(k)] = float(v)
                except Exception:
                    _ser[str(k)] = v
            trimmed = _trim_points(_ser, _points_for_keep(freq, keep))

        latest_period, latest_value = _latest(trimmed or ser)
        indicators[key] = {
            "series": trimmed,
            "latest_period": latest_period,
            "latest_value": latest_value,
            "source": src,
            "freq": freq,
        }

    # Debt integration (reuse existing helper without changing its schema)
    debt_payload = None
    try:
        from app.routes.debt import compute_debt_payload  # type: ignore
        debt_payload = compute_debt_payload(country=country)
    except Exception:
        try:
            from app.services.indicator_service import compute_debt_payload  # type: ignore
            if callable(compute_debt_payload):
                debt_payload = compute_debt_payload(country=country)
        except Exception:
            debt_payload = None

    out: Dict[str, Any] = {
        "ok": True,
        "country": country,
        "iso_codes": iso,
        "series_mode": series,
        "keep_days": keep,
        "indicators": indicators,
        "_debug": {
            "builder": {
                "used": "build_country_payload_v2",
                "module": __name__,
            },
            "source_trace": source_trace,
            "eurostat": {"enabled": eurostat_enabled, "host": eurostat_host, "dns": bool(eurostat_ok)},
        },
    }

    if isinstance(debt_payload, Mapping):
        for key in ("government_debt", "nominal_gdp", "debt_to_gdp", "debt_to_gdp_series"):
            if key in debt_payload:
                out[key] = debt_payload[key]

    return out


# ---------------------------------------------------------------------------
# Legacy compatibility shim (optional): if older code imports build_country_payload
# ---------------------------------------------------------------------------

def build_country_payload(country: str, series: str = "full", keep: int = 180) -> Dict[str, Any]:
    """Compatibility wrapper to the modern builder."""
    return build_country_payload_v2(country=country, series=series, keep=keep)
