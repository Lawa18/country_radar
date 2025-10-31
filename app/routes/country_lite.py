from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Tuple
import time as _time

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

router = APIRouter(tags=["country-lite"])

HIST_POLICY = {"A": 20, "Q": 4, "M": 12}
_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_TTL = 600.0  # 10 minutes


def _cache_get(k: str) -> Optional[Dict[str, Any]]:
    row = _CACHE.get(k.lower())
    if not row:
        return None
    ts, payload = row
    if _time.time() - ts > _TTL:
        return None
    return payload


def _cache_set(k: str, payload: Dict[str, Any]) -> None:
    _CACHE[k.lower()] = (_time.time(), payload)


def _safe_import(module: str):
    try:
        return __import__(module, fromlist=["*"])
    except Exception:
        return None


def _latest(d: Mapping[str, float]) -> Tuple[Optional[str], Optional[float]]:
    if not d:
        return None, None
    ks = sorted(d.keys())
    k = ks[-1]
    return k, float(d[k])


@router.get("/v1/country-lite", summary="Country Lite (minimal, fast)")
def country_lite(country: str = Query(..., description="Full country name, e.g., Mexico")) -> Dict[str, Any]:
    # 0) cache
    cached = _cache_get(country)
    if cached:
        return JSONResponse(content=cached)

    # 1) resolve ISO
    iso = {"name": country, "iso_alpha_2": None, "iso_alpha_3": None, "iso_numeric": None}
    cc_mod = _safe_import("app.utils.country_codes")
    if cc_mod and hasattr(cc_mod, "get_country_codes"):
        try:
            codes = cc_mod.get_country_codes(country) or {}
            iso = {
                "name": codes.get("name") or country,
                "iso_alpha_2": codes.get("iso_alpha_2"),
                "iso_alpha_3": codes.get("iso_alpha_3"),
                "iso_numeric": codes.get("iso_numeric"),
            }
        except Exception:
            pass

    # 2) debt (re-use your working service)
    try:
        from app.services.debt_service import compute_debt_payload
        debt = compute_debt_payload(country) or {}
    except Exception:
        debt = {}

    debt_series = debt.get("series") or {}
    debt_latest = debt.get("latest") or {"year": None, "value": None, "source": "unavailable"}

    # 3) indicators — but **super** defensive
    addl: Dict[str, Any] = {}

    compat = _safe_import("app.providers.compat")
    if compat:
        def _c(fn_name: str, keep: int = 24) -> Dict[str, float]:
            fn = getattr(compat, fn_name, None)
            if not callable(fn):
                return {}
            try:
                data = fn(country=country, series="mini", keep=keep) or {}
            except TypeError:
                try:
                    data = fn(country=country) or {}
                except Exception:
                    data = {}
            # trim naïvely
            items = sorted(data.items())[-keep:]
            return {str(k): float(v) for k, v in items if v is not None}

        cpi = _c("get_cpi_yoy_monthly", 12)
        up  = _c("get_unemployment_rate_monthly", 12)
        fx  = _c("get_fx_rate_usd_monthly", 12)

        cpi_p, cpi_v = _latest(cpi)
        up_p, up_v   = _latest(up)
        fx_p, fx_v   = _latest(fx)

        if cpi:
            addl["cpi_yoy"] = {
                "latest_value": cpi_v,
                "latest_period": cpi_p,
                "source": "compat",
                "series": cpi,
            }
        if up:
            addl["unemployment_rate"] = {
                "latest_value": up_v,
                "latest_period": up_p,
                "source": "compat",
                "series": up,
            }
        if fx:
            addl["fx_rate_usd"] = {
                "latest_value": fx_v,
                "latest_period": fx_p,
                "source": "compat",
                "series": fx,
            }

    resp = {
        "country": country,
        "iso_codes": iso,
        "latest": {
            "year": debt_latest.get("year"),
            "value": debt_latest.get("value"),
            "source": debt_latest.get("source"),
        },
        "series": debt_series,
        "source": debt_latest.get("source"),
        "imf_data": {},
        "government_debt": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
        "nominal_gdp": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
        "debt_to_gdp": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
        "debt_to_gdp_series": {},
        "additional_indicators": addl,
        "debug": {
            "note": "minimal country-lite",
        },
    }

    _cache_set(country, resp)
    return JSONResponse(content=resp)
