from __future__ import annotations
from fastapi import APIRouter, Query
from typing import Dict, Any, Optional, Tuple

from app.utils.country_codes import resolve_country_codes
from app.providers.imf_provider import (
    imf_cpi_yoy_monthly, imf_unemployment_rate_monthly,
    imf_fx_usd_monthly, imf_reserves_usd_monthly, imf_policy_rate_monthly,
    imf_gdp_growth_quarterly,
)
from app.providers.eurostat_provider import (
    eurostat_hicp_yoy_monthly, eurostat_unemployment_rate_monthly,
)
from app.providers.wb_provider import (
    wb_cpi_yoy_annual, wb_unemployment_rate_annual, wb_fx_rate_usd_annual,
    wb_reserves_usd_annual, wb_gdp_growth_annual_pct,
)

# NEW: used by /v1/country-lite response
from fastapi.responses import JSONResponse

router = APIRouter()

def _latest_key(d: Dict[str, float]) -> Optional[str]:
    if not d: return None
    try:
        return max(d.keys())
    except Exception:
        return None

@router.get("/__probe_series")
def probe_series(country: str = Query(...)):
    codes = resolve_country_codes(country)
    if not codes:
        return {"ok": False, "error": "invalid_country"}
    iso2, iso3 = codes["iso_alpha_2"], codes["iso_alpha_3"]

    out: Dict[str, Any] = {"country": country, "iso2": iso2, "iso3": iso3, "series": {}}

    # CPI
    imf_cpi = imf_cpi_yoy_monthly(iso2) or {}
    eu_cpi  = eurostat_hicp_yoy_monthly(iso2) or {}
    wb_cpi  = wb_cpi_yoy_annual(iso3) or {}
    out["series"]["cpi"] = {
        "IMF": {"len": len(imf_cpi), "latest": _latest_key(imf_cpi)},
        "Eurostat": {"len": len(eu_cpi), "latest": _latest_key(eu_cpi)},
        "WB_annual": {"len": len(wb_cpi), "latest": _latest_key(wb_cpi)},
    }

    # Unemployment
    imf_u = imf_unemployment_rate_monthly(iso2) or {}
    eu_u  = eurostat_unemployment_rate_monthly(iso2) or {}
    wb_u  = wb_unemployment_rate_annual(iso3) or {}
    out["series"]["unemployment"] = {
        "IMF": {"len": len(imf_u), "latest": _latest_key(imf_u)},
        "Eurostat": {"len": len(eu_u), "latest": _latest_key(eu_u)},
        "WB_annual": {"len": len(wb_u), "latest": _latest_key(wb_u)},
    }

    # FX
    imf_fx = imf_fx_usd_monthly(iso2) or {}
    wb_fx  = wb_fx_rate_usd_annual(iso3) or {}
    out["series"]["fx"] = {
        "IMF": {"len": len(imf_fx), "latest": _latest_key(imf_fx)},
        "WB_annual": {"len": len(wb_fx), "latest": _latest_key(wb_fx)},
    }

    # Reserves
    imf_r = imf_reserves_usd_monthly(iso2) or {}
    wb_r  = wb_reserves_usd_annual(iso3) or {}
    out["series"]["reserves"] = {
        "IMF": {"len": len(imf_r), "latest": _latest_key(imf_r)},
        "WB_annual": {"len": len(wb_r), "latest": _latest_key(wb_r)},
    }

    # Policy rate
    imf_pol = imf_policy_rate_monthly(iso2) or {}
    out["series"]["policy_rate"] = {
        "IMF": {"len": len(imf_pol), "latest": _latest_key(imf_pol)},
        # ECB override happens higher up; we only need to see if IMF has data here
    }

    # GDP growth
    imf_gdp_q = imf_gdp_growth_quarterly(iso2) or {}
    wb_gdp_a  = wb_gdp_growth_annual_pct(iso3) or {}
    out["series"]["gdp_growth"] = {
        "IMF_quarterly": {"len": len(imf_gdp_q), "latest": _latest_key(imf_gdp_q)},
        "WB_annual": {"len": len(wb_gdp_a), "latest": _latest_key(wb_gdp_a)},
    }

    return {"ok": True, **out}

# ---------------------- appended endpoints (keep existing code above) --------

@router.get("/__action_probe", summary="Connectivity probe")
def __action_probe():
    return {"ok": True, "path": "/__action_probe"}

@router.get("/v1/country-lite")
def country_lite(country: str = Query(..., description="Full country name, e.g., Germany")):
    """
    Prefer the modern monthly-first builder; fall back only if not present.
    """
    try:
        from app.services import indicator_service as _svc
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"indicator_service import failed: {e}"}, status_code=500)

    # 1) Strong preference: the monthly-first builder you’ve been iterating on
    PREFERRED = ("build_country_payload",)

    # 2) Other plausible lite/full builders (in decreasing preference)
    FALLBACKS = (
        "get_country_lite","country_lite","assemble_country_lite",
        "build_country_lite","get_country_compact","country_compact",
        "country_data","build_country_data","assemble_country_data","get_country_data","make_country_data",
    )

    payload = None

    # Try preferred first
    for name in PREFERRED + FALLBACKS:
        f = getattr(_svc, name, None)
        if not callable(f):
            continue
        try:
            # Most of your builders accept country= and series=; try that first.
            try:
                payload = f(country=country, series="none")
            except TypeError:
                payload = f(country)  # older signatures
        except Exception as e:
            return JSONResponse({"ok": False, "error": f"{name} failed: {e}"}, status_code=500)
        else:
            break

    if payload is None:
        return JSONResponse({"ok": False, "error": "No lite builder found and no full builder fallback available."}, status_code=500)

    if not isinstance(payload, dict):
        payload = {"result": payload}
    payload.setdefault("country", country)
    return JSONResponse(payload)
