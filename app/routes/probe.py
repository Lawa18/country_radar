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
