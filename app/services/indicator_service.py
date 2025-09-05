from __future__ import annotations
from typing import Dict, Any
from fastapi.responses import JSONResponse

from app.utils.country_codes import resolve_country_codes
from app.utils.series_math import latest, yoy_from_index, mom_from_index, first_non_empty
from app.providers.eurostat_provider import (
    eurostat_debt_to_gdp_annual,  # keep yours if present
    hicp_index_monthly,
    unemployment_rate_monthly,
)
from app.providers.ecb_provider import ecb_mro_monthly
from app.providers.imf_provider import (
    ifs_cpi_index_monthly,
    ifs_unemployment_rate_monthly,
    ifs_fx_lcu_per_usd_monthly,
    ifs_reserves_usd_monthly,
    ifs_gdp_growth_quarterly,
    ifs_ca_percent_gdp,
    ifs_policy_rate_monthly,
)
from app.providers.wb_provider import (
    fetch_worldbank_data, wb_series, wb_entry, wb_year_dict_from_raw
)
from app.services.debt_service import compute_debt_payload

EURO_AREA_ISO2 = {"AT","BE","CY","DE","EE","ES","FI","FR","GR","HR","IE","IT","LT","LU","LV","MT","NL","PT","SI","SK"}

def _block_from_series_pct(series: Dict[str, float], source: str) -> Dict[str, Any]:
    if not series:
        return {"latest": {"value": None, "date": None, "source": None}, "series": {}}
    k, v = latest(series) or (None, None)
    return {
        "latest": {"value": v, "date": k, "source": source},
        "series": series,
    }

def build_country_payload(country: str) -> Dict[str, Any]:
    codes = resolve_country_codes(country)
    if not codes:
        return {"error": "Invalid country name", "country": country}
    iso2, iso3 = codes["iso_alpha_2"], codes["iso_alpha_3"]

    # --- Base WB batch (annual fallbacks)
    wb = fetch_worldbank_data(iso2, iso3)

    # 1) CPI YoY (prefer monthly from index → fallback to WB annual %)
    es_cpi_idx = hicp_index_monthly(iso2)  # {}
    ifs_cpi_idx = ifs_cpi_index_monthly(iso3)  # {}
    monthly_cpi_yoy = first_non_empty(
        yoy_from_index(es_cpi_idx),
        yoy_from_index(ifs_cpi_idx),
    )
    cpi_block = _block_from_series_pct(monthly_cpi_yoy, "Eurostat/IMF IFS") if monthly_cpi_yoy else {
        "latest": wb_entry(wb.get("FP.CPI.TOTL.ZG")) or {"value": None, "date": None, "source": None},
        "series": (wb_series(wb.get("FP.CPI.TOTL.ZG")) or {}).get("series", {}),
    }
    # Optional CPI MoM (only if index monthly exists)
    cpi_mom = first_non_empty(
        mom_from_index(es_cpi_idx),
        mom_from_index(ifs_cpi_idx),
    )
    if cpi_mom:
        cpi_block["mom_series"] = cpi_mom

    # 2) Unemployment (monthly preferred)
    es_ur = unemployment_rate_monthly(iso2)
    ifs_ur = ifs_unemployment_rate_monthly(iso3)
    unemp_series = first_non_empty(es_ur, ifs_ur)
    unemp_block = _block_from_series_pct(unemp_series, "Eurostat/IMF IFS") if unemp_series else {
        "latest": wb_entry(wb.get("SL.UEM.TOTL.ZS")) or {"value": None, "date": None, "source": None},
        "series": (wb_series(wb.get("SL.UEM.TOTL.ZS")) or {}).get("series", {}),
    }

    # 3) Policy Rate (monthly) – ECB override for euro area; else IFS if available
    policy_series = {}
    policy_source = None
    if iso2 in EURO_AREA_ISO2:
        policy_series = ecb_mro_monthly() or {}
        policy_source = "ECB MRO" if policy_series else None
    if not policy_series:
        s = ifs_policy_rate_monthly(iso3)
        if s:
            policy_series = s
            policy_source = "IMF IFS"
    policy_block = _block_from_series_pct(policy_series, policy_source or "—")

    # 4) FX to USD (monthly preferred)
    fx_series = ifs_fx_lcu_per_usd_monthly(iso3) or {}
    fx_block = _block_from_series_pct(fx_series, "IMF IFS") if fx_series else {
        "latest": wb_entry(wb.get("PA.NUS.FCRF")) or {"value": None, "date": None, "source": None},
        "series": (wb_series(wb.get("PA.NUS.FCRF")) or {}).get("series", {}),
    }

    # 5) Reserves USD (monthly preferred)
    res_series = ifs_reserves_usd_monthly(iso3) or {}
    reserves_block = _block_from_series_pct(res_series, "IMF IFS") if res_series else {
        "latest": wb_entry(wb.get("FI.RES.TOTL.CD")) or {"value": None, "date": None, "source": None},
        "series": (wb_series(wb.get("FI.RES.TOTL.CD")) or {}).get("series", {}),
    }

    # 6) GDP Growth % (quarterly if available; else WB annual)
    gdp_q = ifs_gdp_growth_quarterly(iso3) or {}
    gdp_block = _block_from_series_pct(gdp_q, "IMF IFS") if gdp_q else {
        "latest": wb_entry(wb.get("NY.GDP.MKTP.KD.ZG")) or {"value": None, "date": None, "source": None},
        "series": (wb_series(wb.get("NY.GDP.MKTP.KD.ZG")) or {}).get("series", {}),
    }

    # 7) Current Account % GDP (quarterly/annual IFS; else WB annual)
    ca_series = ifs_ca_percent_gdp(iso3) or {}
    ca_block = _block_from_series_pct(ca_series, "IMF IFS") if ca_series else {
        "latest": wb_entry(wb.get("BN.CAB.XOKA.GD.ZS")) or {"value": None, "date": None, "source": None},
        "series": (wb_series(wb.get("BN.CAB.XOKA.GD.ZS")) or {}).get("series", {}),
    }

    # 8) Government Effectiveness (WB annual)
    ge_latest = wb_entry(wb.get("GE.EST")) or {"value": None, "date": None, "source": None}

    # 9) Debt block from your existing service (keep as-is)
    debt_bundle = compute_debt_payload(country) or {}
    debt_pct = debt_bundle.get("debt_to_gdp") or {"value": None, "date": None, "source": None, "government_type": None}
    debt_series = debt_bundle.get("debt_to_gdp_series") or {}
    gov_debt = debt_bundle.get("government_debt")
    nom_gdp  = debt_bundle.get("nominal_gdp")

    # Assemble output (keeps your keys)
    return {
        "country": country,
        "iso_codes": codes,
        "imf_data": {
            "CPI": cpi_block,  # YoY monthly if available; else WB annual
            "FX Rate": fx_block,
            "Interest Rate (Policy)": policy_block,
            "Reserves (USD)": reserves_block,
            "GDP Growth (%)": gdp_block["latest"],  # for backward compat (you had single dict)
            "Unemployment (%)": unemp_block["latest"],
            "Current Account Balance (% of GDP)": ca_block["latest"],
            "Government Effectiveness": ge_latest,
        },
        "government_debt": {"latest": gov_debt or {"value": None,"date": None,"source": None,"government_type": None,"currency": None,"currency_code": None}, "series": {}},
        "nominal_gdp":     {"latest": nom_gdp  or {"value": None,"date": None,"source": None,"currency": None,"currency_code": None}, "series": {}},
        "debt_to_gdp": {
            "latest": debt_pct,
            "series": debt_series,
        },
        "additional_indicators": {}
    }
