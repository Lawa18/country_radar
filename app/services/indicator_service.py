from __future__ import annotations
from typing import Dict, Any, Optional
from app.utils.country_codes import resolve_country_codes
from app.providers.ecb_provider import EURO_AREA_ISO2, ecb_mro_latest_block

# Providers
from app.providers.wb_provider import fetch_worldbank_data, wb_year_dict_from_raw
from app.providers.imf_provider import (
    fetch_imf_sdmx_series,
    imf_cpi_yoy_monthly,
    imf_fx_to_usd_monthly,
    imf_reserves_usd_monthly,
    imf_unemployment_rate_monthly,
    imf_policy_rate_monthly,
)
from app.providers.eurostat_provider import (
    eurostat_hicp_yoy_monthly,
    eurostat_unemployment_rate_monthly,
)
from app.providers.ecb_provider import ecb_mro_latest_block

# --- Country groups ---
EURO_AREA_ISO2 = {
    "AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE","EL","GR","HU","IE","IT",
    "LV","LT","LU","MT","NL","PL","PT","RO","SK","SI","ES","SE","IS","NO","LI","CH","UK"
}
# ECB MRO applies to euro area members only:
ECB_EA_ISO2 = {
    "AT","BE","CY","EE","FI","FR","DE","EL","GR","IE","IT","LV","LT","LU","MT","NL","PT","SK","SI","ES",
}

# --- helpers for shaping series into our output blocks ---

def _latest_from_series(series: Dict[str, float]) -> Optional[Dict[str, Any]]:
    if not series:
        return None
    last_key = sorted(series.keys())[-1]
    return {"value": series[last_key], "date": last_key}

def _series_block(series: Dict[str, float], source: str) -> Dict[str, Any]:
    latest = _latest_from_series(series)
    if latest:
        latest["source"] = source
    else:
        latest = {"value": None, "date": None, "source": None}
    return {"latest": latest, "series": series}

def wb_entry(raw: Optional[list]) -> Optional[Dict[str, Any]]:
    d = wb_year_dict_from_raw(raw)
    if not d:
        return None
    last_year = sorted(d.keys())[-1]
    return {"value": d[last_year], "date": last_year, "source": "World Bank WDI"}

def wb_series(raw: Optional[list]) -> Optional[Dict[str, Any]]:
    d = wb_year_dict_from_raw(raw)
    if not d:
        return None
    latest = _latest_from_series(d)
    latest["source"] = "World Bank WDI"
    return {"latest": latest, "series": d}

# --- main payload builder used by /country-data route ---

def build_country_payload(country: str) -> Dict[str, Any]:
    codes = resolve_country_codes(country)
    if not codes:
        return {"error": "Invalid country name", "country": country}

    iso2 = codes["iso_alpha_2"]
    iso3 = codes["iso_alpha_3"]

    # prefetch WB raw once (used for fallbacks and annual-only)
    wb_raw = fetch_worldbank_data(iso2, iso3)

    # IMF bundle (monthly) – not used directly, but available if you want
    imf_bundle = fetch_imf_sdmx_series(iso2)

    # --- CPI (prefer Eurostat monthly -> IMF monthly -> WB annual) ---
    cpi_series = eurostat_hicp_yoy_monthly(iso2, start="2019-01")
    if cpi_series:
        cpi_block = _series_block(cpi_series, "Eurostat HICP (YoY, monthly)")
    else:
        cpi_series = imf_cpi_yoy_monthly(iso2, start="2019-01")
        if cpi_series:
            cpi_block = _series_block(cpi_series, "IMF IFS (CPI YoY, monthly)")
        else:
            cpi_block = {"latest": wb_entry(wb_raw.get("FP.CPI.TOTL.ZG")) or {"value": None, "date": None, "source": None}, "series": {}}

    # --- Unemployment (prefer Eurostat monthly -> IMF monthly -> WB annual) ---
    unemp_series = eurostat_unemployment_rate_monthly(iso2, start="2019-01")
    if unemp_series:
        unemp_block = _series_block(unemp_series, "Eurostat (Unemployment, SA, monthly)")
    else:
        unemp_series = imf_unemployment_rate_monthly(iso2, start="2019-01")
        if unemp_series:
            unemp_block = _series_block(unemp_series, "IMF IFS (Unemployment, monthly)")
        else:
            unemp_block = {"latest": wb_entry(wb_raw.get("SL.UEM.TOTL.ZS")) or {"value": None, "date": None, "source": None}, "series": {}}

    # --- Policy Rate (ECB for euro area -> IMF -> N/A) ---
    if iso2 in ECB_EA_ISO2:
        ir_block = ecb_mro_latest_block()
        # ir_block already has {"latest": {...}, "series": {...}}
        if not ir_block.get("latest") or ir_block["latest"].get("value") is None:
            # try IMF policy monthly if ECB failed (some non-euro EU members)
            pol_series = imf_policy_rate_monthly(iso2, start="2019-01")
            ir_block = _series_block(pol_series, "IMF IFS (Policy Rate, monthly)") if pol_series else {"latest": {"value": None, "date": None, "source": None}, "series": {}}
    else:
        pol_series = imf_policy_rate_monthly(iso2, start="2019-01")
        ir_block = _series_block(pol_series, "IMF IFS (Policy Rate, monthly)") if pol_series else {"latest": {"value": None, "date": None, "source": None}, "series": {}}

    # --- FX rate to USD (IMF monthly -> WB annual) ---
    fx_series = imf_fx_to_usd_monthly(iso2, start="2019-01")
    fx_block = _series_block(fx_series, "IMF IFS (FX to USD, monthly)") if fx_series else \
               {"latest": wb_entry(wb_raw.get("PA.NUS.FCRF")) or {"value": None, "date": None, "source": None}, "series": {}}

    # --- Reserves USD (IMF monthly -> WB annual) ---
    res_series = imf_reserves_usd_monthly(iso2, start="2019-01")
    res_block = _series_block(res_series, "IMF IFS (Reserves USD, monthly)") if res_series else \
                {"latest": wb_entry(wb_raw.get("FI.RES.TOTL.CD")) or {"value": None, "date": None, "source": None}, "series": {}}

    # --- Annual indicators from WB (good coverage) ---
    gdp_growth = wb_entry(wb_raw.get("NY.GDP.MKTP.KD.ZG")) or {"value": None, "date": None, "source": None}
    cab_gdp = wb_entry(wb_raw.get("BN.CAB.XOKA.GD.ZS")) or {"value": None, "date": None, "source": None}
    gov_eff = wb_entry(wb_raw.get("GE.EST")) or {"value": None, "date": None, "source": None}

    # Build response: keep your previous JSON shape
    imf_data = {
        "CPI": cpi_block,
        "FX Rate": fx_block,
        "Interest Rate (Policy)": ir_block,
        "Reserves (USD)": res_block,
        "GDP Growth (%)": gdp_growth,
        "Unemployment (%)": unemp_block["latest"],  # for table's 'latest'; series kept below
        "Current Account Balance (% of GDP)": cab_gdp,
        "Government Effectiveness": gov_eff,
    }

    # Attach unemployment series into an 'additional_indicators' if you want,
    # or keep it inside imf_data as a full block. We'll keep full blocks consistent:
    imf_data["Unemployment (%)"] = unemp_block["latest"]

    # Final payload
    return {
        "country": country,
        "iso_codes": codes,
        "imf_data": imf_data,
        # You can also include full series under additional_indicators if your UI renders charts
        "additional_indicators": {
            "CPI_series": cpi_block["series"],
            "Unemployment_series": unemp_block["series"],
            "PolicyRate_series": ir_block["series"],
            "FX_series": fx_block["series"],
            "Reserves_series": res_block["series"],
        }
    }
