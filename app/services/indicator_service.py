from __future__ import annotations
from typing import Any, Dict

# Data providers
from app.providers.wb_provider import fetch_worldbank_data, wb_series, wb_entry
from app.providers.imf_provider import fetch_imf_sdmx_series, imf_series_to_latest_block, imf_series_to_latest_entry
from app.providers.eurostat_provider import fetch_eurostat_indicators, eurostat_series_to_latest_block, eurostat_series_to_latest_entry, EURO_AREA_ISO2
# Debt trio service (preserves your strict order)
from app.services.debt_service import compute_debt_payload
# Country codes
from app.utils.country_codes import resolve_country_codes
# ECB monthly policy-rate override (Euro area style)
from app.providers.ecb_provider import ecb_mro_latest_block

def _empty_latest() -> Dict[str, Any]:
    return {"value": None, "date": None, "source": None}

def _empty_series_block() -> Dict[str, Any]:
    return {"latest": _empty_latest(), "series": {}}

def _merge_indicator_data(imf_data: Dict[str, Dict[str, float]], 
                         eurostat_data: Dict[str, Dict[str, float]], 
                         wb_data: Dict[str, Any], 
                         iso2: str) -> Dict[str, Any]:
    """
    Merge indicator data with priority: IMF > Eurostat (EU only) > World Bank
    Returns the final indicators map for the country payload.
    """
    indicators: Dict[str, Any] = {}
    is_eu_country = iso2.upper() in EURO_AREA_ISO2
    
    # CPI YoY (%)
    if imf_data.get("CPI_YoY"):
        indicators["CPI"] = imf_series_to_latest_block(imf_data["CPI_YoY"], "IMF IFS")
    elif is_eu_country and eurostat_data.get("CPI_YoY"):
        indicators["CPI"] = eurostat_series_to_latest_block(eurostat_data["CPI_YoY"], "Eurostat HICP")
    else:
        indicators["CPI"] = wb_series(wb_data.get("FP.CPI.TOTL.ZG")) or _empty_series_block()

    # FX to USD (LCU per USD)
    if imf_data.get("FX_Rate_USD"):
        indicators["FX Rate"] = imf_series_to_latest_block(imf_data["FX_Rate_USD"], "IMF IFS")
    else:
        indicators["FX Rate"] = wb_series(wb_data.get("PA.NUS.FCRF")) or _empty_series_block()

    # Policy rate – will be overridden by ECB for EU/EEA/UK later
    if imf_data.get("Policy_Rate"):
        indicators["Interest Rate (Policy)"] = imf_series_to_latest_block(imf_data["Policy_Rate"], "IMF IFS")
    else:
        indicators["Interest Rate (Policy)"] = _empty_series_block()

    # Reserves (USD)
    if imf_data.get("Reserves_USD"):
        indicators["Reserves (USD)"] = imf_series_to_latest_block(imf_data["Reserves_USD"], "IMF IFS")
    else:
        indicators["Reserves (USD)"] = wb_series(wb_data.get("FI.RES.TOTL.CD")) or _empty_series_block()

    # Table-only "latest" indicators
    # GDP Growth (%)
    if imf_data.get("GDP_Growth"):
        indicators["GDP Growth (%)"] = imf_series_to_latest_entry(imf_data["GDP_Growth"], "IMF IFS")
    else:
        indicators["GDP Growth (%)"] = wb_entry(wb_data.get("NY.GDP.MKTP.KD.ZG")) or _empty_latest()

    # Unemployment (%)
    if imf_data.get("Unemployment_Rate"):
        indicators["Unemployment (%)"] = imf_series_to_latest_entry(imf_data["Unemployment_Rate"], "IMF IFS")
    elif is_eu_country and eurostat_data.get("Unemployment_Rate"):
        indicators["Unemployment (%)"] = eurostat_series_to_latest_entry(eurostat_data["Unemployment_Rate"], "Eurostat")
    else:
        indicators["Unemployment (%)"] = wb_entry(wb_data.get("SL.UEM.TOTL.ZS")) or _empty_latest()

    # Current Account Balance (% of GDP) - keep World Bank for now as IMF needs conversion
    indicators["Current Account Balance (% of GDP)"] = wb_entry(wb_data.get("BN.CAB.XOKA.GD.ZS")) or _empty_latest()

    # Government Effectiveness - always World Bank
    indicators["Government Effectiveness"] = wb_entry(wb_data.get("GE.EST")) or _empty_latest()

    return indicators

def build_country_payload(country: str) -> Dict[str, Any]:
    """
    Enhanced country data payload that prioritizes IMF and Eurostat data:
      - IMF IFS monthly/quarterly data (when available)
      - Eurostat monthly data for EU countries (when available)  
      - World Bank WDI as fallback
      - ECB MRO monthly override for EU/EEA/UK "Interest Rate (Policy)"
      - Attach Government Debt trio from compute_debt_payload()
    """
    # --- Country codes ---
    codes = resolve_country_codes(country)
    if not codes:
        return {"error": "Invalid country name", "country": country}
    iso2, iso3 = codes["iso_alpha_2"], codes["iso_alpha_3"]

    # --- Fetch data from all providers ---
    try:
        # Get IMF data (monthly/quarterly when available)
        imf_data_raw = fetch_imf_sdmx_series(iso2)
    except Exception as e:
        print(f"[IMF] Error fetching data for {iso2}: {e}")
        imf_data_raw = {}

    try:
        # Get Eurostat data for EU countries
        eurostat_data = fetch_eurostat_indicators(iso2) if iso2.upper() in EURO_AREA_ISO2 else {}
    except Exception as e:
        print(f"[Eurostat] Error fetching data for {iso2}: {e}")
        eurostat_data = {}

    try:
        # Get World Bank data as fallback
        wb = fetch_worldbank_data(iso2, iso3)
    except Exception as e:
        print(f"[World Bank] Error fetching data for {iso2}: {e}")
        wb = {}

    # --- Merge indicators with priority: IMF > Eurostat (EU only) > World Bank ---
    imf_data = _merge_indicator_data(imf_data_raw, eurostat_data, wb, iso2)

    # --- ECB override (monthly) for EU/EEA/UK policy rates ---
    try:
        if iso2.upper() in EURO_AREA_ISO2:
            imf_data["Interest Rate (Policy)"] = ecb_mro_latest_block()
    except Exception:
        # Quietly keep IMF/WB/empty if ECB fetch fails
        pass

    # --- Debt block (reuses your strict order service) ---
    debt_bundle = compute_debt_payload(country)

    gov_debt_latest = {
        "value": None, "date": None, "source": None,
        "government_type": None, "currency": None, "currency_code": None,
    }
    nom_gdp_latest = {
        "value": None, "date": None, "source": None,
        "currency": None, "currency_code": None,
    }
    debt_pct_latest = {"value": None, "date": None, "source": None, "government_type": None}

    if isinstance(debt_bundle, dict):
        gd = debt_bundle.get("government_debt")
        if isinstance(gd, dict):
            for k in list(gov_debt_latest.keys()):
                if k in gd and gd[k] is not None:
                    gov_debt_latest[k] = gd[k]

        ng = debt_bundle.get("nominal_gdp")
        if isinstance(ng, dict):
            for k in list(nom_gdp_latest.keys()):
                if k in ng and ng[k] is not None:
                    nom_gdp_latest[k] = ng[k]

        dp = debt_bundle.get("debt_to_gdp")
        if isinstance(dp, dict):
            for k in list(debt_pct_latest.keys()):
                if k in dp and dp[k] is not None:
                    debt_pct_latest[k] = dp[k]

    ratio_series = {}
    if isinstance(debt_bundle, dict):
        ratio_series = debt_bundle.get("debt_to_gdp_series") or {}

    return {
        "country": country,
        "iso_codes": codes,
        "imf_data": imf_data,
        "government_debt": {"latest": gov_debt_latest, "series": {}},  # series optional for now
        "nominal_gdp":     {"latest": nom_gdp_latest, "series": {}},   # series optional for now
        "debt_to_gdp":     {"latest": debt_pct_latest, "series": ratio_series},
        "additional_indicators": {},
    }