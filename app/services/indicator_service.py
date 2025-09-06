from __future__ import annotations
from typing import Any, Dict

# WB providers/util
from app.providers.wb_provider import fetch_worldbank_data, wb_series, wb_entry
# Debt trio service (preserves your strict order)
from app.services.debt_service import compute_debt_payload
# Country codes
from app.utils.country_codes import resolve_country_codes
# ECB monthly policy-rate override (Euro area style)
from app.providers.ecb_provider import ecb_mro_latest_block

# EU/EEA/UK list for policy-rate override (keep identical to what you used)
EURO_AREA_ISO2 = {
    "AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE","GR","HU","IE","IT","LV","LT","LU","MT","NL","PL","PT","RO",
    "SK","SI","ES","SE",  # EU & SE
    "IS","NO","LI",       # EEA
    "GB"                  # United Kingdom
}

def _empty_latest() -> Dict[str, Any]:
    return {"value": None, "date": None, "source": None}

def _empty_series_block() -> Dict[str, Any]:
    return {"latest": _empty_latest(), "series": {}}

def build_country_payload(country: str) -> Dict[str, Any]:
    """
    Port of your /country-data logic:
      - IMF-first/WB fallback for the Indicators table: for now we keep WB (fast, stable),
        but preserve the same keys your UI expects.
      - ECB MRO monthly override for EU/EEA/UK "Interest Rate (Policy)".
      - Attach Government Debt trio from compute_debt_payload() and keep ratio series.
    """
    # --- Country codes ---
    codes = resolve_country_codes(country)
    if not codes:
        return {"error": "Invalid country name", "country": country}
    iso2, iso3 = codes["iso_alpha_2"], codes["iso_alpha_3"]

    # --- Pull World Bank bundle once ---
    wb = fetch_worldbank_data(iso2, iso3)

    # --- Indicators map (WB for stability; same keys as before) ---
    imf_data: Dict[str, Any] = {}

    # CPI YoY (%)
    imf_data["CPI"] = wb_series(wb.get("FP.CPI.TOTL.ZG")) or _empty_series_block()

    # FX to USD (LCU per USD)
    imf_data["FX Rate"] = wb_series(wb.get("PA.NUS.FCRF")) or _empty_series_block()

    # Policy rate – empty default; overridden by ECB for EU/EEA/UK
    imf_data["Interest Rate (Policy)"] = _empty_series_block()

    # Reserves (USD)
    imf_data["Reserves (USD)"] = wb_series(wb.get("FI.RES.TOTL.CD")) or _empty_series_block()

    # Table-only “latest”
    imf_data["GDP Growth (%)"] = wb_entry(wb.get("NY.GDP.MKTP.KD.ZG")) or _empty_latest()
    imf_data["Unemployment (%)"] = wb_entry(wb.get("SL.UEM.TOTL.ZS")) or _empty_latest()
    imf_data["Current Account Balance (% of GDP)"] = wb_entry(wb.get("BN.CAB.XOKA.GD.ZS")) or _empty_latest()
    imf_data["Government Effectiveness"] = wb_entry(wb.get("GE.EST")) or _empty_latest()

    # --- ECB override (monthly) for EU/EEA/UK ---
    try:
        if iso2 in EURO_AREA_ISO2:
            imf_data["Interest Rate (Policy)"] = ecb_mro_latest_block()
    except Exception:
        # Quietly keep WB/empty if ECB fetch fails
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
