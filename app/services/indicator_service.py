from typing import Dict, Any
from app.utils.country_codes import resolve_country_codes, resolve_currency_code
from app.services.debt_service import compute_debt_payload
from app.providers.wb_provider import fetch_worldbank_data, wb_entry, wb_series

def _blank_gov_debt() -> Dict[str, Any]:
    return {
        "value": None, "date": None, "source": None,
        "government_type": None, "currency": None, "currency_code": None,
    }

def _blank_nom_gdp() -> Dict[str, Any]:
    return {"value": None, "date": None, "source": None, "currency": None, "currency_code": None}

def _blank_debt_pct() -> Dict[str, Any]:
    return {"value": None, "date": None, "source": None, "government_type": None}

def build_country_payload(country: str) -> Dict[str, Any]:
    codes = resolve_country_codes(country)
    if not codes:
        return {"error": "Invalid country name", "country": country}
    iso2, iso3 = codes["iso_alpha_2"], codes["iso_alpha_3"]

    # Debt trio (keeps your strict order logic via debt_service)
    debt_bundle = compute_debt_payload(country) or {}

    gov_latest = debt_bundle.get("government_debt") or _blank_gov_debt()
    gdp_latest = debt_bundle.get("nominal_gdp") or _blank_nom_gdp()
    ratio_latest = debt_bundle.get("debt_to_gdp") or _blank_debt_pct()
    ratio_series = debt_bundle.get("debt_to_gdp_series") or {}

    # World Bank indicators for table
    wb = fetch_worldbank_data(iso2, iso3)

    # Series blocks (latest + series)
    cpi_block = wb_series(wb.get("FP.CPI.TOTL.ZG")) or {"latest": {"value": None, "date": None, "source": None}, "series": {}}
    fx_block  = wb_series(wb.get("PA.NUS.FCRF"))    or {"latest": {"value": None, "date": None, "source": None}, "series": {}}
    res_block = wb_series(wb.get("FI.RES.TOTL.CD")) or {"latest": {"value": None, "date": None, "source": None}, "series": {}}

    # Single latest entries
    gdp_growth  = wb_entry(wb.get("NY.GDP.MKTP.KD.ZG")) or {"value": None, "date": None, "source": None}
    unemp       = wb_entry(wb.get("SL.UEM.TOTL.ZS"))    or {"value": None, "date": None, "source": None}
    cab         = wb_entry(wb.get("BN.CAB.XOKA.GD.ZS")) or {"value": None, "date": None, "source": None}
    gov_eff     = wb_entry(wb.get("GE.EST"))            or {"value": None, "date": None, "source": None}

    # Build "imf_data" table keys (WB-backed for now)
    imf_data: Dict[str, Any] = {
        "CPI": cpi_block,            # Inflation Rate (CPI YoY)
        "FX Rate": fx_block,         # Exchange Rate (to USD)
        "Interest Rate (Policy)": {  # placeholder until IMF/ECB are added
            "latest": {"value": None, "date": None, "source": None},
            "series": {}
        },
        "Reserves (USD)": res_block,
        "GDP Growth (%)": gdp_growth,
        "Unemployment (%)": unemp,
        "Current Account Balance (% of GDP)": cab,
        "Government Effectiveness": gov_eff,
    }

    # Ensure currency codes for LCU flags, if present
    try:
        if gov_latest.get("currency") == "LCU" and not gov_latest.get("currency_code"):
            gov_latest["currency_code"] = resolve_currency_code(iso2)
        if gdp_latest.get("currency") == "LCU" and not gdp_latest.get("currency_code"):
            gdp_latest["currency_code"] = resolve_currency_code(iso2)
    except Exception:
        pass

    return {
        "country": country,
        "iso_codes": codes,
        "imf_data": imf_data,
        "government_debt": {"latest": gov_latest, "series": {}},
        "nominal_gdp":     {"latest": gdp_latest, "series": {}},
        "debt_to_gdp":     {"latest": ratio_latest, "series": ratio_series},
        "additional_indicators": {},
    }
