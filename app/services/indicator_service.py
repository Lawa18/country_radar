# app/services/indicator_service.py
from __future__ import annotations
from typing import Dict, Any, Tuple, Optional
from fastapi.responses import JSONResponse

from app.providers.imf_provider import fetch_imf_sdmx_series, imf_debt_to_gdp_annual
from app.providers.wb_provider import fetch_worldbank_data, wb_series, wb_entry
from app.providers.eurostat_provider import eurostat_debt_to_gdp_annual
from app.services.debt_service import compute_debt_payload
from app.utils.country_codes import resolve_country_codes, resolve_currency_code

def _is_num(x) -> bool:
    try:
        float(x)
        return True
    except Exception:
        return False

def _imf_block(imf: Dict[str, Dict[str, Any]], label: str, wb_raw: Any):
    imf_vals = imf.get(label, {})
    if isinstance(imf_vals, dict) and imf_vals:
        pairs = [(int(y), float(v)) for y, v in imf_vals.items()
                 if str(y).isdigit() and _is_num(v)]
        if pairs:
            y, v = max(pairs, key=lambda x: x[0])
            return {
                "latest": {"value": v, "date": str(y), "source": "IMF"},
                "series": {str(yy): vv for yy, vv in sorted(pairs)}
            }
    # WB fallback
    s = wb_series(wb_raw)
    return s or {"latest": {"value": None, "date": None, "source": None}, "series": {}}

def build_country_payload(country: str) -> Dict[str, Any]:
    codes = resolve_country_codes(country)
    if not codes:
        return {"error": "Invalid country name", "country": country}
    iso2, iso3 = codes["iso_alpha_2"], codes["iso_alpha_3"]

    # Providers
    imf = {}
    try:
        imf = fetch_imf_sdmx_series(iso2) or {}
    except Exception:
        imf = {}
    wb = fetch_worldbank_data(iso2, iso3) or {}

    # IMF-first blocks (fallback to WB codes in comments)
    imf_data: Dict[str, Any] = {
        "CPI": _imf_block(imf, "CPI", wb.get("FP.CPI.TOTL.ZG")),
        "FX Rate": _imf_block(imf, "FX Rate", wb.get("PA.NUS.FCRF")),
        "Interest Rate (Policy)": _imf_block(imf, "Interest Rate", wb.get("FR.INR.RINR")),  # may be None; Euro area override later
        "Reserves (USD)": _imf_block(imf, "Reserves (USD)", wb.get("FI.RES.TOTL.CD")),
    }

    # Single-point indicators (IMF optional; WB fallback straight to latest)
    gdp_growth = wb_entry(wb.get("NY.GDP.MKTP.KD.ZG")) or {"value": None, "date": None, "source": None}
    imf_data["GDP Growth (%)"] = gdp_growth
    imf_data["Unemployment (%)"] = wb_entry(wb.get("SL.UEM.TOTL.ZS")) or {"value": None, "date": None, "source": None}
    imf_data["Current Account Balance (% of GDP)"] = wb_entry(wb.get("BN.CAB.XOKA.GD.ZS")) or {"value": None, "date": None, "source": None}
    imf_data["Government Effectiveness"] = wb_entry(wb.get("GE.EST")) or {"value": None, "date": None, "source": None}

    # Debt block from the dedicated service (already handles Eurostat→IMF→WB order)
    debt_bundle = compute_debt_payload(country) or {}

    # Shape government debt / nominal gdp outputs (latest + empty series for now)
    gov_debt_latest = {
        "value": None, "date": None, "source": None,
        "government_type": None, "currency": None, "currency_code": None,
    }
    nom_gdp_latest = {
        "value": None, "date": None, "source": None,
        "currency": None, "currency_code": None,
    }
    debt_pct_latest = {
        "value": None, "date": None, "source": None, "government_type": None
    }

    try:
        if isinstance(debt_bundle, dict):
            for key in gov_debt_latest.keys():
                if isinstance(debt_bundle.get("government_debt"), dict) and key in debt_bundle["government_debt"]:
                    val = debt_bundle["government_debt"][key]
                    if val is not None:
                        gov_debt_latest[key] = val
            for key in nom_gdp_latest.keys():
                if isinstance(debt_bundle.get("nominal_gdp"), dict) and key in debt_bundle["nominal_gdp"]:
                    val = debt_bundle["nominal_gdp"][key]
                    if val is not None:
                        nom_gdp_latest[key] = val
            for key in debt_pct_latest.keys():
                if isinstance(debt_bundle.get("debt_to_gdp"), dict) and key in debt_bundle["debt_to_gdp"]:
                    val = debt_bundle["debt_to_gdp"][key]
                    if val is not None:
                        debt_pct_latest[key] = val
    except Exception:
        pass

    ratio_series = {}
    try:
        ratio_series = debt_bundle.get("debt_to_gdp_series") or {}
    except Exception:
        ratio_series = {}

    # Ensure currency codes when currency is LCU/USD
    try:
        if gov_debt_latest.get("currency") == "LCU" and not gov_debt_latest.get("currency_code"):
            gov_debt_latest["currency_code"] = resolve_currency_code(iso2)
        if nom_gdp_latest.get("currency") == "LCU" and not nom_gdp_latest.get("currency_code"):
            nom_gdp_latest["currency_code"] = resolve_currency_code(iso2)
        if gov_debt_latest.get("currency") == "USD" and not gov_debt_latest.get("currency_code"):
            gov_debt_latest["currency_code"] = "USD"
        if nom_gdp_latest.get("currency") == "USD" and not nom_gdp_latest.get("currency_code"):
            nom_gdp_latest["currency_code"] = "USD"
    except Exception:
        pass

    return {
        "country": country,
        "iso_codes": codes,
        "imf_data": imf_data,
        "government_debt": {"latest": gov_debt_latest, "series": {}},
        "nominal_gdp": {"latest": nom_gdp_latest, "series": {}},
        "debt_to_gdp": {"latest": debt_pct_latest, "series": ratio_series},
        "additional_indicators": {}
    }
