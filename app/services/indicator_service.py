from app.providers.ecb_provider import ecb_mro_latest_block
from app.providers.eurostat_provider import EURO_AREA_ISO2

from __future__ import annotations

from typing import Dict, Any

from app.utils.country_codes import resolve_country_codes, resolve_currency_code
from app.providers.wb_provider import fetch_worldbank_data, wb_series, wb_entry
from app.providers.eurostat_provider import eurostat_debt_to_gdp_annual
from app.services.debt_service import compute_debt_payload


def _empty_series_block() -> Dict[str, Any]:
    return {
        "latest": {"value": None, "date": None, "source": None},
        "series": {},
    }


def _latest_or_default(entry: Dict[str, Any] | None) -> Dict[str, Any]:
    if isinstance(entry, dict):
        # Normalize to {"value","date","source"} with None defaults
        out = {"value": None, "date": None, "source": None}
        for k in out.keys():
            if k in entry and entry[k] is not None:
                out[k] = entry[k]
        return out
    return {"value": None, "date": None, "source": None}


def build_country_payload(country: str) -> Dict[str, Any]:
    """
    Build the Country Radar indicator payload.

    Source order by indicator (current stable implementation):
      - CPI YoY, FX rate (LCU per USD), Reserves USD: World Bank series (with latest + history)
      - GDP Growth, Unemployment, Current Account % GDP, Gov Effectiveness: WB latest (single-point)
      - Debt-to-GDP:
          Eurostat (ratio) -> IMF (ratio) -> WB (ratio, freshness guarded in debt service)
          -> compute from WB levels (LCU/USD) as last resort, via /v1/debt service

    Returns:
      {
        "country": ...,
        "iso_codes": {...},
        "imf_data": {
            "CPI": {"latest":{...}, "series":{...}},
            "FX Rate": {"latest":{...}, "series":{...}},
            "Interest Rate (Policy)": {"latest":{...}, "series":{...}},  # placeholder for now
            "Reserves (USD)": {"latest":{...}, "series":{...}},
            "GDP Growth (%)": {"value":..., "date":..., "source":"World Bank WDI"},
            "Unemployment (%)": {...},
            "Current Account Balance (% of GDP)": {...},
            "Government Effectiveness": {...}
        },
        "government_debt": {"latest": {...}, "series": {}},
        "nominal_gdp": {"latest": {...}, "series": {}},
        "debt_to_gdp": {"latest": {...}, "series": {...}},
        "additional_indicators": {}
      }
    """
    codes = resolve_country_codes(country)
    if not codes:
        return {"error": "Invalid country name", "country": country}

    iso2 = codes["iso_alpha_2"]
    iso3 = codes["iso_alpha_3"]

    # --- World Bank fetch (primary stable source for now) ---
    wb = fetch_worldbank_data(iso2, iso3)

    # Helper: safely build a series block from a WB indicator
    def wb_series_block(code: str) -> Dict[str, Any]:
        try:
            block = wb_series(wb.get(code))
            # Expecting shape: {"latest": {...}, "series": {...}}
            if isinstance(block, dict) and "latest" in block and "series" in block:
                # Normalize latest
                block["latest"] = _latest_or_default(block.get("latest"))
                # Ensure series keys are strings
                series = block.get("series") or {}
                block["series"] = {str(k): v for k, v in series.items() if v is not None}
                # Backfill source if missing
                if block["latest"].get("source") is None:
                    block["latest"]["source"] = "World Bank WDI"
                return block
        except Exception:
            pass
        return _empty_series_block()

    # --- Indicators with history blocks (series) ---
    cpi_block = wb_series_block("FP.CPI.TOTL.ZG")        # CPI YoY (%)
    fx_block = wb_series_block("PA.NUS.FCRF")            # LCU per USD (avg)
    reserves_block = wb_series_block("FI.RES.TOTL.CD")   # Reserves (USD)

    # Placeholder for policy rate (we will fill when ECB/Eurostat monthly override is wired)
    policy_rate_block = _empty_series_block()

    # --- Single-point indicators (latest only) ---
    gdp_growth = _latest_or_default(wb_entry(wb.get("NY.GDP.MKTP.KD.ZG")))            # GDP growth (annual %)
    unemployment = _latest_or_default(wb_entry(wb.get("SL.UEM.TOTL.ZS")))             # Unemployment (%)
    cab_gdp = _latest_or_default(wb_entry(wb.get("BN.CAB.XOKA.GD.ZS")))               # Current account % GDP
    gov_effect = _latest_or_default(wb_entry(wb.get("GE.EST")))                       # Government effectiveness

    # Normalize sources for single-point entries
    for entry in (gdp_growth, unemployment, cab_gdp, gov_effect):
        if entry.get("source") is None:
            entry["source"] = "World Bank WDI"

    # --- Debt bundle from service (keeps strict compute order + freshness guard) ---
    debt_bundle = compute_debt_payload(country)

    # Prepare latest "components" we may copy from debt bundle if present
    gov_debt_latest = {
        "value": None, "date": None, "source": None,
        "government_type": None, "currency": None, "currency_code": None,
    }
    nom_gdp_latest = {
        "value": None, "date": None, "source": None,
        "currency": None, "currency_code": None,
    }
    debt_pct_latest = {
        "value": None, "date": None, "source": None,
        "government_type": None
    }

    # Copy fields from /v1/debt result if available
    if isinstance(debt_bundle, dict):
        gd = debt_bundle.get("government_debt")
        if isinstance(gd, dict):
            for k in gov_debt_latest.keys():
                if k in gd and gd[k] is not None:
                    gov_debt_latest[k] = gd[k]
        ng = debt_bundle.get("nominal_gdp")
        if isinstance(ng, dict):
            for k in nom_gdp_latest.keys():
                if k in ng and ng[k] is not None:
                    nom_gdp_latest[k] = ng[k]
        dp = debt_bundle.get("debt_to_gdp")
        if isinstance(dp, dict):
            for k in debt_pct_latest.keys():
                if k in dp and dp[k] is not None:
                    debt_pct_latest[k] = dp[k]

    # Currency code backfill for LCU/USD flags
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

    # --- Debt-to-GDP series selection ---
    ratio_series_from_v1 = {}
    if isinstance(debt_bundle, dict):
        ratio_series_from_v1 = debt_bundle.get("debt_to_gdp_series") or {}

    # Fallback: Eurostat annual ratio, else WB ratio history
    if not ratio_series_from_v1:
        try:
            es_ratio_series = eurostat_debt_to_gdp_annual(iso2) or {}
        except Exception:
            es_ratio_series = {}
        try:
            wb_ratio_hist = wb_series(wb.get("GC.DOD.TOTL.GD.ZS")) or {}
            wb_ratio_series = (wb_ratio_hist.get("series") if isinstance(wb_ratio_hist, dict) else {}) or {}
        except Exception:
            wb_ratio_series = {}
        ratio_series_from_v1 = es_ratio_series or wb_ratio_series or {}

    # --- Assemble the "imf_data" block (naming preserved for UI) ---
    imf_data = {
        "CPI": cpi_block,
        "FX Rate": fx_block,
        "Interest Rate (Policy)": policy_rate_block,  # placeholder; override later with ECB monthly if available
        "Reserves (USD)": reserves_block,
        "GDP Growth (%)": gdp_growth,
        "Unemployment (%)": unemployment,
        "Current Account Balance (% of GDP)": cab_gdp,
        "Government Effectiveness": gov_effect,
    }

    # --- Build final response ---
    response = {
        "country": country,
        "iso_codes": codes,
        "imf_data": imf_data,
        "government_debt": {"latest": gov_debt_latest, "series": {}},
        "nominal_gdp": {"latest": nom_gdp_latest, "series": {}},
        "debt_to_gdp": {
            "latest": debt_pct_latest,                # whatever /v1/debt picked as latest
            "series": ratio_series_from_v1            # Eurostat/IMF/WB history if available
        },
        "additional_indicators": {}
    }
    return response
