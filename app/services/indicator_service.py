# app/services/indicator_service.py
from __future__ import annotations

from typing import Dict, Any, Optional, Tuple

# --- Utilities ---------------------------------------------------------------

def _latest_kv(d: Optional[Dict[str, float]]) -> Tuple[Optional[str], Optional[float]]:
    if not isinstance(d, dict) or not d:
        return None, None
    try:
        k = max(d.keys())
        return k, d.get(k)
    except Exception:
        return None, None

def _blank_latest(value_key: str = "value", period_key: str = "date", source: Optional[str] = None) -> Dict[str, Any]:
    return {value_key: None, period_key: None, "source": source}

def _blank_block() -> Dict[str, Any]:
    return {"latest": _blank_latest(), "series": {}}

# --- Country codes -----------------------------------------------------------

def resolve_country_codes(name: str) -> Optional[Dict[str, str]]:
    try:
        from app.utils.country_codes import resolve_country_codes as _rc
        return _rc(name)
    except Exception:
        return None

# --- Optional: debt payload --------------------------------------------------

def compute_debt_payload(country: str) -> Dict[str, Any]:
    """
    Try to load a richer debt payload if available; otherwise return blanks.
    Expected (best case) keys:
      - government_debt: {"latest":{...}, "series":{...}}
      - nominal_gdp:     {"latest":{...}, "series":{...}}
      - debt_to_gdp:     {"latest":{...}, "series":{...}}
      - debt_to_gdp_series: {...}  (optional convenience)
    """
    try:
        from app.services.debt_service import compute_debt_payload as _cdp  # type: ignore
        out = _cdp(country) or {}
        # Normalize shapes to avoid KeyErrors
        if "government_debt" not in out:
            out["government_debt"] = _blank_block()
        if "nominal_gdp" not in out:
            out["nominal_gdp"] = _blank_block()
        if "debt_to_gdp" not in out:
            out["debt_to_gdp"] = _blank_block()
        out.setdefault("debt_to_gdp_series", out.get("debt_to_gdp", {}).get("series") or {})
        return out
    except Exception:
        return {
            "government_debt": _blank_block(),
            "nominal_gdp": _blank_block(),
            "debt_to_gdp": _blank_block(),
            "debt_to_gdp_series": {},
        }

# --- Providers (import defensively) -----------------------------------------

def _load_providers():
    p: Dict[str, Any] = {}
    # IMF monthly
    try:
        from app.providers.imf_provider import (
            imf_cpi_yoy_monthly,
            imf_unemployment_rate_monthly,
            imf_fx_usd_monthly,
            imf_reserves_usd_monthly,
            imf_policy_rate_monthly,
            imf_gdp_growth_quarterly,
        )
        p.update(locals())
    except Exception:
        pass

    # Eurostat monthly (EU overrides for CPI/unemployment if present)
    try:
        from app.providers.eurostat_provider import (
            eurostat_hicp_yoy_monthly,
            eurostat_unemployment_rate_monthly,
        )
        p.update(locals())
    except Exception:
        pass

    # ECB policy (for euro area/GB logic is handled by caller if needed)
    try:
        from app.providers.ecb_provider import ecb_policy_rate_for_country
        p.update(locals())
    except Exception:
        pass

    # World Bank annual fallbacks
    try:
        from app.providers.wb_provider import (
            wb_current_account_balance_pct_gdp_annual,
            wb_government_effectiveness_annual,
        )
        p.update(locals())
    except Exception:
        pass

    return p

# --- Indicator assembly ------------------------------------------------------

_EU_UK_ISO2 = {
    "AT","BE","BG","HR","CY","CZ","DE","DK","EE","ES","FI","FR","GR","EL","HU","IE",
    "IT","LT","LU","LV","MT","NL","PL","PT","RO","SE","SI","SK","IS","NO","LI","GB"
}

def _indicators_for(country: str, iso2: str, iso3: str) -> Dict[str, Any]:
    """
    Assemble the indicator block with monthly-first preference.
    Keys we return (match your UI): cpi_yoy, unemployment_rate, fx_rate_usd, reserves_usd,
    policy_rate, gdp_growth, current_account_balance_pct_gdp, government_effectiveness.
    Each sub-block: {"latest_value":..., "latest_period":..., "source": "...", "series": {}}
    """
    P = _load_providers()
    out: Dict[str, Any] = {}

    # CPI YoY (IMF monthly; Eurostat override for EU/EEA/UK if available)
    latest_period, latest_value, src = None, None, "N/A"
    ser = {}
    if "imf_cpi_yoy_monthly" in P:
        ser = (P["imf_cpi_yoy_monthly"](iso2) or {})
        lp, lv = _latest_kv(ser)
        latest_period, latest_value, src = lp, lv, "IMF"
    if iso2 in _EU_UK_ISO2 and "eurostat_hicp_yoy_monthly" in P:
        eu = P["eurostat_hicp_yoy_monthly"](iso2) or {}
        lp, lv = _latest_kv(eu)
        # prefer the fresher of IMF vs Eurostat
        if lp and (not latest_period or lp > latest_period):
            latest_period, latest_value, src = lp, lv, "Eurostat"
            ser = eu
    out["cpi_yoy"] = {
        "latest_value": latest_value,
        "latest_period": latest_period,
        "source": src,
        "series": {},
    }

    # Unemployment rate (IMF monthly; Eurostat override for EU/EEA/UK)
    ser = {}
    latest_period, latest_value, src = None, None, "N/A"
    if "imf_unemployment_rate_monthly" in P:
        ser = P["imf_unemployment_rate_monthly"](iso2) or {}
        lp, lv = _latest_kv(ser)
        latest_period, latest_value, src = lp, lv, "IMF"
    if iso2 in _EU_UK_ISO2 and "eurostat_unemployment_rate_monthly" in P:
        eu = P["eurostat_unemployment_rate_monthly"](iso2) or {}
        lp, lv = _latest_kv(eu)
        if lp and (not latest_period or lp > latest_period):
            latest_period, latest_value, src = lp, lv, "Eurostat"
            ser = eu
    out["unemployment_rate"] = {
        "latest_value": latest_value,
        "latest_period": latest_period,
        "source": src,
        "series": {},
    }

    # FX rate to USD (IMF monthly)
    ser = {}
    latest_period, latest_value, src = None, None, "N/A"
    if "imf_fx_usd_monthly" in P:
        ser = P["imf_fx_usd_monthly"](iso2) or {}
        lp, lv = _latest_kv(ser)
        latest_period, latest_value, src = lp, lv, "IMF"
    out["fx_rate_usd"] = {
        "latest_value": latest_value,
        "latest_period": latest_period,
        "source": src,
        "series": {},
    }

    # Reserves USD (IMF monthly)
    ser = {}
    latest_period, latest_value, src = None, None, "N/A"
    if "imf_reserves_usd_monthly" in P:
        ser = P["imf_reserves_usd_monthly"](iso2) or {}
        lp, lv = _latest_kv(ser)
        latest_period, latest_value, src = lp, lv, "IMF"
    out["reserves_usd"] = {
        "latest_value": latest_value,
        "latest_period": latest_period,
        "source": src,
        "series": {},
    }

    # Policy rate (ECB override for EU area if available; else IMF monthly)
    ser = {}
    latest_period, latest_value, src = None, None, "N/A"
    used = False
    if iso2 in _EU_UK_ISO2 and "ecb_policy_rate_for_country" in P:
        ecb = P["ecb_policy_rate_for_country"](iso2) or {}
        lp, lv = _latest_kv(ecb)
        if lp:
            latest_period, latest_value, src = lp, lv, "ECB"
            used = True
    if (not used) and "imf_policy_rate_monthly" in P:
        ser = P["imf_policy_rate_monthly"](iso2) or {}
        lp, lv = _latest_kv(ser)
        latest_period, latest_value, src = lp, lv, "IMF"
    out["policy_rate"] = {
        "latest_value": latest_value,
        "latest_period": latest_period,
        "source": src,
        "series": {},
    }

    # GDP growth (IMF quarterly preferred)
    ser = {}
    latest_period, latest_value, src = None, None, "N/A"
    if "imf_gdp_growth_quarterly" in P:
        ser = P["imf_gdp_growth_quarterly"](iso2) or {}
        lp, lv = _latest_kv(ser)
        latest_period, latest_value, src = lp, lv, "IMF"
    out["gdp_growth"] = {
        "latest_value": latest_value,
        "latest_period": latest_period,
        "source": src,
        "series": {},
    }

    # Current account balance % GDP (WB annual)
    ser = {}
    latest_period, latest_value, src = None, None, "N/A"
    if "wb_current_account_balance_pct_gdp_annual" in P:
        ser = P["wb_current_account_balance_pct_gdp_annual"](iso3) or {}
        lp, lv = _latest_kv(ser)
        latest_period, latest_value, src = lp, lv, "WorldBank"
    out["current_account_balance_pct_gdp"] = {
        "latest_value": latest_value,
        "latest_period": latest_period,
        "source": src,
        "series": {},
    }

    # Government effectiveness (WB annual)
    ser = {}
    latest_period, latest_value, src = None, None, "N/A"
    if "wb_government_effectiveness_annual" in P:
        ser = P["wb_government_effectiveness_annual"](iso3) or {}
        lp, lv = _latest_kv(ser)
        latest_period, latest_value, src = lp, lv, "WorldBank"
    out["government_effectiveness"] = {
        "latest_value": latest_value,
        "latest_period": latest_period,
        "source": src,
        "series": {},
    }

    return out

# --- Public entrypoint (keeps same signature your route expects) --------------

def build_country_payload(country: str) -> Dict[str, Any]:
    """
    The only function your /country-data route calls right now.
    It resolves codes, builds indicators (monthly-first), and merges the debt block
    (if available). It never throws: on any failure it returns a safe skeleton.
    """
    codes = resolve_country_codes(country)
    if not codes:
        return {"error": "Invalid country name", "country": country}

    iso2, iso3 = codes["iso_alpha_2"], codes["iso_alpha_3"]

    # Debt block (won’t crash if service missing)
    debt_bundle = compute_debt_payload(country)

    # Indicators
    indicators = _indicators_for(country, iso2, iso3)

    return {
        "country": country,
        "iso_codes": {
            "name": country,
            "iso_alpha_2": iso2,
            "iso_alpha_3": iso3,
            "iso_numeric": codes.get("iso_numeric"),
        },
        # leave this for compatibility
        "imf_data": {},
        # debt bundle (as produced by debt_service or blanks)
        **debt_bundle,
        # indicators for the UI
        "additional_indicators": indicators,
    }
