from fastapi.responses import JSONResponse

from app.providers.imf_provider import fetch_imf_sdmx_series, imf_debt_to_gdp_annual
from app.providers.wb_provider import fetch_worldbank_data, wb_series, wb_entry
from app.providers.eurostat_provider import eurostat_debt_to_gdp_annual
from app.services.debt_service import compute_debt_payload
from app.utils.country_codes import resolve_country_codes, resolve_currency_code

EURO_AREA_ISO2 = {
    "AT","BE","CY","DE","EE","ES","FI","FR","GR","HR",
    "IE","IT","LT","LU","LV","MT","NL","PT","SI","SK",
}

def _blank_gov_debt() -> Dict[str, Any]:
    return {
        "value": None, "date": None, "source": None,
        "government_type": None, "currency": None, "currency_code": None,
    }

def _blank_nom_gdp() -> Dict[str, Any]:
    return {"value": None, "date": None, "source": None, "currency": None, "currency_code": None}

def _blank_debt_pct() -> Dict[str, Any]:
    return {"value": None, "date": None, "source": None, "government_type": None}

def _latest_from_year_series(series: Dict[str, float]) -> Dict[str, Any]:
    if not series:
        return {"value": None, "date": None, "source": None}
    try:
        y = max(int(k) for k in series.keys() if str(k).isdigit())
        return {"value": float(series[str(y)]), "date": str(y), "source": "IMF WEO"}
    except Exception:
        return {"value": None, "date": None, "source": None}

def _latest_from_month_series(series: Dict[str, float]) -> Dict[str, Any]:
    # keys like 'YYYY-MM'
    if not series:
        return {"value": None, "date": None, "source": None}
    try:
        def ym_key(s: str) -> Tuple[int, int]:
            parts = s.split("-")
            return (int(parts[0]), int(parts[1]) if len(parts) > 1 else 1)
        latest_key = max(series.keys(), key=lambda k: ym_key(k))
        return {"value": float(series[latest_key]), "date": latest_key, "source": "Eurostat (ECB MRO)"}
    except Exception:
        return {"value": None, "date": None, "source": None}

def build_country_payload(country: str) -> Dict[str, Any]:
    codes = resolve_country_codes(country)
    if not codes:
        return {"error": "Invalid country name", "country": country}
    iso2, iso3 = codes["iso_alpha_2"], codes["iso_alpha_3"]

    # Debt trio (strict order via debt_service)
    debt_bundle = compute_debt_payload(country) or {}

    gov_latest = debt_bundle.get("government_debt") or _blank_gov_debt()
    gdp_latest = debt_bundle.get("nominal_gdp") or _blank_nom_gdp()
    ratio_latest = debt_bundle.get("debt_to_gdp") or _blank_debt_pct()
    ratio_series = debt_bundle.get("debt_to_gdp_series") or {}

    # WB basket for fallbacks/table
    wb = fetch_worldbank_data(iso2, iso3)

    # IMF WEO basket (annual) for now

    # CPI (YoY %): IMF-first (annual), WB fallback (annual)
    cpi_imf = weo.get("CPI") or {}
    cpi_block = {"latest": _latest_from_year_series(cpi_imf), "series": cpi_imf} if cpi_imf else (
        wb_series(wb.get("FP.CPI.TOTL.ZG")) or {"latest": {"value": None, "date": None, "source": None}, "series": {}}
    )

    # FX Rate (WB annual)
    fx_block  = wb_series(wb.get("PA.NUS.FCRF")) or {"latest": {"value": None, "date": None, "source": None}, "series": {}}

    # Reserves (USD) (WB annual)
    res_block = wb_series(wb.get("FI.RES.TOTL.CD")) or {"latest": {"value": None, "date": None, "source": None}, "series": {}}

    # GDP Growth (%): IMF-first (annual), WB fallback (annual)
    gdp_growth_imf_series = weo.get("GDP Growth (%)") or {}
    gdp_growth = _latest_from_year_series(gdp_growth_imf_series) if gdp_growth_imf_series else (
        wb_entry(wb.get("NY.GDP.MKTP.KD.ZG")) or {"value": None, "date": None, "source": None}
    )

    # Unemployment (%): IMF-first (annual), WB fallback (annual)
    unemp_imf_series = weo.get("Unemployment (%)") or {}
    unemp = _latest_from_year_series(unemp_imf_series) if unemp_imf_series else (
        wb_entry(wb.get("SL.UEM.TOTL.ZS")) or {"value": None, "date": None, "source": None}
    )

    # Current Account Balance (% of GDP): IMF-first (annual), WB fallback (annual)
    cab_imf_series = weo.get("Current Account Balance (% of GDP)") or {}
    cab = _latest_from_year_series(cab_imf_series) if cab_imf_series else (
        wb_entry(wb.get("BN.CAB.XOKA.GD.ZS")) or {"value": None, "date": None, "source": None}
    )

    # Government Effectiveness (WB annual)
    gov_eff = wb_entry(wb.get("GE.EST")) or {"value": None, "date": None, "source": None}

    # Interest Rate (Policy): Euro Area → monthly Eurostat MRO (latest month)
    ir_block = {"latest": {"value": None, "date": None, "source": None}, "series": {}}
    try:
        if iso2 in EURO_AREA_ISO2:
            mro_monthly = eurostat_mro_monthly() or {}
            if mro_monthly:
                latest = _latest_from_month_series(mro_monthly)
                series_sorted = {k: mro_monthly[k] for k in sorted(mro_monthly.keys())}
                ir_block = {"latest": latest, "series": series_sorted}
    except Exception:
        pass

    imf_data: Dict[str, Any] = {
        "CPI": cpi_block,
        "FX Rate": fx_block,
        "Interest Rate (Policy)": ir_block,  # monthly for euro area, else empty for now
        "Reserves (USD)": res_block,
        "GDP Growth (%)": gdp_growth,
        "Unemployment (%)": unemp,
        "Current Account Balance (% of GDP)": cab,
        "Government Effectiveness": gov_eff,
    }

    # Ensure LCU codes if needed
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
