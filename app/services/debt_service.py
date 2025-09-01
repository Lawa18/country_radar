from typing import Dict, Any, Optional

from app.providers.wb_provider import fetch_worldbank_data, wb_year_dict_from_raw
from app.providers.eurostat_provider import eurostat_debt_to_gdp_annual
from app.providers.imf_provider import imf_debt_to_gdp_annual
from app.utils.country_codes import resolve_country_codes, resolve_currency_code


def compute_debt_payload(country: str) -> Dict[str, Any]:
    """
    Strict selection order for Debt-to-GDP:
      1) Eurostat annual ratio (EU/EEA/UK)
      2) IMF WEO annual ratio
      3) World Bank WDI annual ratio
      4) If none: compute from WB annual levels (LCU preferred; USD fallback)
    """
    try:
        codes = resolve_country_codes(country)
        if not codes:
            return {"error": "Invalid country name", "country": country}

        iso2 = codes["iso_alpha_2"]
        iso3 = codes["iso_alpha_3"]
        path_used: Optional[str] = None

        eurostat_series: Dict[str, float] = {}
        best: Optional[Dict[str, Any]] = None

        # 1) Eurostat ratio
        try:
            eurostat_series = eurostat_debt_to_gdp_annual(iso2) or {}
            if eurostat_series:
                y = max(int(k) for k in eurostat_series.keys() if str(k).isdigit())
                best = {
                    "source": "Eurostat (debt-to-GDP ratio)",
                    "period": str(y),
                    "debt_to_gdp": float(eurostat_series[str(y)]),
                    "government_type": "General Government",
                }
                path_used = "EUROSTAT_ANNUAL_RATIO"
        except Exception:
            eurostat_series = {}

        # 2) IMF ratio
        if best is None:
            try:
                imf_series = imf_debt_to_gdp_annual(iso3) or {}
                if imf_series:
                    y = max(int(k) for k in imf_series.keys() if str(k).isdigit())
                    best = {
                        "source": "IMF WEO (ratio)",
                        "period": str(y),
                        "debt_to_gdp": float(imf_series[str(y)]),
                        "government_type": "General Government",
                    }
                    path_used = "IMF_ANNUAL_RATIO"
            except Exception:
                pass

        # 3) WB ratio
        if best is None:
            try:
                wb = fetch_worldbank_data(iso2, iso3)
                ratio_raw = wb.get("GC.DOD.TOTL.GD.ZS")
                ratio_dict = wb_year_dict_from_raw(ratio_raw)
                if ratio_dict:
                    years = sorted([int(y) for y, v in ratio_dict.items() if v is not None])
                    if years:
                        y = years[-1]
                        best = {
                            "source": "World Bank WDI (ratio)",
                            "period": str(y),
                            "debt_to_gdp": round(float(ratio_dict[y]), 2),
                            "government_type": "Central Government",
                        }
                        path_used = "WB_ANNUAL_RATIO"
            except Exception:
                pass

        # 4) Compute from levels (LCU first, USD fallback)
        government_debt = None
        nominal_gdp = None
        if best is None:
            try:
                wb = fetch_worldbank_data(iso2, iso3)
                debt_lcu = wb_year_dict_from_raw(wb.get("GC.DOD.TOTL.CN")) or {}
                gdp_lcu = wb_year_dict_from_raw(wb.get("NY.GDP.MKTP.CN")) or {}
                common_years = sorted(
                    set(int(y) for y in debt_lcu if debt_lcu[y] is not None)
                    & set(int(y) for y in gdp_lcu if gdp_lcu[y] is not None)
                )
                if common_years:
                    y = common_years[-1]
                    d = float(debt_lcu[y]); g = float(gdp_lcu[y])
                    if g != 0:
                        best = {
                            "source": "World Bank WDI (computed)",
                            "period": str(y),
                            "debt_to_gdp": round((d / g) * 100, 2),
                            "government_type": "Central Government",
                        }
                        path_used = "WB_ANNUAL_COMPUTED"
                        government_debt = {
                            "value": d, "date": str(y), "source": "World Bank WDI",
                            "government_type": "Central Government",
                            "currency": "LCU", "currency_code": resolve_currency_code(iso2),
                        }
                        nominal_gdp = {
                            "value": g, "date": str(y), "source": "World Bank WDI",
                            "currency": "LCU", "currency_code": resolve_currency_code(iso2),
                        }
                if best is None:
                    debt_usd = wb_year_dict_from_raw(wb.get("GC.DOD.TOTL.CD")) or {}
                    gdp_usd = wb_year_dict_from_raw(wb.get("NY.GDP.MKTP.CD")) or {}
                    common_years = sorted(
                        set(int(y) for y in debt_usd if debt_usd[y] is not None)
                        & set(int(y) for y in gdp_usd if gdp_usd[y] is not None)
                    )
                    if common_years:
                        y = common_years[-1]
                        d = float(debt_usd[y]); g = float(gdp_usd[y])
                        if g != 0:
                            best = {
                                "source": "World Bank WDI (computed USD)",
                                "period": str(y),
                                "debt_to_gdp": round((d / g) * 100, 2),
                                "government_type": "Central Government",
                            }
                            path_used = "WB_ANNUAL_COMPUTED_USD"
                            government_debt = {
                                "value": d, "date": str(y), "source": "World Bank WDI",
                                "government_type": "Central Government",
                                "currency": "USD", "currency_code": "USD",
                            }
                            nominal_gdp = {
                                "value": g, "date": str(y), "source": "World Bank WDI",
                                "currency": "USD", "currency_code": "USD",
                            }
            except Exception:
                pass

        # If still nothing, return empty but include Eurostat series if any
        if best is None:
            return {
                "country": country,
                "iso_codes": codes,
                "debt_to_gdp": {"value": None, "date": None, "source": None, "government_type": None},
                "debt_to_gdp_series": eurostat_series if isinstance(eurostat_series, dict) else {},
                "path_used": path_used,
            }

        # Historical series aligned to chosen source
        series: Dict[str, float] = {}
        if path_used == "EUROSTAT_ANNUAL_RATIO":
            series = eurostat_series or {}
        elif path_used == "IMF_ANNUAL_RATIO":
            try:
                series = imf_debt_to_gdp_annual(iso3) or {}
            except Exception:
                series = {}
        elif path_used and path_used.startswith("WB_"):
            try:
                wb = fetch_worldbank_data(iso2, iso3)
                ratio_raw = wb.get("GC.DOD.TOTL.GD.ZS")
                series = wb_year_dict_from_raw(ratio_raw) or {}
                series = {str(k): v for k, v in series.items() if v is not None}
            except Exception:
                series = {}

        resp: Dict[str, Any] = {
            "country": country,
            "iso_codes": codes,
            "debt_to_gdp": {
                "value": best["debt_to_gdp"],
                "date": best["period"],
                "source": best["source"],
                "government_type": best["government_type"],
            },
            "debt_to_gdp_series": series,
            "path_used": path_used,
        }
        if government_debt:
            resp["government_debt"] = government_debt
        if nominal_gdp:
            resp["nominal_gdp"] = nominal_gdp

        return resp

    except Exception as e:
        return {"error": str(e), "country": country}
