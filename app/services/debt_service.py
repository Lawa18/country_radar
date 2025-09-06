from __future__ import annotations

from typing import Dict, Any, Optional
from datetime import datetime

from app.providers.wb_provider import fetch_worldbank_data, wb_year_dict_from_raw
from app.providers.eurostat_provider import eurostat_debt_to_gdp_annual
from app.providers.imf_provider import imf_debt_to_gdp_annual
from app.utils.country_codes import resolve_country_codes, resolve_currency_code


def compute_debt_payload(country: str) -> Dict[str, Any]:
    """
    Strict selection order for Debt-to-GDP:
      1) Eurostat annual ratio (EU/EEA/UK)  -> General Government
      2) IMF WEO annual ratio               -> General Government
      3) World Bank WDI annual ratio        -> Central Government (guard for staleness)
      4) If none: compute from WB annual levels (LCU preferred; USD fallback)
    Returns a dict with:
      - country
      - iso_codes
      - debt_to_gdp: { value, date, source, government_type }
      - debt_to_gdp_series: { "YYYY": value } aligned to chosen source when possible
      - path_used
      - optional: government_debt, nominal_gdp (when computed from levels)
    """
    try:
        codes = resolve_country_codes(country)
        if not codes:
            return {
                "error": "Invalid country name",
                "country": country,
            }

        iso2 = codes["iso_alpha_2"]
        iso3 = codes["iso_alpha_3"]
        path_used: Optional[str] = None

        best: Optional[Dict[str, Any]] = None
        government_debt: Optional[Dict[str, Any]] = None
        nominal_gdp: Optional[Dict[str, Any]] = None

        # Keep any series we fetch so we can return aligned history
        eurostat_series: Dict[str, float] = {}
        imf_series: Dict[str, float] = {}

        # ------------------------------------------------------------------------------
        # 1) Eurostat annual ratio (EU/EEA/UK) - ratio first
        # ------------------------------------------------------------------------------
        try:
            eurostat_series = eurostat_debt_to_gdp_annual(iso2) or {}
            if eurostat_series:
                latest_years = [int(k) for k in eurostat_series.keys() if str(k).isdigit()]
                if latest_years:
                    y = max(latest_years)
                    val = eurostat_series.get(str(y))
                    if val is not None:
                        best = {
                            "source": "Eurostat (debt-to-GDP ratio)",
                            "period": str(y),
                            "debt_to_gdp": float(val),
                            "government_type": "General Government",
                        }
                        path_used = "EUROSTAT_ANNUAL_RATIO"
        except Exception:
            eurostat_series = {}

        # ------------------------------------------------------------------------------
        # 2) IMF WEO annual ratio
        # ------------------------------------------------------------------------------
        if best is None:
            try:
                imf_series = imf_debt_to_gdp_annual(iso3) or {}
                if imf_series:
                    latest_years = [int(k) for k in imf_series.keys() if str(k).isdigit()]
                    if latest_years:
                        y = max(latest_years)
                        val = imf_series.get(str(y))
                        if val is not None:
                            best = {
                                "source": "IMF WEO (ratio)",
                                "period": str(y),
                                "debt_to_gdp": float(val),
                                "government_type": "General Government",
                            }
                            path_used = "IMF_ANNUAL_RATIO"
            except Exception:
                pass

        # ------------------------------------------------------------------------------
        # 3) World Bank WDI annual ratio (only if reasonably fresh)
        #     - WB % ratio is CENTRAL government; can be very stale for AEs.
        #     - We accept only if within the last N years; else fall through to compute.
        # ------------------------------------------------------------------------------
        if best is None:
            try:
                wb = fetch_worldbank_data(iso2, iso3)
                ratio_raw = wb.get("GC.DOD.TOTL.GD.ZS")
                ratio_dict = wb_year_dict_from_raw(ratio_raw)
                if ratio_dict:
                    years = sorted([int(y) for y, v in ratio_dict.items() if v is not None])
                    if years:
                        y = years[-1]
                        CURRENT_YEAR = datetime.utcnow().year
                        FRESH_YEARS = 10  # accept ratios within the last 10 years
                        if y >= (CURRENT_YEAR - FRESH_YEARS):
                            best = {
                                "source": "World Bank WDI (ratio)",
                                "period": str(y),
                                "debt_to_gdp": round(float(ratio_dict[y]), 2),
                                "government_type": "Central Government",
                            }
                            path_used = "WB_ANNUAL_RATIO"
            except Exception:
                pass

        # ------------------------------------------------------------------------------
        # 4) Compute from annual levels only if no ratio found
        # ------------------------------------------------------------------------------
        if best is None:
            try:
                wb = fetch_worldbank_data(iso2, iso3)

                # Prefer LCU compute first
                debt_lcu = wb_year_dict_from_raw(wb.get("GC.DOD.TOTL.CN")) or {}
                gdp_lcu = wb_year_dict_from_raw(wb.get("NY.GDP.MKTP.CN")) or {}
                common_years_lcu = sorted(
                    set(int(y) for y in debt_lcu if debt_lcu[y] is not None)
                    & set(int(y) for y in gdp_lcu if gdp_lcu[y] is not None)
                )
                if common_years_lcu:
                    y = common_years_lcu[-1]
                    d = float(debt_lcu[y]); g = float(gdp_lcu[y])
                    if g != 0:
                        best = {
                            "source": "World Bank WDI (computed)",
                            "period": str(y),
                            "debt_to_gdp": round((d / g) * 100, 2),
                            "government_type": "Central Government",
                        }
                        path_used = "WB_ANNUAL_COMPUTED"
                        # attach components (LCU)
                        ccode = resolve_currency_code(iso2)
                        government_debt = {
                            "value": d,
                            "date": str(y),
                            "source": "World Bank WDI",
                            "government_type": "Central Government",
                            "currency": "LCU",
                            "currency_code": ccode,
                        }
                        nominal_gdp = {
                            "value": g,
                            "date": str(y),
                            "source": "World Bank WDI",
                            "currency": "LCU",
                            "currency_code": ccode,
                        }

                # If LCU not possible, try USD compute
                if best is None:
                    debt_usd = wb_year_dict_from_raw(wb.get("GC.DOD.TOTL.CD")) or {}
                    gdp_usd = wb_year_dict_from_raw(wb.get("NY.GDP.MKTP.CD")) or {}
                    common_years_usd = sorted(
                        set(int(y) for y in debt_usd if debt_usd[y] is not None)
                        & set(int(y) for y in gdp_usd if gdp_usd[y] is not None)
                    )
                    if common_years_usd:
                        y = common_years_usd[-1]
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
                                "value": d,
                                "date": str(y),
                                "source": "World Bank WDI",
                                "government_type": "Central Government",
                                "currency": "USD",
                                "currency_code": "USD",
                            }
                            nominal_gdp = {
                                "value": g,
                                "date": str(y),
                                "source": "World Bank WDI",
                                "currency": "USD",
                                "currency_code": "USD",
                            }
            except Exception:
                pass

        # ------------------------------------------------------------------------------
        # If still nothing, return empty ratio but keep any Eurostat series we found
        # ------------------------------------------------------------------------------
        if best is None:
            return {
                "country": country,
                "iso_codes": codes,
                "debt_to_gdp": {
                    "value": None, "date": None, "source": None, "government_type": None
                },
                "debt_to_gdp_series": eurostat_series if isinstance(eurostat_series, dict) else {},
                "path_used": path_used,
            }

        # ------------------------------------------------------------------------------
        # Choose historical series aligned to chosen source
        # ------------------------------------------------------------------------------
        series: Dict[str, float] = {}
        if path_used == "EUROSTAT_ANNUAL_RATIO":
            series = eurostat_series or {}
        elif path_used == "IMF_ANNUAL_RATIO":
            series = imf_series or {}
        elif path_used and path_used.startswith("WB_"):
            try:
                wb = wb if 'wb' in locals() else fetch_worldbank_data(iso2, iso3)
                ratio_raw = wb.get("GC.DOD.TOTL.GD.ZS")
                wb_series = wb_year_dict_from_raw(ratio_raw) or {}
                # keep only years with numeric values, stringify keys
                series = {str(k): v for k, v in wb_series.items() if v is not None}
            except Exception:
                series = {}

        # ------------------------------------------------------------------------------
        # Build response
        # ------------------------------------------------------------------------------
        resp: Dict[str, Any] = {
            "country": country,
            "iso_codes": codes,
            "debt_to_gdp": {
                "value": float(best["debt_to_gdp"]) if best["debt_to_gdp"] is not None else None,
                "date": best["period"],
                "source": best["source"],
                "government_type": best.get("government_type"),
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
