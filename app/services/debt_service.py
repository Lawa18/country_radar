# app/services/debt_service.py
from __future__ import annotations

from typing import Any, Dict, Optional
from datetime import date as _date


def _safe_import(path: str):
    try:
        return __import__(path, fromlist=["*"])
    except Exception:
        return None


def _get_iso3(country: str) -> Optional[str]:
    cc_mod = _safe_import("app.utils.country_codes")
    if not cc_mod or not hasattr(cc_mod, "get_country_codes"):
        return None
    try:
        codes = cc_mod.get_country_codes(country) or {}
    except Exception:
        return None
    return (
        codes.get("iso_alpha_3")
        or codes.get("alpha3")
        or codes.get("iso3")
    )


def _wb_years(iso3: str, code: str) -> Dict[str, float]:
    """Thin wrapper over wb_provider: fetch a WB indicator and return {year: float}."""
    wb_mod = _safe_import("app.providers.wb_provider")
    if not wb_mod:
        return {}
    try:
        raw = wb_mod.fetch_wb_indicator_raw(iso3, code)
        return wb_mod.wb_year_dict_from_raw(raw)
    except Exception:
        return {}


def _is_recent_year(year: Any, *, max_age_years: int = 5, today: Optional[_date] = None) -> bool:
    """Return True if a given year is within max_age_years of today.

    This is a light guardrail so that extremely old ratios (e.g. a single
    observation in 1990) are not surfaced as current Debt-to-GDP values.
    """
    try:
        y = int(str(year))
    except Exception:
        return False
    today = today or _date.today()
    return (today.year - y) <= max_age_years


def compute_debt_payload(country: str) -> Dict[str, Any]:
    """Compute a normalized debt bundle for a country.

    For now we:
      - Use World Bank GC.DOD.TOTL.GD.ZS (central gov debt % of GDP) as
        the primary Debt-to-GDP series.
      - Leave government_debt and nominal_gdp series empty (can be
        filled later with CN/CD indicators if desired).

    Returns a dict that matches what indicator_service.py expects:
      {
        "government_debt": { "latest": {...}, "series": {...} },
        "nominal_gdp":     { "latest": {...}, "series": {...} },
        "debt_to_gdp":     { "latest": {...}, "series": {...} },
        "debt_to_gdp_series": {...}
      }
    """
    iso3 = _get_iso3(country)
    if not iso3:
        return {
            "government_debt": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
            "nominal_gdp":     {"latest": {"value": None, "date": None, "source": None}, "series": {}},
            "debt_to_gdp":     {"latest": {"value": None, "date": None, "source": "computed:NA/no-iso3"}, "series": {}},
            "debt_to_gdp_series": {},
        }

    # World Bank: central gov debt % of GDP
    # https://api.worldbank.org/v2/country/MEX/indicator/GC.DOD.TOTL.GD.ZS
    ratio_series = _wb_years(iso3, "GC.DOD.TOTL.GD.ZS")

    # If the series exists but the latest observation is very old, treat it
    # as effectively unavailable to avoid surfacing 1990-style values.
    if ratio_series:
        years = sorted(ratio_series.keys())
        latest_year = years[-1]
        if not _is_recent_year(latest_year, max_age_years=5):
            ratio_series = {}

    if ratio_series:
        years = sorted(ratio_series.keys())
        latest_year = years[-1]
        latest_val = ratio_series[latest_year]
        debt_to_gdp_block = {
            "latest": {
                "value": latest_val,
                "date": latest_year,
                "source": "World Bank (ratio)",
            },
            "series": ratio_series,
        }
        debt_to_gdp_series = ratio_series
    else:
        debt_to_gdp_block = {
            "latest": {"value": None, "date": None, "source": "World Bank (ratio/empty)"},
            "series": {},
        }
        debt_to_gdp_series = {}

    # For now we leave government_debt and nominal_gdp empty –
    # you can later fill them from:
    #  - GC.DOD.TOTL.CN (central gov debt, LCU)
    #  - GC.DOD.TOTL.CD (central gov debt, USD)
    #  - NY.GDP.MKTP.CN / NY.GDP.MKTP.CD (GDP, LCU/USD)
    gov_debt_block = {
        "latest": {"value": None, "date": None, "source": None},
        "series": {},
    }
    nominal_gdp_block = {
        "latest": {"value": None, "date": None, "source": None},
        "series": {},
    }

    return {
        "government_debt": gov_debt_block,
        "nominal_gdp": nominal_gdp_block,
        "debt_to_gdp": debt_to_gdp_block,
        "debt_to_gdp_series": debt_to_gdp_series,
    }


__all__ = ["compute_debt_payload"]
