# app/services/debt_service.py
from __future__ import annotations

from typing import Dict, Any, Optional, Tuple
import math

from app.utils.country_codes import resolve_country_codes

# Defensive provider imports ----------------------------------------------------
# Eurostat (annual debt/GDP for EU/EEA/UK)
try:
    from app.providers import eurostat_provider as euro
except Exception:
    euro = None  # type: ignore

# IMF (WEO annual debt/GDP ratio)
try:
    from app.providers import imf_provider as imf
except Exception:
    imf = None  # type: ignore

# World Bank raw helpers
try:
    from app.providers.wb_provider import fetch_wb_indicator_raw, wb_year_dict_from_raw
except Exception:
    fetch_wb_indicator_raw = None  # type: ignore
    wb_year_dict_from_raw = None   # type: ignore


# ------------------------ helpers ------------------------

def _latest(dict_yyyy_val: Dict[str, float]) -> Optional[Tuple[str, float]]:
    if not dict_yyyy_val:
        return None
    try:
        year = max(dict_yyyy_val.keys())
        return year, float(dict_yyyy_val[year])
    except Exception:
        return None


def _clean_series(data: Optional[Dict[str, float]]) -> Dict[str, float]:
    if not data:
        return {}
    out: Dict[str, float] = {}
    for k, v in data.items():
        try:
            out[str(k)] = float(v)
        except Exception:
            continue
    # chronological
    return dict(sorted(out.items()))


# ----------------- provider-specific fetchers -----------------

def _eurostat_ratio_annual(iso2: str) -> Dict[str, float]:
    """Try Eurostat general government gross debt as % of GDP (annual)."""
    if euro is None:
        return {}
    fn_names = [
        "eurostat_debt_to_gdp_ratio_annual",
        "eurostat_gov_debt_gdp_ratio_annual",
        "eurostat_gg_debt_gdp_ratio_annual",
    ]
    for fn in fn_names:
        if hasattr(euro, fn):
            try:
                return _clean_series(getattr(euro, fn)(iso2))
            except Exception:
                continue
    return {}


def _imf_weo_ratio_annual(iso3: str) -> Dict[str, float]:
    """Try IMF WEO general government gross debt (% of GDP), annual."""
    if imf is None:
        return {}
    fn_names = [
        "imf_weo_debt_to_gdp_ratio_annual",
        "imf_weo_gg_debt_gdp_ratio_annual",
        "imf_weo_debt_gdp_ratio_annual",
    ]
    for fn in fn_names:
        if hasattr(imf, fn):
            try:
                return _clean_series(getattr(imf, fn)(iso3))
            except Exception:
                continue
    return {}


def _wb_ratio_direct_annual(iso3: str) -> Dict[str, float]:
    """World Bank direct ratio (% of GDP): GC.DOD.TOTL.GD.ZS"""
    if fetch_wb_indicator_raw is None or wb_year_dict_from_raw is None:
        return {}
    try:
        raw = fetch_wb_indicator_raw(iso3, "GC.DOD.TOTL.GD.ZS")
        return _clean_series(wb_year_dict_from_raw(raw))
    except Exception:
        return {}


def _wb_series(iso3: str, code: str) -> Dict[str, float]:
    if fetch_wb_indicator_raw is None or wb_year_dict_from_raw is None:
        return {}
    try:
        raw = fetch_wb_indicator_raw(iso3, code)
        return _clean_series(wb_year_dict_from_raw(raw))
    except Exception:
        return {}


def _wb_computed_ratio_annual(iso3: str) -> Dict[str, float]:
    """Compute ratio from levels on a same-currency basis.
    Prefer USD pair (debt USD / GDP USD), else fall back to LCU pair.
    Multiply by 100 to get % of GDP.
    """
    # Try USD pair first
    debt_usd = _wb_series(iso3, "GC.DOD.TOTL.CD")
    gdp_usd  = _wb_series(iso3, "NY.GDP.MKTP.CD")
    years = set(debt_usd.keys()) & set(gdp_usd.keys())
    if years:
        ratio: Dict[str, float] = {}
        for y in years:
            try:
                if gdp_usd[y] != 0:
                    ratio[y] = (debt_usd[y] / gdp_usd[y]) * 100.0
            except Exception:
                continue
        return dict(sorted(ratio.items()))

    # Fall back to LCU pair
    debt_lcu = _wb_series(iso3, "GC.DOD.TOTL.CN")
    gdp_lcu  = _wb_series(iso3, "NY.GDP.MKTP.CN")
    years = set(debt_lcu.keys()) & set(gdp_lcu.keys())
    if not years:
        return {}
    ratio2: Dict[str, float] = {}
    for y in years:
        try:
            if gdp_lcu[y] != 0:
                ratio2[y] = (debt_lcu[y] / gdp_lcu[y]) * 100.0
        except Exception:
            continue
    return dict(sorted(ratio2.items()))


# ------------------------ main ------------------------

def get_debt_to_gdp(country: str) -> Dict[str, Any]:
    """Tiered selection:
    Eurostat -> IMF WEO -> World Bank direct ratio -> Computed (WB levels).
    Returns a consistent object shape:
    {
      "latest": {"year": "2023", "value": 61.2, "source": "Eurostat"} | None,
      "series": {"2019": 59.4, "2020": 73.1, ...},
      "source": "Eurostat" | "IMF WEO" | "World Bank (ratio)" | "Computed (WB levels)" | "unavailable"
    }
    """
    codes = resolve_country_codes(country)
    if not codes:
        return {"latest": None, "series": {}, "source": "invalid_country"}

    iso2 = codes["iso_alpha_2"]
    iso3 = codes["iso_alpha_3"]

    # 1) Eurostat for EU/EEA/UK
    euro_series = _eurostat_ratio_annual(iso2)
    if euro_series:
        latest = _latest(euro_series)
        return {
            "latest": {"year": latest[0], "value": latest[1], "source": "Eurostat"} if latest else None,
            "series": euro_series,
            "source": "Eurostat",
        }

    # 2) IMF WEO ratio
    weo_series = _imf_weo_ratio_annual(iso3)
    if weo_series:
        latest = _latest(weo_series)
        return {
            "latest": {"year": latest[0], "value": latest[1], "source": "IMF WEO"} if latest else None,
            "series": weo_series,
            "source": "IMF WEO",
        }

    # 3) World Bank direct ratio
    wb_ratio = _wb_ratio_direct_annual(iso3)
    if wb_ratio:
        latest = _latest(wb_ratio)
        return {
            "latest": {"year": latest[0], "value": latest[1], "source": "World Bank (ratio)"} if latest else None,
            "series": wb_ratio,
            "source": "World Bank (ratio)",
        }

    # 4) Computed from WB levels
    wb_comp = _wb_computed_ratio_annual(iso3)
    if wb_comp:
        latest = _latest(wb_comp)
        return {
            "latest": {"year": latest[0], "value": latest[1], "source": "Computed (WB levels)"} if latest else None,
            "series": wb_comp,
            "source": "Computed (WB levels)",
        }

    return {"latest": None, "series": {}, "source": "unavailable"}
