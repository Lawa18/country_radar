# app/providers/gmd_provider.py
from __future__ import annotations

from typing import Dict, Any, Optional


"""
Thin wrapper for the Global Macro Database (GMD).

Phase 1: this is a stub that returns {}, so the rest of the system
does not crash while you wire in a real GMD backend.

Phase 2 (later): load GMD from a local CSV/Parquet, or an external
API, and implement the lookup functions below.
"""


def _not_implemented(*args: Any, **kwargs: Any) -> Dict[str, float]:
    # TODO: real implementation – for now, return empty dict => caller will try next source
    return {}


def gmd_series(iso3: str, indicator: str) -> Dict[str, float]:
    """
    Generic helper: given an ISO3 code and an internal indicator name,
    return {year: value} from the GMD dataset.

    Example indicators from indicator_matrix:
      - "gdp_real_growth"
      - "unemployment_rate"
      - "gov_debt_pct_gdp"
      - "gov_balance_pct_gdp"
      - "current_account"
      - "current_account_pct_gdp"
    """
    # TODO: implement real lookup
    return _not_implemented(iso3=iso3, indicator=indicator)


# Convenience wrappers for specific indicators (optional – use if you like:
# they map 1:1 to the indicator names in INDICATOR_MATRIX)
def gmd_gdp_real_growth(iso3: str) -> Dict[str, float]:
    return gmd_series(iso3, "gdp_real_growth")


def gmd_unemployment_rate(iso3: str) -> Dict[str, float]:
    return gmd_series(iso3, "unemployment_rate")


def gmd_gov_debt_pct_gdp(iso3: str) -> Dict[str, float]:
    return gmd_series(iso3, "gov_debt_pct_gdp")


def gmd_gov_balance_pct_gdp(iso3: str) -> Dict[str, float]:
    return gmd_series(iso3, "gov_balance_pct_gdp")


def gmd_current_account(iso3: str) -> Dict[str, float]:
    return gmd_series(iso3, "current_account")


def gmd_current_account_pct_gdp(iso3: str) -> Dict[str, float]:
    return gmd_series(iso3, "current_account_pct_gdp")
