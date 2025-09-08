# app/services/indicator_service.py
from __future__ import annotations

from typing import Dict, Tuple, Optional, Any, List
import time

_EU_UK_ISO2 = {
    "AT","BE","BG","HR","CY","CZ","DE","DK","EE","ES","FI","FR","GR","EL","HU","IE",
    "IT","LT","LU","LV","MT","NL","PL","PT","RO","SE","SI","SK","IS","NO","LI","UK"
}

# --------- Lightweight TTL cache for the assembled country payloads ---------
class _TTLCache:
    def __init__(self, ttl_seconds: int = 900) -> None:  # 15 minutes
        self.ttl = ttl_seconds
        self._store: Dict[str, Tuple[float, Any]] = {}

    def get(self, key: str) -> Optional[Any]:
        hit = self._store.get(key)
        if not hit:
            return None
        exp, value = hit
        if exp < time.time():
            self._store.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any) -> None:
        self._store[key] = (time.time() + self.ttl, value)

_payload_cache = _TTLCache(ttl_seconds=900)

# --------- Utilities ---------
def _safe_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _parse_period_key(k: str) -> Tuple[int, int]:
    """
    Returns (year, month). For annual 'YYYY', month=0 so monthly beats annual in sorting.
    Accepts 'YYYY', 'YYYY-MM' (Eurostat/IMF), or 'YYYYMmm' (some IMF SDMX shapes).
    """
    k = (k or "").strip()
    if len(k) == 7 and k[4] == "-":  # YYYY-MM
        y = int(k[:4])
        m = int(k[5:7])
        return (y, m)
    if len(k) == 6 and k[4] in ("M", "m"):  # YYYYMmm
        y = int(k[:4])
        m = int(k[5:7])
        return (y, m)
    if len(k) == 4 and k.isdigit():         # YYYY
        return (int(k), 0)
    # Fallback: try to coerce
    try:
        y = int(k[:4])
        m = int(k[-2:])
        if 1 <= m <= 12:
            return (y, m)
        return (y, 0)
    except Exception:
        return (0, 0)

def _latest_from_series(series: Dict[str, Any]) -> Optional[Tuple[str, float]]:
    """
    Given a dict {period -> value}, return (latest_period, value) preferring the chronologically latest period.
    """
    if not isinstance(series, dict) or not series:
        return None
    # Filter to finite floats
    items: List[Tuple[str, float]] = []
    for k, v in series.items():
        fv = _safe_float(v)
        if fv is not None:
            items.append((k, fv))
    if not items:
        return None
    items.sort(key=lambda kv: _parse_period_key(kv[0]))  # ascending
    return items[-1]

def _choose_monthly_then_annual(candidates: List[Tuple[str, Dict[str, Any]]],
                                annual_fallback: Optional[Tuple[str, Dict[str, Any]]] = None
                                ) -> Tuple[Optional[str], Optional[str], Optional[float], Dict[str, Dict[str, Any]]]:
    """
    candidates: list of (source_name, series_dict) where series are expected to be monthly (IMF, Eurostat).
    annual_fallback: optional (source_name, series_dict) for annual WB series.

    Returns:
      (source_used, latest_period, latest_value, all_series_map_by_source)
    """
    all_series: Dict[str, Dict[str, Any]] = {}
    # Try monthly sources in order
    for src, ser in candidates:
        if ser:
            all_series[src] = ser
            latest = _latest_from_series(ser)
            if latest:
                lp, lv = latest
                return src, lp, lv, all_series
    # Monthly failed → try annual fallback
    if annual_fallback:
        src, ser = annual_fallback
        if ser:
            all_series[src] = ser
            latest = _latest_from_series(ser)
            if latest:
                lp, lv = latest
                return src, lp, lv, all_series
    # Nothing available
    for src, ser in candidates:
        if ser:
            all_series[src] = ser
    if annual_fallback and annual_fallback[1]:
        all_series[annual_fallback[0]] = annual_fallback[1]
    return None, None, None, all_series

# --------- Providers (import defensively so missing functions don't crash app) ---------
# Country code resolver
try:
    from app.utils.country_codes import resolve_country_codes  # type: ignore
except Exception:  # pragma: no cover
    def resolve_country_codes(country: str) -> Optional[Dict[str, str]]:  # type: ignore
        return None

# ECB provider (policy rate and euro-area list)
_EURO_AREA_SET = set()
try:
    from app.providers.ecb_provider import ecb_policy_rate_for_country, EURO_AREA_ISO2  # type: ignore
    _EURO_AREA_SET = set(EURO_AREA_ISO2 or [])
except Exception:  # pragma: no cover
    def ecb_policy_rate_for_country(iso2: str) -> Dict[str, Any]:  # type: ignore
        return {}

# Eurostat (monthly HICP, unemployment; annual debt ratio is handled in debt service)
try:
    from app.providers.eurostat_provider import (
        eurostat_hicp_yoy_monthly,
        eurostat_unemployment_rate_monthly,
    )  # type: ignore
except Exception:  # pragma: no cover
    def eurostat_hicp_yoy_monthly(iso2: str) -> Dict[str, Any]:  # type: ignore
        return {}
    def eurostat_unemployment_rate_monthly(iso2: str) -> Dict[str, Any]:  # type: ignore
        return {}

# IMF SDMX (monthly)
try:
    from app.providers.imf_provider import (
        imf_cpi_yoy_monthly,
        imf_unemployment_rate_monthly,
        imf_fx_usd_monthly,
        imf_reserves_usd_monthly,
        imf_policy_rate_monthly,
        imf_gdp_growth_quarterly,
    )  # type: ignore
except Exception:  # pragma: no cover
    def imf_cpi_yoy_monthly(iso2: str) -> Dict[str, Any]:  # type: ignore
        return {}
    def imf_unemployment_rate_monthly(iso2: str) -> Dict[str, Any]:  # type: ignore
        return {}
    def imf_fx_usd_monthly(iso2: str) -> Dict[str, Any]:  # type: ignore
        return {}
    def imf_reserves_usd_monthly(iso2: str) -> Dict[str, Any]:  # type: ignore
        return {}
    def imf_policy_rate_monthly(iso2: str) -> Dict[str, Any]:  # type: ignore
        return {}
    def imf_gdp_growth_quarterly(iso2: str) -> Dict[str, Any]:  # type: ignore
        return {}

# World Bank (annual fallback for many indicators)
try:
    from app.providers.wb_provider import (
        wb_cpi_inflation_annual_pct,                # CPI inflation, annual %
        wb_unemployment_rate_annual_pct,            # Unemployment, annual %
        wb_fx_lcu_per_usd_annual,                   # LC per USD, annual (or similar)
        wb_total_reserves_usd_annual,               # Reserves (USD), annual
        wb_gdp_growth_annual_pct,                   # GDP growth (annual %)
        wb_current_account_balance_pct_gdp_annual,  # CAB % GDP
        wb_government_effectiveness_index_annual,   # Gov effectiveness
    )  # type: ignore
except Exception:  # pragma: no cover
    def wb_cpi_inflation_annual_pct(iso2: str) -> Dict[str, Any]:  # type: ignore
        return {}
    def wb_unemployment_rate_annual_pct(iso2: str) -> Dict[str, Any]:  # type: ignore
        return {}
    def wb_fx_lcu_per_usd_annual(iso2: str) -> Dict[str, Any]:  # type: ignore
        return {}
    def wb_total_reserves_usd_annual(iso2: str) -> Dict[str, Any]:  # type: ignore
        return {}
    def wb_gdp_growth_annual_pct(iso2: str) -> Dict[str, Any]:  # type: ignore
        return {}
    def wb_current_account_balance_pct_gdp_annual(iso2: str) -> Dict[str, Any]:  # type: ignore
        return {}
    def wb_government_effectiveness_index_annual(iso2: str) -> Dict[str, Any]:  # type: ignore
        return {}

# Debt service: import defensively. If it exposes a function returning the payload, use it.
try:
    # Expected to return something like {"latest": {...}, "series": {...}, "source": "Eurostat"...}
    from app.services.debt_service import debt_payload_for_iso2  # type: ignore
except Exception:  # pragma: no cover
    def debt_payload_for_iso2(iso2: str) -> Dict[str, Any]:  # type: ignore
        return {}

# --------- Core assembly ---------
def _build_indicator_block(source: Optional[str],
                           latest_period: Optional[str],
                           latest_value: Optional[float],
                           series_by_source: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    if source and latest_period is not None and latest_value is not None:
        return {
            "latest_value": latest_value,
            "latest_period": latest_period,
            "source": source,
            "series": series_by_source,  # keep raw for transparency
        }
    return {
        "latest_value": None,
        "latest_period": None,
        "source": "N/A",
        "series": series_by_source,
    }

def _assemble_policy_rate(iso2: str) -> Dict[str, Any]:
    """
    Policy rate: ECB (euro area) → IMF monthly → N/A (no WB fallback).
    """
    series_map: Dict[str, Dict[str, Any]] = {}
    # ECB override for euro area
    if iso2.upper() in _EURO_AREA_SET:
        ecb_ser = ecb_policy_rate_for_country(iso2) or {}
        if ecb_ser:
            series_map["ECB"] = ecb_ser
            latest = _latest_from_series(ecb_ser)
            if latest:
                lp, lv = latest
                return _build_indicator_block("ECB", lp, lv, series_map)

    # IMF
    imf_ser = imf_policy_rate_monthly(iso2) or {}
    if imf_ser:
        series_map["IMF"] = imf_ser
        latest = _latest_from_series(imf_ser)
        if latest:
            lp, lv = latest
            return _build_indicator_block("IMF", lp, lv, series_map)

    # None available
    return _build_indicator_block(None, None, None, series_map)

def _assemble_cpi_yoy(iso2: str) -> Dict[str, Any]:
    imf_ser = imf_cpi_yoy_monthly(iso2) or {}
    wb_ser  = wb_cpi_inflation_annual_pct(iso2) or {}
    candidates = [("IMF", imf_ser)]
    if iso2.upper() in _EU_UK_ISO2:
        candidates.append(("Eurostat", eurostat_hicp_yoy_monthly(iso2) or {}))
    src, lp, lv, all_ser = _choose_monthly_then_annual(candidates, ("WorldBank", wb_ser))
    return _build_indicator_block(src, lp, lv, all_ser)

def _assemble_unemployment(iso2: str) -> Dict[str, Any]:
    imf_ser = imf_unemployment_rate_monthly(iso2) or {}
    wb_ser  = wb_unemployment_rate_annual_pct(iso2) or {}
    candidates = [("IMF", imf_ser)]
    if iso2.upper() in _EU_UK_ISO2:
        candidates.append(("Eurostat", eurostat_unemployment_rate_monthly(iso2) or {}))
    src, lp, lv, all_ser = _choose_monthly_then_annual(candidates, ("WorldBank", wb_ser))
    return _build_indicator_block(src, lp, lv, all_ser)

def _assemble_fx_rate_usd(iso2: str) -> Dict[str, Any]:
    """
    FX vs USD: IMF monthly → WB annual (LCU per USD or equivalent).
    """
    imf_ser = imf_fx_usd_monthly(iso2) or {}
    wb_ser  = wb_fx_lcu_per_usd_annual(iso2) or {}

    src, lp, lv, all_ser = _choose_monthly_then_annual(
        candidates=[("IMF", imf_ser)],
        annual_fallback=("WorldBank", wb_ser),
    )
    return _build_indicator_block(src, lp, lv, all_ser)

def _assemble_reserves_usd(iso2: str) -> Dict[str, Any]:
    """
    Reserves (USD): IMF monthly → WB annual.
    """
    imf_ser = imf_reserves_usd_monthly(iso2) or {}
    wb_ser  = wb_total_reserves_usd_annual(iso2) or {}

    src, lp, lv, all_ser = _choose_monthly_then_annual(
        candidates=[("IMF", imf_ser)],
        annual_fallback=("WorldBank", wb_ser),
    )
    return _build_indicator_block(src, lp, lv, all_ser)

def _assemble_gdp_growth(iso2: str) -> Dict[str, Any]:
    """
    GDP growth: IMF quarterly (preferred) → WB annual.
    """
    imf_ser = imf_gdp_growth_quarterly(iso2) or {}
    wb_ser  = wb_gdp_growth_annual_pct(iso2) or {}

    src, lp, lv, all_ser = _choose_monthly_then_annual(
        candidates=[("IMF", imf_ser)],
        annual_fallback=("WorldBank", wb_ser),
    )
    return _build_indicator_block(src, lp, lv, all_ser)

def _assemble_cab_pct_gdp(iso2: str) -> Dict[str, Any]:
    """
    Current Account Balance % of GDP: WB only (stable).
    """
    wb_ser = wb_current_account_balance_pct_gdp_annual(iso2) or {}
    latest = _latest_from_series(wb_ser)
    if latest:
        lp, lv = latest
        return _build_indicator_block("WorldBank", lp, lv, {"WorldBank": wb_ser})
    return _build_indicator_block(None, None, None, {"WorldBank": wb_ser} if wb_ser else {})

def _assemble_gov_effectiveness(iso2: str) -> Dict[str, Any]:
    """
    Government Effectiveness: WB Worldwide Governance Indicators (annual).
    """
    wb_ser = wb_government_effectiveness_index_annual(iso2) or {}
    latest = _latest_from_series(wb_ser)
    if latest:
        lp, lv = latest
        return _build_indicator_block("WorldBank", lp, lv, {"WorldBank": wb_ser})
    return _build_indicator_block(None, None, None, {"WorldBank": wb_ser} if wb_ser else {})

def _assemble_debt_block(iso2: str) -> Dict[str, Any]:
    """
    Debt-to-GDP payload is delegated to debt_service with the tiered selection:
    Eurostat → IMF WEO → World Bank → computed (WB levels).
    """
    payload = debt_payload_for_iso2(iso2) or {}
    # Expect payload to already contain latest/source/series; pass through.
    return payload if isinstance(payload, dict) else {}

# --------- Public: main entry used by routes/country.py ---------
def build_country_payload(country: str) -> Dict[str, Any]:
    """
    Build the bundle returned by GET /country-data.
    This function is intentionally defensive:
      - If any provider fails, we still return a coherent payload.
      - Monthly sources are preferred; WB is promoted only when monthly sources fail.
      - Policy rate: ECB (euro area) → IMF; no WB fallback.
    """
    cache_key = f"country_payload::{country}"
    cached = _payload_cache.get(cache_key)
    if cached is not None:
        return cached

    codes = resolve_country_codes(country) if callable(resolve_country_codes) else None
    if not codes:
        # Keep a consistent error shape for the route to return
        data = {"error": "Invalid country name"}
        _payload_cache.set(cache_key, data)
        return data

    iso2 = (codes.get("iso_alpha_2") or "").upper()
    iso3 = (codes.get("iso_alpha_3") or "").upper()

    # Assemble indicator blocks
    try:
        cpi_block = _assemble_cpi_yoy(iso2)
    except Exception:
        cpi_block = _build_indicator_block(None, None, None, {})

    try:
        unemp_block = _assemble_unemployment(iso2)
    except Exception:
        unemp_block = _build_indicator_block(None, None, None, {})

    try:
        fx_block = _assemble_fx_rate_usd(iso2)
    except Exception:
        fx_block = _build_indicator_block(None, None, None, {})

    try:
        reserves_block = _assemble_reserves_usd(iso2)
    except Exception:
        reserves_block = _build_indicator_block(None, None, None, {})

    try:
        policy_block = _assemble_policy_rate(iso2)
    except Exception:
        policy_block = _build_indicator_block(None, None, None, {})

    try:
        gdp_block = _assemble_gdp_growth(iso2)
    except Exception:
        gdp_block = _build_indicator_block(None, None, None, {})

    try:
        cab_block = _assemble_cab_pct_gdp(iso2)
    except Exception:
        cab_block = _build_indicator_block(None, None, None, {})

    try:
        gov_eff_block = _assemble_gov_effectiveness(iso2)
    except Exception:
        gov_eff_block = _build_indicator_block(None, None, None, {})

    try:
        debt_block = _assemble_debt_block(iso2)
    except Exception:
        debt_block = {}

    payload: Dict[str, Any] = {
        "country": country,
        "iso2": iso2,
        "iso3": iso3,
        "indicators": {
            "cpi_yoy": cpi_block,
            "unemployment_rate": unemp_block,
            "fx_rate_usd": fx_block,
            "reserves_usd": reserves_block,
            "policy_rate": policy_block,
            "gdp_growth": gdp_block,
            "current_account_balance_pct_gdp": cab_block,
            "government_effectiveness": gov_eff_block,
        },
        "debt": debt_block,
    }

    _payload_cache.set(cache_key, payload)
    return payload
