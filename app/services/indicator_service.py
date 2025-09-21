# app/services/indicator_service.py
from __future__ import annotations

from typing import Dict, Tuple, Optional, Any, List
import time

# EU/EEA + UK (use ISO2 "GB" for the United Kingdom)
_EU_UK_ISO2 = {
    "AT","BE","BG","HR","CY","CZ","DE","DK","EE","ES","FI","FR","GR","EL","HU","IE",
    "IT","LT","LU","LV","MT","NL","PL","PT","RO","SE","SI","SK","IS","NO","LI","GB"
}

# --------- Lightweight TTL cache for the assembled country payloads ---------
class _TTLCache:
    def __init__(self, ttl_seconds: int = 900) -> None:  # 15 minutes
        self.ttl = ttl_seconds
        self._store: Dict[str, Tuple[float, Any]] = {}

def _format_debt_for_lite_block(raw: dict) -> dict:
    """
    Normalize debt payload so lite/debt block always reflects the newest series point.
    Does not remove fields; returns the standard debt block shape expected by lite.
    """
    series = raw.get("series") or {}
    # ensure string keys
    if isinstance(series, dict):
        series = {str(k): v for k, v in series.items()}
    latest = raw.get("latest") if isinstance(raw.get("latest"), dict) else {}

    # derive period
    latest_period = raw.get("latest_period") or latest.get("period") or latest.get("year")
    if not latest_period and series:
        try:
            latest_period = max(series.keys())  # keys are strings
        except Exception:
            latest_period = None

    # derive value: prefer the value at selected period in series
    latest_value = raw.get("latest_value")
    if latest_value is None and isinstance(latest, dict):
        latest_value = latest.get("value")
    if latest_period and isinstance(series, dict) and latest_period in series and series[latest_period] is not None:
        try:
            latest_value = float(series[latest_period])
        except Exception:
            pass

    source = raw.get("source") or latest.get("source") or "N/A"

    return {
        "latest_value": latest_value,
        "latest_period": latest_period,
        "source": source or "N/A",
        "series": {},  # lite omits history
        "latest": {"period": latest_period, "value": latest_value, "source": source or "N/A"},
    }

    def get(self, key: str) -> Any:
        ent = self._store.get(key)
        if not ent:
            return None
        ts, val = ent
        if (time.time() - ts) > self.ttl:
            try:
                del self._store[key]
            except Exception:
                pass
            return None
        return val

    def set(self, key: str, value: Any) -> None:
        self._store[key] = (time.time(), value)


_payload_cache = _TTLCache(ttl_seconds=3600)  # assembled payloads ~1h


# --------- Small helpers ---------
def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        f = float(v)
        if f != f:  # NaN
            return None
        return f
    except Exception:
        return None

def _parse_period_key(k: str) -> Tuple[int, int]:
    """
    Convert period keys into sortable tuples: (year, month_index)
    Supports:
      - YYYY
      - YYYY-MM
      - YYYY-M (single digit month)
      - YYYY-Q1..Q4  (mapped to months 3,6,9,12)
    Fallback: try last 2 chars as month else (year, 0)
    """
    try:
        if len(k) == 4 and k.isdigit():  # YYYY
            return (int(k), 0)
        if "-" in k:
            y, rest = k.split("-", 1)
            if rest.startswith(("Q","q")) and len(rest) == 2:
                q = int(rest[1])
                m = min(max(q,1),4) * 3
                return (int(y), m)
            m = int(rest)
            return (int(y), min(max(m,0),12))
        if k.upper().startswith("Q") and len(k) == 2:
            # just in case "Qx" appears alone; treat as (0, quarter*3)
            q = int(k[1])
            return (0, min(max(q,1),4) * 3)
        # e.g. 202501 -> (2025, 1)
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
    Only returns non-null, finite values.
    """
    if not isinstance(series, dict) or not series:
        return None
    items: List[Tuple[str, float]] = []
    for k, v in series.items():
        fv = _safe_float(v)
        if fv is not None:
            items.append((k, fv))
    if not items:
        return None
    items.sort(key=lambda kv: _parse_period_key(kv[0]))  # ascending
    return items[-1]

def _choose_monthly_then_annual(monthly_candidates, annual_fallback) -> tuple:
    """
    Pick the first usable monthly series, otherwise fall back to annual.
    Accepts monthly_candidates as:
      - dict: {"IMF": {...}, "Eurostat": {...}}
      - list of (label, series): [("IMF", {...}), ("Eurostat", {...})]
      - list of series (no labels): [ {...}, {...} ]  -> auto-labels: IMF, Eurostat, Source3...
    Returns (source_str, latest_period_str, latest_value_float, all_series_map) for _build_indicator_block.
    """
    # Helper: monthly-looking key "YYYY-MM"
    def _looks_monthly_key(k: str) -> bool:
        return isinstance(k, str) and len(k) == 7 and k[4] == "-" and k[:4].isdigit() and k[5:7].isdigit()

    # Normalize candidates to a list of (src, ser)
    norm: list[tuple[str, dict]] = []
    if isinstance(monthly_candidates, dict):
        # Dict preserves insertion order in Py3.7+
        for src, ser in monthly_candidates.items():
            norm.append((str(src), ser or {}))
    elif isinstance(monthly_candidates, (list, tuple)):
        if monthly_candidates and isinstance(monthly_candidates[0], (list, tuple)) and len(monthly_candidates[0]) == 2 and isinstance(monthly_candidates[0][0], str):
            # Already a list of (label, series)
            for src, ser in monthly_candidates:
                norm.append((str(src), ser or {}))
        else:
            # Plain list of series with no labels → auto-label
            default_labels = ["IMF", "Eurostat", "Source3", "Source4"]
            for i, ser in enumerate(monthly_candidates):
                label = default_labels[i] if i < len(default_labels) else f"Source{i+1}"
                norm.append((label, ser or {}))
    else:
        norm = []

    all_series: dict[str, dict] = {}

    # Keep first non-empty monthly-looking series
    for src, ser in norm:
        if not ser:
            continue
        all_series[src] = ser
        latest = _latest_from_series(ser)
        if not latest:
            continue
        lp, lv = latest
        # Require monthly-looking key (YYYY-MM)
        if not _looks_monthly_key(lp):
            # Not a monthly series after all; try next candidate
            continue
        return src, lp, lv, all_series

    # Nothing monthly usable → fall back to annual (years "YYYY")
    src = "WorldBank"
    latest = _latest_from_series(annual_fallback or {})
    if latest:
        lp, lv = latest  # lp is "YYYY"
        all_series[src] = annual_fallback or {}
        return src, lp, lv, all_series

    # Truly nothing
    return "N/A", None, None, all_series

# --------- Providers (import defensively so missing functions don't crash app) ---------
# Country code resolver
try:
    from app.utils.country_codes import resolve_country_codes  # type: ignore
except Exception:  # pragma: no cover
    def resolve_country_codes(country: str) -> Optional[Dict[str, str]]:  # type: ignore
        return None

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

# IMF (monthly CPI YoY, unemployment, FX, reserves, policy rate; quarterly GDP growth; WEO debt if needed)
try:
    from app.providers.imf_provider import (
        imf_cpi_yoy_monthly,
        imf_unemployment_rate_monthly,
        imf_fx_usd_monthly,
        imf_reserves_usd_monthly,
        imf_policy_rate_monthly,
        imf_gdp_growth_quarterly,
        imf_weo_debt_to_gdp_annual,  # only for debt service if used there
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
    def imf_weo_debt_to_gdp_annual(iso3: str) -> Dict[str, Any]:  # type: ignore
        return {}

# World Bank (annual fallbacks + stable indicators)
try:
    from app.providers.wb_provider import (  # type: ignore
        wb_cpi_yoy_annual,                         # e.g., FP.CPI.TOTL.ZG
        wb_unemployment_rate_annual,               # e.g., SL.UEM.TOTL.ZS (annual)
        wb_fx_rate_usd_annual,                     # e.g., PA.NUS.FCRF (LCU per USD)
        wb_reserves_usd_annual,                    # e.g., FI.RES.TOTL.CD
        wb_gdp_growth_annual_pct,                  # NY.GDP.MKTP.KD.ZG
        wb_current_account_balance_pct_gdp_annual, # BN.CAB.XOKA.GD.ZS
        wb_government_effectiveness_annual,        # GE.EST
    )
except Exception:  # pragma: no cover
    def wb_cpi_yoy_annual(iso3: str) -> Dict[str, Any]:  # type: ignore
        return {}
    def wb_unemployment_rate_annual(iso3: str) -> Dict[str, Any]:  # type: ignore
        return {}
    def wb_fx_rate_usd_annual(iso3: str) -> Dict[str, Any]:  # type: ignore
        return {}
    def wb_reserves_usd_annual(iso3: str) -> Dict[str, Any]:  # type: ignore
        return {}
    def wb_gdp_growth_annual_pct(iso3: str) -> Dict[str, Any]:  # type: ignore
        return {}
    def wb_current_account_balance_pct_gdp_annual(iso3: str) -> Dict[str, Any]:  # type: ignore
        return {}
    def wb_government_effectiveness_annual(iso3: str) -> Dict[str, Any]:  # type: ignore
        return {}

# ECB policy rate (MRO) monthly with per-country mapping
try:
    from app.providers.ecb_provider import EURO_AREA_ISO2, ecb_policy_rate_for_country  # type: ignore
except Exception:  # pragma: no cover
    EURO_AREA_ISO2 = set()  # type: ignore
    def ecb_policy_rate_for_country(iso2: str) -> Dict[str, Any]:  # type: ignore
        return {}

# Debt service — we will import lazily and try several function names
def _wb_pick(name: str):
    """
    Returns (mode, func) where mode is 'country' or 'iso2' meaning how to call it.
    """
    try:
        from app.services.debt_service import compute_debt_payload_by_iso2 as f  # type: ignore
        return ("iso2", f)
    except Exception:
        pass
    try:
        from app.services.debt_service import build_debt_payload_by_iso2 as f  # type: ignore
        return ("iso2", f)
    except Exception:
        pass
    try:
        from app.services.debt_service import get_debt_payload_by_iso2 as f  # type: ignore
        return ("iso2", f)
    except Exception:
        pass
    try:
        from app.services.debt_service import compute_debt_payload as f  # type: ignore
        return ("country", f)
    except Exception:
        pass
    try:
        from app.services.debt_service import debt_payload_for_country as f  # type: ignore
        return ("country", f)
    except Exception:
        pass
    return (None, None)

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
            "series": series_by_source or {},
        }
    # no latest
    return {
        "latest_value": None,
        "latest_period": None,
        "source": "N/A",
        "series": series_by_source or {},
    }

def _assemble_cpi(iso2: str, iso3: str) -> Dict[str, Any]:
    """
    CPI YoY: IMF monthly → (EU only) Eurostat monthly → WB annual fallback
    """
    try:
        imf_ser = imf_cpi_yoy_monthly(iso2) or {}
    except Exception:
        imf_ser = {}

    euro_ser: Dict[str, float] = {}
    if iso2 in _EU_UK_ISO2:
        try:
            euro_ser = eurostat_hicp_yoy_monthly(iso2) or {}
        except Exception:
            euro_ser = {}

    try:
        wb_ser = wb_cpi_yoy_annual(iso3) or {}
    except Exception:
        wb_ser = {}

    src, lp, lv, all_series = _choose_monthly_then_annual(
        [("IMF", imf_ser), ("Eurostat", euro_ser)],
        wb_ser,
    )
    # lite: series flattened/omitted; we still pass an empty series map for size
    return _build_indicator_block(src if src != "N/A" else None, lp, lv, {})


def _assemble_unemployment(iso2: str, iso3: str) -> Dict[str, Any]:
    """
    Unemployment rate: IMF monthly → (EU only) Eurostat monthly → WB annual fallback
    """
    try:
        imf_ser = imf_unemployment_rate_monthly(iso2) or {}
    except Exception:
        imf_ser = {}

    euro_ser: Dict[str, float] = {}
    if iso2 in _EU_UK_ISO2:
        try:
            euro_ser = eurostat_unemployment_rate_monthly(iso2) or {}
        except Exception:
            euro_ser = {}

    try:
        wb_ser = wb_unemployment_rate_annual(iso3) or {}
    except Exception:
        wb_ser = {}

    src, lp, lv, all_series = _choose_monthly_then_annual(
        [("IMF", imf_ser), ("Eurostat", euro_ser)],
        wb_ser,
    )
    return _build_indicator_block(src if src != "N/A" else None, lp, lv, {})


def _assemble_fx(iso2: str, iso3: str) -> Dict[str, Any]:
    """
    FX (LCU per USD): IMF monthly → WB annual fallback
    Note: for euro members IMF monthly often ends at 1998-12; WB annual may be newer.
    """
    try:
        imf_ser = imf_fx_usd_monthly(iso2) or {}
    except Exception:
        imf_ser = {}

    try:
        wb_ser = wb_fx_rate_usd_annual(iso3) or {}
    except Exception:
        wb_ser = {}

    src, lp, lv, all_series = _choose_monthly_then_annual(
        [("IMF", imf_ser)],  # no Eurostat FX in current design
        wb_ser,
    )
    return _build_indicator_block(src if src != "N/A" else None, lp, lv, {})


def _assemble_reserves(iso2: str, iso3: str) -> Dict[str, Any]:
    """
    Reserves (USD): IMF monthly → WB annual fallback
    """
    try:
        imf_ser = imf_reserves_usd_monthly(iso2) or {}
    except Exception:
        imf_ser = {}

    try:
        wb_ser = wb_reserves_usd_annual(iso3) or {}
    except Exception:
        wb_ser = {}

    src, lp, lv, all_series = _choose_monthly_then_annual(
        [("IMF", imf_ser)],
        wb_ser,
    )
    return _build_indicator_block(src if src != "N/A" else None, lp, lv, {})

def _assemble_policy_rate(iso2: str) -> Dict[str, Any]:
    """
    Policy rate: ECB MRO monthly for euro area (override) → IMF monthly.
    No WB fallback by design.
    """
    ecb_ser: Dict[str, float] = {}
    imf_ser: Dict[str, float] = {}

    # 1) ECB override for euro area
    if iso2 in EURO_AREA_ISO2:
        try:
            # expects monthly series keyed like "YYYY-MM"
            ecb_ser = ecb_policy_rate_for_country(iso2) or {}
        except Exception:
            ecb_ser = {}
        if ecb_ser:
            latest = _latest_from_series(ecb_ser)
            if latest:
                lp, lv = latest
                return _build_indicator_block("ECB", lp, lv, {"ECB": ecb_ser})
        # If ECB empty or failed, fall through to IMF

    # 2) IMF monthly (all countries, incl. euro area as fallback)
    try:
        imf_ser = imf_policy_rate_monthly(iso2) or {}
    except Exception:
        imf_ser = {}
    if imf_ser:
        latest = _latest_from_series(imf_ser)
        if latest:
            lp, lv = latest
            return _build_indicator_block("IMF", lp, lv, {"IMF": imf_ser})

    # 3) Nothing available
    series_map: Dict[str, Dict[str, float]] = {}
    if ecb_ser:
        series_map["ECB"] = ecb_ser
    if imf_ser:
        series_map["IMF"] = imf_ser
    return _build_indicator_block(None, None, None, series_map)

def _assemble_gdp_growth(iso2: str, iso3: str) -> Dict[str, Any]:
    """
    GDP growth: IMF quarterly → WB annual fallback
    """
    def _looks_quarterly_key(k: str) -> bool:
        # e.g. "2025-Q2"
        return isinstance(k, str) and len(k) == 7 and k[4:6] == "-Q" and k[:4].isdigit() and k[6].isdigit()

    # Try IMF quarterly first
    try:
        imf_q = imf_gdp_growth_quarterly(iso2) or {}
    except Exception:
        imf_q = {}

    if imf_q:
        latest = _latest_from_series(imf_q)
        if latest:
            lp, lv = latest
            if _looks_quarterly_key(lp):
                return _build_indicator_block("IMF", lp, lv, {})  # lite: omit history

    # Fallback: WB annual
    try:
        wb_a = wb_gdp_growth_annual_pct(iso3) or {}
    except Exception:
        wb_a = {}

    if wb_a:
        latest = _latest_from_series(wb_a)
        if latest:
            lp, lv = latest  # "YYYY"
            return _build_indicator_block("WorldBank", lp, lv, {})

    return _build_indicator_block(None, None, None, {})

def _assemble_cab_pct_gdp(iso3: str) -> Dict[str, Any]:
    """
    Current Account Balance % of GDP: WB only (stable).
    """
    wb_ser = wb_current_account_balance_pct_gdp_annual(iso3) or {}
    latest = _latest_from_series(wb_ser)
    if latest:
        lp, lv = latest
        return _build_indicator_block("WorldBank", lp, lv, {"WorldBank": wb_ser})
    return _build_indicator_block(None, None, None, {"WorldBank": wb_ser} if wb_ser else {})

def _assemble_gov_effectiveness(iso3: str) -> Dict[str, Any]:
    """
    Government Effectiveness (WGI): WB only.
    """
    wb_ser = wb_government_effectiveness_annual(iso3) or {}
    latest = _latest_from_series(wb_ser)
    if latest:
        lp, lv = latest
        return _build_indicator_block("WorldBank", lp, lv, {"WorldBank": wb_ser})
    return _build_indicator_block(None, None, None, {"WorldBank": wb_ser} if wb_ser else {})

def _assemble_debt_block(country: str, iso2: str) -> Dict[str, Any]:
    """
    Debt-to-GDP: delegate to debt_service (which enforces Eurostat → WEO → WB → computed).
    We DO NOT overwrite this with other sources here.
    Returns a block with keys: latest_value, latest_period, source, series
    (If the service returns a 'latest' sub-dict, we normalize top-level fields from it.)
    """
    mode, fn = _wb_pick("debt")
    if not callable(fn):
        # graceful empty block
        return {
            "latest_value": None,
            "latest_period": None,
            "source": "N/A",
            "series": {},
        }
    try:
        if mode == "iso2":
            raw = fn(iso2)  # type: ignore
        else:
            raw = fn(country)  # type: ignore
    except Exception:
        raw = None

    if not isinstance(raw, dict):
        return {
            "latest_value": None,
            "latest_period": None,
            "source": "N/A",
            "series": {},
        }

    # Normalize
    latest = raw.get("latest") if isinstance(raw.get("latest"), dict) else {}
    latest_period = raw.get("latest_period") or latest.get("period")
    # Fallbacks for period: prefer explicit 'year', else infer from series keys
    if not latest_period:
        latest_period = latest.get("year")
    if not latest_period and isinstance(series, dict) and series:
        try:
            latest_period = max(series.keys())  # years are strings
        except Exception:
            latest_period = None
    latest_value  = raw.get("latest_value")  if raw.get("latest_value") is not None else latest.get("value")
    source        = raw.get("source")        or latest.get("source") or "N/A"
    series        = raw.get("series")        or {}

    return {
        "latest_value": latest_value,
        "latest_period": latest_period,
        "source": source or "N/A",
        "series": series if isinstance(series, dict) else {},
        "latest": {"period": latest_period, "value": latest_value, "source": source or "N/A"},
    }

# --------- Public: build_country_payload ---------
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
        # Best-effort passthrough of provided name; leave codes empty but structured
        payload = {
            "country": country,
            "iso2": None,
            "iso3": None,
            "indicators": {
                "cpi_yoy": _build_indicator_block(None, None, None, {}),
                "unemployment_rate": _build_indicator_block(None, None, None, {}),
                "fx_rate_usd": _build_indicator_block(None, None, None, {}),
                "reserves_usd": _build_indicator_block(None, None, None, {}),
                "policy_rate": _build_indicator_block(None, None, None, {}),
                "gdp_growth": _build_indicator_block(None, None, None, {}),
                "current_account_balance_pct_gdp": _build_indicator_block(None, None, None, {}),
                "government_effectiveness": _build_indicator_block(None, None, None, {}),
            },
            "debt": {
                "latest_value": None,
                "latest_period": None,
                "source": "N/A",
                "series": {},
            },
            "error": "Invalid country name",
        }
        _payload_cache.set(cache_key, payload)
        return payload

    iso2 = codes.get("iso_alpha_2")
    iso3 = codes.get("iso_alpha_3")

    # Assemble each indicator with the source order rules
    try:
        cpi_block = _assemble_cpi(iso2, iso3)
    except Exception:
        cpi_block = _build_indicator_block(None, None, None, {})

    try:
        unemp_block = _assemble_unemployment(iso2, iso3)
    except Exception:
        unemp_block = _build_indicator_block(None, None, None, {})

    try:
        fx_block = _assemble_fx(iso2, iso3)
    except Exception:
        fx_block = _build_indicator_block(None, None, None, {})

    try:
        reserves_block = _assemble_reserves(iso2, iso3)
    except Exception:
        reserves_block = _build_indicator_block(None, None, None, {})

    try:
        policy_block = _assemble_policy_rate(iso2)
    except Exception:
        policy_block = _build_indicator_block(None, None, None, {})

    try:
        gdp_block = _assemble_gdp_growth(iso2, iso3)
    except Exception:
        gdp_block = _build_indicator_block(None, None, None, {})

    try:
        cab_block = _assemble_cab_pct_gdp(iso3)
    except Exception:
        cab_block = _build_indicator_block(None, None, None, {})

    try:
        gov_eff_block = _assemble_gov_effectiveness(iso3)
    except Exception:
        gov_eff_block = _build_indicator_block(None, None, None, {})

    try:
        debt_block = _assemble_debt_block(country, iso2)
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
