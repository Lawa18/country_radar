from __future__ import annotations
from typing import Dict, Any, Optional
from functools import lru_cache
import httpx

# ---- Eurostat dissemination API (v1.0) base ----
EUROSTAT_BASE = "https://data-api.ec.europa.eu/api/dissemination/statistics/1.0/data"
TIMEOUT = 6.0

# Countries where Eurostat is relevant (EU/EEA + UK). You can expand as needed.
EU_EEA_UK_ISO2 = {
    # EU
    "AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE","EL","GR","HU","IE",
    "IT","LV","LT","LU","MT","NL","PL","PT","RO","SK","SI","ES","SE",
    # EEA (non-EU)
    "IS","LI","NO",
    # UK
    "GB",
}

def _eurostat_geo(iso2: str) -> str:
    """Eurostat uses 'UK' instead of ISO2 'GB'."""
    return "UK" if iso2.upper() == "GB" else iso2.upper()

def _client() -> httpx.Client:
    return httpx.Client(
        timeout=TIMEOUT,
        headers={
            "Accept": "application/json",
            "User-Agent": "country-radar/1.0"
        },
        follow_redirects=True,
    )

def _parse_time_value_series(payload: Dict[str, Any]) -> Dict[str, float]:
    """
    Parse Eurostat JSON (v1.0) into {period -> value}.

    Assumes the request filtered to a single series and only the 'time'
    dimension varies, so the 'value' dict index maps directly to time order.
    """
    try:
        dataset = payload.get("value")
        dims = payload.get("dimension", {})
        time_cat = ((dims.get("time") or {}).get("category") or {}).get("label") or {}
        if not dataset or not time_cat:
            return {}

        out: Dict[str, float] = {}
        # Enumerate time in order; values are at indices "0","1",...
        for idx, period in enumerate(time_cat.keys()):
            key = str(idx)
            if key in dataset and dataset[key] is not None:
                try:
                    out[period] = float(dataset[key])
                except (TypeError, ValueError):
                    pass
        return out
    except Exception:
        return {}

# --------------------------------------------------------------------
# 1) CPI YoY (monthly) – HICP all-items (coicop=CP00). Unit = rate (%).
#    Dataset: prc_hicp_manr
# --------------------------------------------------------------------
@lru_cache(maxsize=128)
def eurostat_cpi_yoy_monthly(iso2: str) -> Dict[str, float]:
    """
    Returns monthly YoY CPI (%) for the country, keyed by 'YYYY-MM'.
    Example: {"2023-01": 8.7, ...}
    """
    geo = _eurostat_geo(iso2)
    if geo not in EU_EEA_UK_ISO2:
        return {}

    params = {
        "format": "json",
        "coicop": "CP00",
        "unit": "RTE",     # monthly annual rate
        "geo": geo,
        # fetch ~15y to be safe; you can narrow later
        "time": "2010-01/2025-12",
    }
    url = f"{EUROSTAT_BASE}/prc_hicp_manr"
    try:
        with _client() as client:
            r = client.get(url, params=params)
            r.raise_for_status()
            series = _parse_time_value_series(r.json())
            return series
    except Exception as e:
        print(f"[Eurostat] CPI fetch error for {iso2}: {e}")
        return {}

# --------------------------------------------------------------------
# 2) Unemployment rate (monthly, seasonally adjusted) – percent.
#    Dataset: une_rt_m (PC, s_adj=SA, age=Y15-74, sex=T)
# --------------------------------------------------------------------
@lru_cache(maxsize=128)
def eurostat_unemployment_rate_monthly(iso2: str) -> Dict[str, float]:
    """
    Returns monthly unemployment rate (%) for the country, keyed by 'YYYY-MM'.
    """
    geo = _eurostat_geo(iso2)
    if geo not in EU_EEA_UK_ISO2:
        return {}

    params = {
        "format": "json",
        "unit": "PC",
        "s_adj": "SA",
        "sex": "T",
        "age": "Y15-74",
        "geo": geo,
        "time": "2010-01/2025-12",
    }
    url = f"{EUROSTAT_BASE}/une_rt_m"
    try:
        with _client() as client:
            r = client.get(url, params=params)
            r.raise_for_status()
            series = _parse_time_value_series(r.json())
            return series
    except Exception as e:
        print(f"[Eurostat] unemployment fetch error for {iso2}: {e}")
        return {}

# --------------------------------------------------------------------
# 3) Debt-to-GDP ratio (annual, %) – General Government (S13)
#    Dataset: gov_10dd_edpt1 (annual EDP data)
#    Filters: sect=S13, na_item=GD (gross debt), unit=PC_GDP
# --------------------------------------------------------------------
@lru_cache(maxsize=128)
def eurostat_debt_to_gdp_annual(iso2: str) -> Dict[str, float]:
    """
    Returns annual general government debt-to-GDP (%) keyed by 'YYYY' for EU/EEA/UK.
    Example: {"2021": 69.3, "2022": 66.1, ...}
    """
    geo = _eurostat_geo(iso2)
    if geo not in EU_EEA_UK_ISO2:
        return {}

    params = {
        "format": "json",
        "sect": "S13",        # General government
        "na_item": "GD",      # Gross debt
        "unit": "PC_GDP",     # % of GDP
        "geo": geo,
        # annual horizon (broad); Eurostat accepts "YYYY/YYYY"
        "time": "1990/2050",
    }
    url = f"{EUROSTAT_BASE}/gov_10dd_edpt1"
    try:
        with _client() as client:
            r = client.get(url, params=params)
            r.raise_for_status()
            series = _parse_time_value_series(r.json())
            # returns keys like "1995", "1996", ...
            # Clean to only keep numeric year keys
            out: Dict[str, float] = {}
            for k, v in series.items():
                if len(k) == 4 and k.isdigit():
                    out[k] = v
            return out
    except Exception as e:
        print(f"[Eurostat] debt-to-GDP fetch error for {iso2}: {e}")
        return {}

# --------------------------------------------------------------------
# Small helper: pick latest value from a series (used by services)
# --------------------------------------------------------------------
def eurostat_latest_value(series_data: Dict[str, float], source_name: str) -> Dict[str, Any]:
    """
    Given a {period: value} dict, return {"value": v, "date": period, "source": source_name}
    for the latest period. If empty, returns N/A block.
    """
    if not series_data:
        return {"value": None, "date": None, "source": None}
    try:
        periods = sorted(series_data.keys())
        last = periods[-1]
        return {"value": series_data[last], "date": last, "source": source_name}
    except Exception:
        return {"value": None, "date": None, "source": None}
