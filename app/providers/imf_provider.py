from __future__ import annotations
from typing import Dict, Any, Optional
import os
import httpx
from functools import lru_cache

IMF_BASE = os.getenv("IMF_BASE", "https://dataservices.imf.org/REST/SDMX_JSON.svc")
IMF_TIMEOUT = float(os.getenv("IMF_TIMEOUT", "8.0"))

def ifs_cpi_index_monthly(iso3: str) -> Dict[str, float]:
    """Monthly CPI inflation year-over-year % from IMF IFS."""
    try:
        # IFS concept: PCPI_PC_CP_A_PT = Consumer Prices, Percent change, Corresponding period previous year
        key = f"M.{iso3}.PCPI_PC_CP_A_PT"
        return imf_series_map("IFS", key, start="2010-01")
    except Exception:
        return {}

def ifs_unemployment_rate_monthly(iso3: str) -> Dict[str, float]:
    """Monthly unemployment rate % from IMF IFS."""
    try:
        # IFS concept: LUR_PT = Unemployment Rate, Percent
        key = f"M.{iso3}.LUR_PT"
        return imf_series_map("IFS", key, start="2010-01")
    except Exception:
        return {}

def ifs_fx_lcu_per_usd_monthly(iso3: str) -> Dict[str, float]:
    """Monthly exchange rate (LCU per USD) from IMF IFS."""
    try:
        # IFS concept: ENDA_XDC_USD_RATE = End of Period, National Currency per US Dollar
        key = f"M.{iso3}.ENDA_XDC_USD_RATE"
        return imf_series_map("IFS", key, start="2010-01")
    except Exception:
        return {}

def ifs_reserves_usd_monthly(iso3: str) -> Dict[str, float]:
    """Monthly total reserves in USD from IMF IFS."""
    try:
        # IFS concept: RAXGS_USD = Total Reserves excluding Gold, US Dollars
        key = f"M.{iso3}.RAXGS_USD"
        return imf_series_map("IFS", key, start="2010-01")
    except Exception:
        return {}

def ifs_gdp_growth_quarterly(iso3: str) -> Dict[str, float]:
    """Quarterly GDP growth % year-over-year from IMF IFS."""
    try:
        # IFS concept: NGDP_R_PC_CP_A_PT = Gross Domestic Product, Real, Percent change, Corresponding period previous year
        key = f"Q.{iso3}.NGDP_R_PC_CP_A_PT"
        return imf_series_map("IFS", key, start="2010-Q1")
    except Exception:
        return {}

def ifs_ca_percent_gdp(iso3: str) -> Dict[str, float]:
    """Quarterly current account balance as % of GDP from IMF IFS."""
    try:
        # IFS concept: BCA_BP6_USD = Current Account, US Dollars
        # We'll need to convert this to % of GDP, but for now return the series
        key = f"Q.{iso3}.BCA_BP6_USD"
        return imf_series_map("IFS", key, start="2010-Q1")
    except Exception:
        return {}

def ifs_policy_rate_monthly(iso3: str) -> Dict[str, float]:
    """Monthly policy/central bank rate from IMF IFS."""
    try:
        # IFS concept: FPOLM_PA = Monetary Policy-Related Interest Rate, Percent per annum
        key = f"M.{iso3}.FPOLM_PA"
        return imf_series_map("IFS", key, start="2010-01")
    except Exception:
        return {}

def fetch_imf_sdmx_series(iso2: str) -> Dict[str, Dict[str, float]]:
    """
    Fetch IMF IFS series for key indicators using ISO2 -> ISO3 mapping.
    Returns: {"indicator_name": {"YYYY-MM": value, ...}}
    """
    # Map ISO2 to ISO3 for IMF API calls
    try:
        import pycountry
        country = pycountry.countries.get(alpha_2=iso2.upper())
        if not country:
            return {}
        iso3 = country.alpha_3
    except Exception:
        return {}
    
    indicators = {}
    
    # Try to fetch each indicator, but don't fail if one doesn't work
    try:
        indicators["CPI_YoY"] = ifs_cpi_index_monthly(iso3)
    except Exception:
        indicators["CPI_YoY"] = {}
    
    try:
        indicators["Unemployment_Rate"] = ifs_unemployment_rate_monthly(iso3)
    except Exception:
        indicators["Unemployment_Rate"] = {}
    
    try:
        indicators["FX_Rate_USD"] = ifs_fx_lcu_per_usd_monthly(iso3)
    except Exception:
        indicators["FX_Rate_USD"] = {}
    
    try:
        indicators["Reserves_USD"] = ifs_reserves_usd_monthly(iso3)
    except Exception:
        indicators["Reserves_USD"] = {}
    
    try:
        indicators["Policy_Rate"] = ifs_policy_rate_monthly(iso3)
    except Exception:
        indicators["Policy_Rate"] = {}
    
    try:
        indicators["GDP_Growth"] = ifs_gdp_growth_quarterly(iso3)
    except Exception:
        indicators["GDP_Growth"] = {}
    
    try:
        indicators["Current_Account"] = ifs_ca_percent_gdp(iso3)
    except Exception:
        indicators["Current_Account"] = {}
    
    return indicators

def imf_debt_to_gdp_annual(iso3: str) -> Dict[str, float]:
    """
    Placeholder for IMF WEO annual debt/GDP ratio series (by ISO3).
    Returning {} ensures your strict compute order continues without breaking imports.
    """
    return {}
    
def _client() -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(IMF_TIMEOUT, connect=min(IMF_TIMEOUT/2, 4.0)),
        headers={"User-Agent": "country-radar/1.0 (+https://country-radar.onrender.com)"},
        limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
    )

def _compactdata(dataset: str, key: str, start: Optional[str], end: Optional[str]) -> Dict[str, Any]:
    params = {}
    if start: params["startPeriod"] = start
    if end:   params["endPeriod"]   = end
    url = f"{IMF_BASE}/CompactData/{dataset}/{key}"
    with _client() as c:
        r = c.get(url, params=params)
        r.raise_for_status()
        return r.json()

def _parse_obs_to_map(payload: Dict[str, Any]) -> Dict[str, float]:
    """
    Defensive SDMX parser: returns {period -> value} for either monthly (YYYY-MM) or annual (YYYY).
    Works with common SDMX 2.1 JSON variants.
    """
    try:
        compact = payload.get("CompactData") or {}
        dataset = compact.get("DataSet") or {}
        series = dataset.get("Series") or {}
        if isinstance(series, list):
            series = series[0] if series else {}
        obs = series.get("Obs") or []
        out: Dict[str, float] = {}
        for o in obs:
            period = o.get("TIME_PERIOD") or o.get("@TIME_PERIOD") or o.get("Time") or o.get("@Time")
            val = o.get("OBS_VALUE") or o.get("@OBS_VALUE") or o.get("value") or o.get("@value")
            if period is None or val in (None, "", "NA"):
                continue
            try:
                out[str(period)] = float(val)
            except Exception:
                continue
        # sort by period if it looks like year or year-month
        return dict(sorted(out.items(), key=lambda k: k[0]))
    except Exception as e:
        print(f"[IMF] parse error: {e}")
        return {}

@lru_cache(maxsize=256)
def imf_series_map(dataset: str, key: str, start: Optional[str] = None, end: Optional[str] = None) -> Dict[str, float]:
    """
    Fetches a trimmed IMF series and returns {period: value}.
    Example:
      dataset="IFS", key="M.DEU.PCPI_PC_CP_A_PT"   # CPI yoy %, Germany, monthly
      start="2010-01", end="2025-12"
    """
    try:
        data = _compactdata(dataset, key, start, end)
        return _parse_obs_to_map(data)
    except Exception as e:
        print(f"[IMF] fetch error {dataset}/{key}: {e}")
        return {}

def imf_latest(dataset: str, key: str, start: Optional[str] = None, end: Optional[str] = None) -> Optional[tuple[str, float]]:
    series = imf_series_map(dataset, key, start, end)
    if not series:
        return None
    # last period lexicographically -> latest
    last = sorted(series.keys())[-1]
    return last, series[last]

def imf_series_to_latest_block(series_data: Dict[str, float], source_name: str) -> Dict[str, Any]:
    """Convert IMF series data to the format expected by indicator service."""
    if not series_data:
        return {"latest": {"value": None, "date": None, "source": None}, "series": {}}
    
    # Get the latest value
    sorted_periods = sorted(series_data.keys())
    latest_period = sorted_periods[-1]
    latest_value = series_data[latest_period]
    
    return {
        "latest": {
            "value": latest_value,
            "date": latest_period,
            "source": source_name
        },
        "series": series_data
    }

def imf_series_to_latest_entry(series_data: Dict[str, float], source_name: str) -> Dict[str, Any]:
    """Convert IMF series data to latest entry format for table-only indicators."""
    if not series_data:
        return {"value": None, "date": None, "source": None}
    
    sorted_periods = sorted(series_data.keys())
    latest_period = sorted_periods[-1]
    latest_value = series_data[latest_period]
    
    return {
        "value": latest_value,
        "date": latest_period,
        "source": source_name
    }
