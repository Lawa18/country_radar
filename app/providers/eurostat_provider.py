from __future__ import annotations
from typing import Dict, Optional, Any
import httpx
from functools import lru_cache

EUROSTAT_TIMEOUT = 6.0  # keep reasonable timeout for API calls

# EU + EEA + UK (ISO2) for determining when to use Eurostat
EURO_AREA_ISO2 = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IE", "IT", "LT", "LU",
    "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK", "ES", "IS", "NO", "LI", "GB",
}

def eurostat_debt_to_gdp_annual(iso2: str) -> Dict[str, float]:
    """
    Fetch General Government debt-to-GDP ratio from Eurostat.
    Dataset: gov_10dd_edpt1 (Government deficit/surplus, debt and associated data)
    """
    if iso2.upper() not in EURO_AREA_ISO2:
        return {}
    
    try:
        base_url = "https://data-api.ec.europa.eu/api/dissemination/statistics/1.0/data/gov_10dd_edpt1"
        params = {
            "format": "json",
            "na_item": "GD",  # Government debt
            "sector": "S13",   # General government
            "unit": "PC_GDP",  # Percentage of GDP
            "geo": iso2.upper(),
            "time": "2000-01-01/2025-12-31"
        }
        
        with httpx.Client(timeout=EUROSTAT_TIMEOUT) as client:
            response = client.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            result = {}
            if "value" in data and "dimension" in data:
                time_dim = data["dimension"]["time"]["category"]["label"]
                values = data["value"]
                for idx, period in enumerate(time_dim.keys()):
                    if str(idx) in values and values[str(idx)] is not None:
                        try:
                            year = period[:4]
                            result[year] = float(values[str(idx)])
                        except (ValueError, TypeError):
                            continue
            return result
    except Exception as e:
        print(f"[Eurostat] debt-to-GDP fetch error for {iso2}: {e}")
        return {}

@lru_cache(maxsize=128)
def eurostat_unemployment_monthly(iso2: str) -> Dict[str, float]:
    """
    Fetch monthly unemployment rate from Eurostat.
    Dataset: une_rt_m (Unemployment by sex and age - monthly data)
    """
    if iso2.upper() not in EURO_AREA_ISO2:
        return {}
    
    try:
        base_url = "https://data-api.ec.europa.eu/api/dissemination/statistics/1.0/data/une_rt_m"
        params = {
            "format": "json",
            "s_adj": "SA",     # Seasonally adjusted
            "age": "TOTAL",    # All ages
            "sex": "T",        # Total (both sexes)
            "unit": "PC_ACT",  # Percentage of active population
            "geo": iso2.upper(),
            "time": "2010-01/2025-12"
        }
        
        with httpx.Client(timeout=EUROSTAT_TIMEOUT) as client:
            response = client.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            result = {}
            if "value" in data and "dimension" in data:
                time_dim = data["dimension"]["time"]["category"]["label"]
                values = data["value"]
                for idx, period in enumerate(time_dim.keys()):
                    if str(idx) in values and values[str(idx)] is not None:
                        try:
                            result[period] = float(values[str(idx)])
                        except (ValueError, TypeError):
                            continue
            return result
    except Exception as e:
        print(f"[Eurostat] unemployment fetch error for {iso2}: {e}")
        return {}

@lru_cache(maxsize=128)
def eurostat_cpi_monthly(iso2: str) -> Dict[str, float]:
    """
    Fetch monthly CPI year-over-year inflation from Eurostat.
    Dataset: prc_hicp_manr (HICP - monthly data - annual rate of change)
    """
    if iso2.upper() not in EURO_AREA_ISO2:
        return {}
    
    try:
        base_url = "https://data-api.ec.europa.eu/api/dissemination/statistics/1.0/data/prc_hicp_manr"
        params = {
            "format": "json",
            "coicop": "CP00",   # All-items HICP
            "unit": "RTE",      # Rate (annual rate of change)
            "geo": iso2.upper(),
            "time": "2010-01/2025-12"
        }
        
        with httpx.Client(timeout=EUROSTAT_TIMEOUT) as client:
            response = client.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            result = {}
            if "value" in data and "dimension" in data:
                time_dim = data["dimension"]["time"]["category"]["label"]
                values = data["value"]
                for idx, period in enumerate(time_dim.keys()):
                    if str(idx) in values and values[str(idx)] is not None:
                        try:
                            result[period] = float(values[str(idx)])
                        except (ValueError, TypeError):
                            continue
            return result
    except Exception as e:
        print(f"[Eurostat] CPI fetch error for {iso2}: {e}")
        return {}

@lru_cache(maxsize=2)
def eurostat_policy_rate_monthly() -> Dict[str, float]:
    """
    Fetch monthly ECB Main Refinancing Operations (MRO) rate from ECB SDW (direct SDMX-JSON API).
    Returns {YYYY-MM: value, ...}
    """
    base_url = "https://data-api.ecb.europa.eu/service/data/FM/M.U2.EUR.4F.KR.MRR_FR.LEV"
    params = {
        "format": "sdmx-json",
        "lastNObservations": 120
    }
    try:
        with httpx.Client(timeout=EUROSTAT_TIMEOUT, follow_redirects=True) as client:
            response = client.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            dataset = data["dataSets"][0]
            series_key = next(iter(dataset["series"].keys()))
            obs_map = dataset["series"][series_key]["observations"]
            times = data["structure"]["dimensions"]["observation"][0]["values"]
            result = {}
            for idx_str, arr in obs_map.items():
                idx = int(idx_str)
                meta = times[idx]
                date = meta.get("id") or meta.get("name")
                val = arr[0] if arr else None
                if val is not None and date:
                    result[date] = float(val)
            return result
    except Exception as e:
        print(f"[Eurostat/ECB] policy rate fetch error: {e}")
        return {}

def fetch_eurostat_indicators(iso2: str) -> Dict[str, Dict[str, float]]:
    """
    Fetch Eurostat indicators for EU countries.
    Returns: {"indicator_name": {"YYYY-MM": value, ...}}
    """
    if iso2.upper() not in EURO_AREA_ISO2:
        return {}
    
    indicators = {}
    
    try:
        indicators["CPI_YoY"] = eurostat_cpi_monthly(iso2)
    except Exception:
        indicators["CPI_YoY"] = {}
    
    try:
        indicators["Unemployment_Rate"] = eurostat_unemployment_monthly(iso2)
    except Exception:
        indicators["Unemployment_Rate"] = {}
    
    try:
        indicators["Debt_to_GDP"] = {str(k): v for k, v in eurostat_debt_to_gdp_annual(iso2).items()}
    except Exception:
        indicators["Debt_to_GDP"] = {}

    try:
        indicators["Policy_Rate"] = eurostat_policy_rate_monthly()
    except Exception:
        indicators["Policy_Rate"] = {}

    return indicators

def eurostat_series_to_latest_block(series_data: Dict[str, float], source_name: str) -> Dict[str, Any]:
    """
    Convert Eurostat series data to the format expected by indicator service.
    """
    if not series_data:
        return {"latest": {"value": None, "date": None, "source": None}, "series": {}}
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

def eurostat_series_to_latest_entry(series_data: Dict[str, float], source_name: str) -> Dict[str, Any]:
    """
    Convert Eurostat series data to latest entry format for table-only indicators.
    """
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
