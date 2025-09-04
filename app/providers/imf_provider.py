from __future__ import annotations
import os
from typing import Dict
import httpx

IMF_BASE = "https://dataservices.imf.org/REST/SDMX_JSON.svc"
IMF_TIMEOUT = float(os.getenv("UPSTREAM_TIMEOUT", "6.0"))
IMF_START = os.getenv("IMF_START_YEAR", "1990")

def _get(url: str, params: Dict[str, str]) -> dict:
    try:
        with httpx.Client(timeout=IMF_TIMEOUT) as client:
            r = client.get(url, params=params)
            r.raise_for_status()
            return r.json()
    except Exception as e:
        print(f"[IMF] GET failed {url}: {e}")
        return {}

def fetch_imf_sdmx_series(iso2: str) -> Dict[str, Dict[str, float]]:
    """
    Return a mapping like: {"CPI": {...}, "FX Rate": {...}, ...}
    For now, return {} quickly so WB fallbacks fill data without timeouts.
    """
    return {}

def imf_debt_to_gdp_annual(iso3: str) -> Dict[str, float]:
    """
    If/when you wire an IMF WEO ratio series, return {YYYY: value}.
    For now, fast-fail with {} so /v1/debt can proceed to WB fallbacks.
    """
    return {}
