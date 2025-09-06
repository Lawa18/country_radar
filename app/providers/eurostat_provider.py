from __future__ import annotations
from typing import Dict
import httpx

EUROSTAT_TIMEOUT = 3.0  # keep very short so we never hang Render

# Optional: use this later if you want to gate EU-only lookups
EURO_AREA_ISO2 = {
    # EU + EEA + UK (ISO2). Safe to adjust later.
    "AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE","GR","HU","IE","IT","LT","LU",
    "LV","MT","NL","PL","PT","RO","SE","SI","SK","ES","IS","NO","LI","GB",
}

def eurostat_debt_to_gdp_annual(iso2: str) -> Dict[str, float]:
    """
    Placeholder that FAILS FAST and returns {} (no Eurostat data yet).
    This keeps your service stable and lets Debt-to-GDP fall back to WB/IMF
    exactly as your compute order defines.

    When we wire the real Eurostat call, we will:
      - Hit the SDMX JSON endpoint for the General Government gross debt as % of GDP
      - Filter sector=S13, unit=PC_GDP, freq=A, geo=<iso2>
      - Parse to { "YYYY": value } floats, newest year included
    """
    # Example skeleton (commented out to avoid 404s/timeouts until we confirm dataset):
    # try:
    #     base = "https://ec.europa.eu/eurostat/api/discoveries/tgm/table"  # placeholder
    #     # TODO: replace with correct SDMX dataset & params; keep timeout short
    #     with httpx.Client(timeout=EUROSTAT_TIMEOUT) as client:
    #         resp = client.get(base, params={...})
    #         resp.raise_for_status()
    #         data = resp.json()
    #         # parse into {"YYYY": float}
    #         return parsed
    # except Exception:
    #     return {}
    return {}
