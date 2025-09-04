# app/providers/eurostat_provider.py
import os, httpx
from typing import Dict

EURO_TIMEOUT = float(os.getenv("UPSTREAM_TIMEOUT", "6.0"))
EURO_START = os.getenv("EURO_START_YEAR", "1995")

def eurostat_debt_to_gdp_annual(iso2: str) -> Dict[str, float]:
    """
    Return { 'YYYY': value } or {} on failure/slow.
    Keep params narrow (EURO_START:9999) and add timeout.
    """
    try:
        with httpx.Client(timeout=EURO_TIMEOUT) as client:
            # TODO: replace with your real Eurostat dataset + filters
            # r = client.get(EUROSTAT_URL, params={...})
            # js = r.json()
            # return parsed_dict
            return {}
    except Exception as e:
        print(f"[EUROSTAT] debt ratio failed for {iso2}: {e}")
        return {}
