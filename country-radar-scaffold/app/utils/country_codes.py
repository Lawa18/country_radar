# app/utils/country_codes.py
from __future__ import annotations
from functools import lru_cache
from typing import Dict, Optional
import unicodedata
import httpx
import pycountry

# Common aliases → canonical names
ALIASES: Dict[str, str] = {
    "u.s.": "united states",
    "usa": "united states",
    "u.s.a.": "united states",
    "uk": "united kingdom",
    "u.k.": "united kingdom",
    "united mexican states": "mexico",
}

def normalize_country_name(name: str) -> str:
    """ASCII-normalize & lowercase for robust lookups."""
    if not isinstance(name, str):
        return ""
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    s = s.strip().lower()
    return ALIASES.get(s, s)

def resolve_country_codes(name: str) -> Optional[Dict[str, str]]:
    """Return {'iso_alpha_2': 'DE', 'iso_alpha_3': 'DEU'} or None."""
    try:
        nm = normalize_country_name(name)
        c = pycountry.countries.lookup(nm or name)
        return {"iso_alpha_2": c.alpha_2, "iso_alpha_3": c.alpha_3}
    except LookupError:
        return None

@lru_cache(maxsize=512)
def get_currency_code_wb(iso2: str) -> Optional[str]:
    """
    Try World Bank country endpoint for currency (cached).
    Returns a 3-letter code (e.g., 'EUR'), else None.
    """
    try:
        url = f"http://api.worldbank.org/v2/country/{iso2}?format=json"
        with httpx.Client(timeout=10) as client:
            r = client.get(url)
            r.raise_for_status()
            data = r.json()
        if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list) and data[1]:
            node = data[1][0]
            # WB returns 'currency' object with 'id' (ISO3) for many countries
            cur = node.get("currency") or {}
            code = cur.get("id") or cur.get("iso2code") or node.get("currencyCode")
            if isinstance(code, str):
                code = code.strip().upper()
                if len(code) == 3:
                    return code
    except Exception:
        pass
    return None

# Minimal fallback map if WB lookup fails
FALLBACK_CCY = {
    "US": "USD", "SE": "SEK", "GB": "GBP", "DE": "EUR", "FR": "EUR",
    "IT": "EUR", "ES": "EUR", "NL": "EUR", "MX": "MXN", "NG": "NGN",
    "CA": "CAD", "AU": "AUD", "NO": "NOK",
}

def resolve_currency_code(iso_alpha_2: str) -> Optional[str]:
    """Return 3-letter currency for ISO2 (tries WB first, then fallback)."""
    return get_currency_code_wb(iso_alpha_2) or FALLBACK_CCY.get(iso_alpha_2)

# Used to decide ECB/Eurostat policy-rate overrides
EURO_AREA_ISO2 = {
    "AT","BE","CY","DE","EE","ES","FI","FR","GR","IE","IT",
    "LT","LU","LV","MT","NL","PT","SI","SK"
}

