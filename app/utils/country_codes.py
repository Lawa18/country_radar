# app/utils/country_codes.py
from __future__ import annotations
from typing import Optional, Dict
import re

try:
    import pycountry  # optional but nice to have
except Exception:
    pycountry = None

# Minimal, fast internal map (covers common names & synonyms).
# You can extend this safely over time.
_BUILTIN: Dict[str, Dict[str, str]] = {
    # Core set
    "mexico":              {"name": "Mexico", "iso_alpha_2": "MX", "iso_alpha_3": "MEX", "iso_numeric": "484"},
    "united states":       {"name": "United States", "iso_alpha_2": "US", "iso_alpha_3": "USA", "iso_numeric": "840"},
    "usa":                 {"name": "United States", "iso_alpha_2": "US", "iso_alpha_3": "USA", "iso_numeric": "840"},
    "u.s.":                {"name": "United States", "iso_alpha_2": "US", "iso_alpha_3": "USA", "iso_numeric": "840"},
    "united kingdom":      {"name": "United Kingdom", "iso_alpha_2": "GB", "iso_alpha_3": "GBR", "iso_numeric": "826"},
    "uk":                  {"name": "United Kingdom", "iso_alpha_2": "GB", "iso_alpha_3": "GBR", "iso_numeric": "826"},
    "germany":             {"name": "Germany", "iso_alpha_2": "DE", "iso_alpha_3": "DEU", "iso_numeric": "276"},
    "france":              {"name": "France", "iso_alpha_2": "FR", "iso_alpha_3": "FRA", "iso_numeric": "250"},
    "italy":               {"name": "Italy", "iso_alpha_2": "IT", "iso_alpha_3": "ITA", "iso_numeric": "380"},
    "spain":               {"name": "Spain", "iso_alpha_2": "ES", "iso_alpha_3": "ESP", "iso_numeric": "724"},
    "sweden":              {"name": "Sweden", "iso_alpha_2": "SE", "iso_alpha_3": "SWE", "iso_numeric": "752"},
    "norway":              {"name": "Norway", "iso_alpha_2": "NO", "iso_alpha_3": "NOR", "iso_numeric": "578"},
    "denmark":             {"name": "Denmark", "iso_alpha_2": "DK", "iso_alpha_3": "DNK", "iso_numeric": "208"},
    "netherlands":         {"name": "Netherlands", "iso_alpha_2": "NL", "iso_alpha_3": "NLD", "iso_numeric": "528"},
    "switzerland":         {"name": "Switzerland", "iso_alpha_2": "CH", "iso_alpha_3": "CHE", "iso_numeric": "756"},
    "canada":              {"name": "Canada", "iso_alpha_2": "CA", "iso_alpha_3": "CAN", "iso_numeric": "124"},
    "brazil":              {"name": "Brazil", "iso_alpha_2": "BR", "iso_alpha_3": "BRA", "iso_numeric": "076"},
    "argentina":           {"name": "Argentina", "iso_alpha_2": "AR", "iso_alpha_3": "ARG", "iso_numeric": "032"},
    "chile":               {"name": "Chile", "iso_alpha_2": "CL", "iso_alpha_3": "CHL", "iso_numeric": "152"},
    "peru":                {"name": "Peru", "iso_alpha_2": "PE", "iso_alpha_3": "PER", "iso_numeric": "604"},
    "colombia":            {"name": "Colombia", "iso_alpha_2": "CO", "iso_alpha_3": "COL", "iso_numeric": "170"},
    "japan":               {"name": "Japan", "iso_alpha_2": "JP", "iso_alpha_3": "JPN", "iso_numeric": "392"},
    "china":               {"name": "China", "iso_alpha_2": "CN", "iso_alpha_3": "CHN", "iso_numeric": "156"},
    "india":               {"name": "India", "iso_alpha_2": "IN", "iso_alpha_3": "IND", "iso_numeric": "356"},
    "korea, republic of":  {"name": "Korea, Republic of", "iso_alpha_2": "KR", "iso_alpha_3": "KOR", "iso_numeric": "410"},
    "south korea":         {"name": "Korea, Republic of", "iso_alpha_2": "KR", "iso_alpha_3": "KOR", "iso_numeric": "410"},
    "russian federation":  {"name": "Russian Federation", "iso_alpha_2": "RU", "iso_alpha_3": "RUS", "iso_numeric": "643"},
    "russia":              {"name": "Russian Federation", "iso_alpha_2": "RU", "iso_alpha_3": "RUS", "iso_numeric": "643"},
    "australia":           {"name": "Australia", "iso_alpha_2": "AU", "iso_alpha_3": "AUS", "iso_numeric": "036"},
    "new zealand":         {"name": "New Zealand", "iso_alpha_2": "NZ", "iso_alpha_3": "NZL", "iso_numeric": "554"},
    "turkey":              {"name": "Türkiye", "iso_alpha_2": "TR", "iso_alpha_3": "TUR", "iso_numeric": "792"},
    "saudi arabia":        {"name": "Saudi Arabia", "iso_alpha_2": "SA", "iso_alpha_3": "SAU", "iso_numeric": "682"},
    "united arab emirates":{"name": "United Arab Emirates", "iso_alpha_2": "AE", "iso_alpha_3": "ARE", "iso_numeric": "784"},
    "south africa":        {"name": "South Africa", "iso_alpha_2": "ZA", "iso_alpha_3": "ZAF", "iso_numeric": "710"},
    "nigeria":             {"name": "Nigeria", "iso_alpha_2": "NG", "iso_alpha_3": "NGA", "iso_numeric": "566"},
    "egypt":               {"name": "Egypt", "iso_alpha_2": "EG", "iso_alpha_3": "EGY", "iso_numeric": "818"},
    # Add more as you need; this list is a safe fallback when pycountry isn't available.
}

def _norm(text: str) -> str:
    t = re.sub(r"[\u200b\s]+", " ", (text or "")).strip().lower()
    # unify punctuation/accents lite: remove dots and apostrophes
    t = t.replace(".", "").replace("’", "'")
    return t

def get_country_codes(country: str) -> Dict[str, Optional[str]]:
    """
    Return a dict with: name, iso_alpha_2, iso_alpha_3, iso_numeric (as strings)
    Never raises; returns None values on failure.
    """
    if not country:
        return {"name": None, "iso_alpha_2": None, "iso_alpha_3": None, "iso_numeric": None}

    key = _norm(country)

    # 1) builtin quick map
    if key in _BUILTIN:
        row = _BUILTIN[key]
        return {
            "name": row["name"],
            "iso_alpha_2": row["iso_alpha_2"],
            "iso_alpha_3": row["iso_alpha_3"],
            "iso_numeric": row["iso_numeric"],
        }

    # 2) pycountry lookup (works for most names, codes, and common aliases)
    if pycountry:
        try:
            m = pycountry.countries.lookup(country)
            return {
                "name": getattr(m, "name", country),
                "iso_alpha_2": getattr(m, "alpha_2", None),
                "iso_alpha_3": getattr(m, "alpha_3", None),
                "iso_numeric": getattr(m, "numeric", None),
            }
        except Exception:
            pass

    # 3) heuristic: if they passed a code directly
    if len(country) == 2 and country.isalpha():
        return {"name": country.upper(), "iso_alpha_2": country.upper(), "iso_alpha_3": None, "iso_numeric": None}
    if len(country) == 3 and country.isalpha():
        return {"name": country.upper(), "iso_alpha_2": None, "iso_alpha_3": country.upper(), "iso_numeric": None}

    # 4) failure path
    return {"name": country, "iso_alpha_2": None, "iso_alpha_3": None, "iso_numeric": None}
