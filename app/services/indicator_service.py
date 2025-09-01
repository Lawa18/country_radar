cat > app/services/indicator_service.py <<'PY'
from typing import Dict, Any

from app.utils.country_codes import resolve_country_codes
from app.services.debt_service import compute_debt_payload

def _blank_gov_debt() -> Dict[str, Any]:
    return {
        "value": None, "date": None, "source": None,
        "government_type": None, "currency": None, "currency_code": None,
    }

def _blank_nom_gdp() -> Dict[str, Any]:
    return {"value": None, "date": None, "source": None, "currency": None, "currency_code": None}

def _blank_debt_pct() -> Dict[str, Any]:
    return {"value": None, "date": None, "source": None, "government_type": None}

def build_country_payload(country: str) -> Dict[str, Any]:
    """
    Minimal, robust implementation:
    - Validates country
    - Pulls debt trio via compute_debt_payload
    - Returns the stable schema your UI/GPT expects
    """
    codes = resolve_country_codes(country)
    if not codes:
        return {"error": "Invalid country name", "country": country}

    # Reuse debt service (already implements Eurostat -> IMF -> WB -> computed)
    debt_bundle = compute_debt_payload(country) or {}

    gov_latest = debt_bundle.get("government_debt") or _blank_gov_debt()
    gdp_latest = debt_bundle.get("nominal_gdp") or _blank_nom_gdp()
    ratio_latest = debt_bundle.get("debt_to_gdp") or _blank_debt_pct()
    ratio_series = debt_bundle.get("debt_to_gdp_series") or {}

    return {
        "country": country,
        "iso_codes": codes,
        "imf_data": {},  # we'll fill these later; schema stays stable
        "government_debt": {"latest": gov_latest, "series": {}},
        "nominal_gdp":     {"latest": gdp_latest, "series": {}},
        "debt_to_gdp":     {"latest": ratio_latest, "series": ratio_series},
        "additional_indicators": {},
    }
PY

