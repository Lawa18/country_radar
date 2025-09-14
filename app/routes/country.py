# app/routes/country.py
from __future__ import annotations

from typing import Any, Dict, Literal

from fastapi import APIRouter, HTTPException, Query

# Import the module (not specific names) so we can probe multiple function names safely.
from app.services import indicator_service as svc

router = APIRouter()


def _assemble_country_payload(country: str, series: str, keep: int) -> Dict[str, Any]:
    """
    Call into indicator_service using whichever entrypoint exists in your codebase.
    This preserves compatibility with older/alternate function names.
    """
    candidates = (
        "get_country_data",          # preferred
        "assemble_country_data",     # older naming
        "build_country_payload",     # legacy
    )
    for fn in candidates:
        if hasattr(svc, fn):
            return getattr(svc, fn)(country=country, series=series, keep=keep)
    raise HTTPException(
        status_code=500,
        detail="No compatible country assembly function found in indicator_service.",
    )


@router.get("/country-data")
def get_country_data(
    country: str = Query(..., description="Full country name, e.g., Sweden"),
    series: Literal["none", "mini", "full"] = Query(
        "mini", description="Timeseries payload size (none=latest only)"
    ),
    keep: int = Query(
        60,
        ge=0,
        le=20000,
        description="Trim timeseries length (number of points to keep)",
    ),
) -> Dict[str, Any]:
    """
    Full macro bundle for a given country.

    - Forwards `series` and `keep` directly to indicator_service.
    - indicator_service is responsible for:
        * monthly-first merge and fallbacks
        * debt tiering (not overwritten later)
        * series trimming logic
    - Returns a plain JSON object to satisfy strict Actions validators.
    """
    try:
        payload = _assemble_country_payload(country=country, series=series, keep=keep)

        # Be strict for Actions validators: ensure a dict is returned.
        if not isinstance(payload, dict):
            raise ValueError("indicator_service returned a non-dict payload")

        # Guarantee minimally valid JSON object (optional belt-and-suspenders).
        if "country" not in payload:
            payload.setdefault("country", country)
        if "series_mode" not in payload:
            payload.setdefault("series_mode", series)

        return payload

    except HTTPException:
        # Pass through explicit HTTPExceptions
        raise
    except Exception as e:
        # Hide internals but give actionable message
        raise HTTPException(status_code=500, detail=f"country-data error: {e}")
