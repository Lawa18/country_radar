# app/routes/country.py
from __future__ import annotations

from typing import Any, Dict
from fastapi import APIRouter, Query, Response, HTTPException

from app.services.indicator_service import build_country_payload

router = APIRouter(tags=["country"])

@router.get(
    "/country-data",
    summary="Country macro indicators bundle",
    response_description="Compact bundle of macro indicators for a given country.",
)
def country_data(
    country: str = Query(
        ...,
        min_length=2,
        description=(
            "Full country name (e.g., 'Germany', 'United States', 'Sweden'). "
            "Common aliases are accepted (e.g., 'USA')."
        ),
        examples={
            "germany": {"summary": "Germany", "value": "Germany"},
            "usa": {"summary": "United States (alias accepted)", "value": "USA"},
            "sweden": {"summary": "Sweden", "value": "Sweden"},
        },
    )
) -> Dict[str, Any]:
    """
    Returns a structured payload with CPI (YoY), unemployment, FX vs USD,
    reserves (USD), policy rate, GDP growth, CAB %GDP, government effectiveness,
    and the debt-to-GDP block (from the debt service).
    - Monthly sources (ECB/IMF/Eurostat) are preferred; WB is used as fallback where applicable.
    - On unknown country names, a 400 is returned with a helpful message.
    """
    name = country.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Country must be provided.")

    payload = build_country_payload(name)

    # If the service signals a bad country, convert to 400 for the client.
    if isinstance(payload, dict) and payload.get("error"):
        raise HTTPException(status_code=400, detail=str(payload.get("error")))

    return payload


# Keep HEAD for cache warm, but hide it from OpenAPI to avoid connector confusion
@router.head(
    "/country-data",
    include_in_schema=False,   # important: do not expose HEAD in the spec
    summary="(hidden) HEAD for /country-data",
)
def country_data_head(
    country: str = Query(
        ...,
        min_length=2,
        description="Same as GET /country-data; used to pre-warm cache without a response body.",
    )
) -> Response:
    # Build once to warm in-process cache; return empty body per HEAD semantics.
    _ = build_country_payload(country.strip())
    return Response(status_code=200)
