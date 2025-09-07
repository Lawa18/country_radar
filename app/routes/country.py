# app/routes/country.py
from __future__ import annotations

from typing import Any, Dict
from fastapi import APIRouter, Query, Response

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
        description="Full country name (e.g., 'Germany', 'United States', 'Sweden'). "
                    "Common aliases are accepted (e.g., 'USA').",
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
    - On unknown country names, the payload includes {"error": "Invalid country name"}.
    """
    return build_country_payload(country.strip())

@router.head(
    "/country-data",
    summary="HEAD for /country-data (warms cache, no body)",
)
def country_data_head(
    country: str = Query(
        ...,
        min_length=2,
        description="Same as GET /country-data; used to pre-warm cache without a response body.",
    )
) -> Response:
    # Build once to warm in-process cache; return empty body as per HEAD semantics.
    _ = build_country_payload(country.strip())
    return Response(status_code=200)
