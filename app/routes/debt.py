# app/routes/debt.py
from __future__ import annotations

from typing import Any, Dict
from fastapi import APIRouter, Query, Response

# Back-compat: we expose both names; compute_debt_payload is a wrapper in debt_service.py
from app.services.debt_service import compute_debt_payload, debt_payload_for_country

router = APIRouter(tags=["debt"])

@router.get(
    "/v1/debt",
    summary="Latest general government debt-to-GDP (tiered sources)",
    response_description="Latest value + full annual series with source selection details.",
)
def get_debt(
    country: str = Query(
        ...,
        min_length=2,
        description="Full country name (e.g., 'Germany', 'Nigeria', 'United States').",
    )
) -> Dict[str, Any]:
    return compute_debt_payload(country.strip())

@router.head("/v1/debt", summary="HEAD for /v1/debt (warms cache)")
def head_debt(country: str = Query(..., min_length=2)) -> Response:
    _ = debt_payload_for_country(country.strip())
    return Response(status_code=200)
