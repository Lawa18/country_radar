from fastapi import APIRouter, Query
from app.services.debt_service import compute_debt_payload

router = APIRouter()

@router.get("/v1/debt")
def v1_debt(country: str = Query(..., description="Full country name, e.g., Germany")):
    return compute_debt_payload(country)

