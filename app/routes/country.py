from fastapi import APIRouter, Query
from app.services.indicator_service import build_country_payload

router = APIRouter()

@router.get("/country-data")
def country_data(country: str = Query(..., description="Full country name, e.g., Germany")):
    return build_country_payload(country)
