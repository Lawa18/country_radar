from fastapi import APIRouter, Query

router = APIRouter()

@router.get("/v1/debt")
def get_debt(country: str = Query(..., description="Country name, e.g., Germany")):
    # placeholder response
    return {"country": country, "debt_to_gdp": "Placeholder ratio"}
