from fastapi import APIRouter, Query

router = APIRouter()

@router.get("/country-data")
def get_country_data(country: str = Query(..., description="Country name, e.g., Germany")):
    # placeholder response
    return {"country": country, "data": "Country indicators here"}
