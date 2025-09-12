# app/routes/debt.py
from typing import Any, Dict
from fastapi import APIRouter, Query, HTTPException

router = APIRouter()

@router.get("/v1/debt")
def debt_latest(country: str = Query(..., description="Full country name, e.g., Germany")) -> Dict[str, Any]:
    """
    Returns latest debt-to-GDP and series for the given country.
    We import the service lazily so the app can still boot if imports fail.
    """
    try:
        from app.services import debt_service as ds  # lazy import
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to import app.services.debt_service: {e}")

    # Try a few likely function names for compatibility
    for name in ("compute_debt_payload", "build_debt_payload", "get_debt_payload", "debt_payload_for_country"):
        fn = getattr(ds, name, None)
        if callable(fn):
            try:
                return fn(country)
            except Exception as e:
                raise HTTPException(status_code=502, detail=f"debt_service.{name} error: {e}")

    raise HTTPException(
        status_code=500,
        detail="No supported debt payload function found in app.services.debt_service "
               "(expected one of: compute_debt_payload, build_debt_payload, get_debt_payload, debt_payload_for_country)."
    )
