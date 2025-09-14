# app/routes/country.py
from __future__ import annotations

from typing import Any, Dict, Literal
from fastapi import APIRouter, HTTPException, Query
import inspect

# Import the module so we can select among multiple entrypoints.
from app.services import indicator_service as svc

router = APIRouter()


def _call_service_with_supported_kwargs(func, **kwargs) -> Any:
    """
    Call `func` with only the kwargs it actually accepts.
    This prevents 'unexpected keyword argument' errors when older
    service functions don't take (series, keep).
    """
    try:
        sig = inspect.signature(func)
        accepted = set(sig.parameters.keys())
        filtered = {k: v for k, v in kwargs.items() if k in accepted}
        # If 'country' isn't in accepted but the function takes a single positional,
        # fall back to positional call with the country value.
        if "country" not in accepted and len(sig.parameters) == 1:
            only_param = next(iter(sig.parameters.values()))
            if only_param.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
                return func(kwargs.get("country"))
        return func(**filtered)
    except TypeError as e:
        # Re-raise as HTTPException to show a clean error to the client.
        raise HTTPException(status_code=500, detail=f"indicator_service call error: {e}")


def _assemble_country_payload(country: str, series: str, keep: int) -> Dict[str, Any]:
    """
    Try multiple indicator_service entrypoints and call them safely
    with only the kwargs they accept.
    """
    candidates = (
        "get_country_data",          # preferred new name
        "assemble_country_data",     # older
        "build_country_payload",     # legacy, often only (country)
    )
    for name in candidates:
        func = getattr(svc, name, None)
        if callable(func):
            result = _call_service_with_supported_kwargs(
                func,
                country=country,
                series=series,
                keep=keep,
            )
            if isinstance(result, dict):
                return result
            # If the service returns non-dict, wrap a minimal object for Actions validators
            return {"country": country, "series_mode": series, "data": result}
    raise HTTPException(
        status_code=500,
        detail="No compatible country assembly function found in indicator_service.",
    )


@router.get("/country-data")
def get_country_data(
    country: str = Query(..., description="Full country name, e.g., Sweden"),
    series: Literal["none", "mini", "full"] = Query(
        "mini", description="Timeseries size (none=latest only)"
    ),
    keep: int = Query(
        60, ge=0, le=20000, description="Trim timeseries length (points to keep)"
    ),
) -> Dict[str, Any]:
    """
    Full macro bundle. This route passes 'series' and 'keep' when supported by the
    underlying indicator_service function; otherwise it only passes 'country'.
    """
    try:
        payload = _assemble_country_payload(country=country, series=series, keep=keep)
        if not isinstance(payload, dict):
            raise ValueError("indicator_service returned a non-dict payload")
        payload.setdefault("country", country)
        payload.setdefault("series_mode", series)
        return payload
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"country-data error: {e}")
