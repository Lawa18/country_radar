from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Tuple, Literal
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

router = APIRouter()

# ---------------------------------------------------------------------------
# Helper: pick the best available builder on indicator_service
# Priority:
#   1) build_country_payload_v2(country, series, keep)  <-- modern, monthly-first
#   2) assemble_country_payload / build_country_data / assemble_country_data / get_country_data / make_country_data
#   3) build_country_payload(country)                   <-- legacy fallback (returns placeholders)
# ---------------------------------------------------------------------------
def _choose_indicator_builder() -> Tuple[Optional[Callable[..., Any]], str, Optional[str]]:
    try:
        from app.services import indicator_service as svc
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"indicator_service import failed: {e}")

    import inspect
    svc_file = getattr(svc, "__file__", None)

    candidates: Tuple[str, ...] = (
        # modern / monthly-first (expects series & keep)
        "build_country_payload_v2",
        # other plausible builders you may have used earlier
        "assemble_country_payload",
        "build_country_data",
        "assemble_country_data",
        "get_country_data",
        "make_country_data",
        # legacy fallback (country-only; returns placeholders)
        "build_country_payload",
    )

    for name in candidates:
        fn = getattr(svc, name, None)
        if callable(fn):
            return fn, name, svc_file

    return None, "", svc_file


@router.get("/country-data", tags=["country"], summary="Country Data")
def country_data(
    country: str = Query(..., description="Full country name, e.g., Sweden"),
    series: Literal["none", "mini", "full"] = Query(
        "mini", description='Timeseries size (none = latest only, "mini" ~ 5y)'
    ),
    keep: int = Query(
        60, ge=0, le=20000, description="Trim timeseries length (points to keep)"
    ),
) -> Dict[str, Any]:
    """
    Full macro bundle. Prefers a modern monthly-first builder in indicator_service,
    passing `series` and `keep` when supported; falls back to legacy (country-only) signature.
    """
    fn, chosen, svc_file = _choose_indicator_builder()
    if fn is None:
        raise HTTPException(
            status_code=500,
            detail=(
                "No suitable country-data builder found on indicator_service. "
                "Export a modern builder (e.g., build_country_payload_v2(country, series, keep)) "
                "or keep build_country_payload(country) available as a fallback."
            ),
        )

    # Call with modern kwargs if possible; otherwise fall back to (country).
    try:
        try:
            payload = fn(country=country, series=series, keep=keep)  # type: ignore[misc]
        except TypeError:
            payload = fn(country)  # type: ignore[misc]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{chosen} failed: {e}")

    if not isinstance(payload, dict):
        payload = {"result": payload}

    # Debug: which builder/file executed (harmless to keep; helps during deploy)
    try:
        import inspect
        payload.setdefault("_debug", {}).setdefault("builder", {})
        payload["_debug"]["builder"].update(
            {
                "name": chosen,
                "indicator_service_file": svc_file,
                "builder_file": inspect.getsourcefile(fn),
                "signature": str(inspect.signature(fn)),
            }
        )
    except Exception:
        pass

    payload.setdefault("country", country)
    payload.setdefault("series_mode", series)
    return payload
