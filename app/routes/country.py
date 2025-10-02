# app/routes/country.py
from __future__ import annotations

from typing import Any, Dict, Literal, Optional
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

router = APIRouter()


def _choose_indicator_builder() -> tuple[Optional[callable], Optional[str], Optional[str]]:
    """
    Load app.services.indicator_service and pick the best available builder.
    Returns (fn, chosen_name, svc_file) or (None, None, svc_file_if_loaded).
    """
    try:
        from app.services import indicator_service as _svc  # type: ignore
    except Exception:
        return (None, None, None)

    candidates = (
        # Strongly preferred modern builders (add your actual modern name if different)
        "build_country_payload_v2",
        "assemble_country_payload",
        "assemble_country_data_v2",
        "build_country_data_v2",
        # Other plausible alternates
        "get_country_data_v2",
        "country_data_v2",
        "get_country_bundle",
        "build_country_bundle",
        # Legacy names (your current export is build_country_payload)
        "build_country_payload",
        "build_country_data",
        "assemble_country_data",
        "get_country_data",
        "make_country_data",
    )

    for name in candidates:
        fn = getattr(_svc, name, None)
        if callable(fn):
            return (fn, name, getattr(_svc, "__file__", None))
    return (None, None, getattr(_svc, "__file__", None))


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

    # Debug: which builder/file executed (safe to keep)
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


# --- Optional: keep the old behavior on a separate path ----------------------
# This preserves the simple "legacy" call that always uses build_country_payload(country)
try:
    from app.services.indicator_service import build_country_payload as _legacy_build  # type: ignore
except Exception:
    _legacy_build = None

@router.get("/country-data-legacy", tags=["country"], summary="Country Data (legacy)")
def country_data_legacy(
    country: str = Query(..., description="Full country name, e.g., Germany")
):
    """
    Legacy behavior: directly calls build_country_payload(country).
    Useful for debugging or parity checks with the old implementation.
    """
    if _legacy_build is None:
        return JSONResponse(
            {"ok": False, "error": "legacy build_country_payload is unavailable"},
            status_code=500,
        )
    try:
        payload = _legacy_build(country)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    if not isinstance(payload, dict):
        payload = {"result": payload}
    payload.setdefault("country", country)
    return payload
# ---------------------------------------------------------------------------
