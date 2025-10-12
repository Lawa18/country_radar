# app/routes/country.py — robust /country-data (prefers v2 builder; series/keep; _debug)
from __future__ import annotations

from typing import Any, Dict, Literal

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

router = APIRouter()


def _flex_call_builder(builder, country: str, series: str, keep: int):
    """
    Call builder with flexible signatures to support legacy functions.
    Tries (country=..., series=..., keep=...) then (country=...), then (country,).
    """
    try:
        return builder(country=country, series=series, keep=keep)
    except TypeError:
        try:
            return builder(country=country)
        except TypeError:
            return builder(country)


@router.get(
    "/country-data",
    tags=["country"],
    summary="Country Data",
    description="Returns the assembled country payload with indicators and debt.",
)
def country_data(
    country: str = Query(..., description="Full country name, e.g., Sweden"),
    series: Literal["none", "mini", "full"] = Query(
        "mini", description='Timeseries size (none = latest only, "mini" ~ 5y, "full" = full history)'
    ),
    keep: int = Query(
        180, ge=1, le=3650, description="Keep N days of history (approx by freq)"
    ),
    debug: bool = Query(False, description="Include _debug info about builder + sources."),
) -> Dict[str, Any]:
    """
    Return the assembled country payload.

    Prefers modern builder `build_country_payload_v2`. Falls back to
    legacy `build_country_payload` if v2 is not available.
    """
    # Lazy import so import-time errors elsewhere don't break app startup
    try:
        from app.services import indicator_service as svc  # type: ignore
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "error": f"indicator_service import failed: {e}",
            },
        )

    builder = getattr(svc, "build_country_payload_v2", None) or getattr(
        svc, "build_country_payload", None
    )

    if not callable(builder):
        return {
            "ok": False,
            "error": "No builder found in indicator_service (expected build_country_payload_v2 or build_country_payload).",
            "_debug": {"module": getattr(svc, "__file__", None)},
        }

    try:
        payload = _flex_call_builder(builder, country=country, series=series, keep=keep)
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "_debug": {
                "builder": getattr(builder, "__name__", None),
                "module": getattr(builder, "__module__", None),
            },
        }

    # Normalize to dict
    if not isinstance(payload, dict):
        payload = {"result": payload}

    # If builder didn't include debt blocks, enrich using existing helper(s)
    if not any(k in payload for k in ("government_debt", "debt_to_gdp", "nominal_gdp")):
        debt_payload = None
        try:
            # Preferred: reuse the existing route helper if present
            from app.routes.debt import compute_debt_payload  # type: ignore
            debt_payload = compute_debt_payload(country=country)
        except Exception:
            try:
                # Fallback: service-level helper, if available
                from app.services.indicator_service import compute_debt_payload  # type: ignore
                debt_payload = compute_debt_payload(country=country)
            except Exception:
                debt_payload = None
        if isinstance(debt_payload, dict):
            for key in ("government_debt", "nominal_gdp", "debt_to_gdp", "debt_to_gdp_series"):
                if key in debt_payload:
                    payload[key] = debt_payload[key]

    if debug:
        dbg = payload.setdefault("_debug", {})
        try:
            mod = __import__(getattr(builder, "__module__", ""), fromlist=["*"])  # type: ignore
            file_path = getattr(mod, "__file__", None)
        except Exception:
            file_path = None
        dbg.setdefault(
            "builder",
            {
                "used": getattr(builder, "__name__", None),
                "module": getattr(builder, "__module__", None),
                "file": file_path,
                "series_arg": series,
                "keep_arg": keep,
            },
        )

    # Friendly top-level fields
    payload.setdefault("country", country)
    payload.setdefault("series_mode", series)
    payload.setdefault("keep_days", keep)
    return payload
