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


# --- Country Radar: added probe + lite endpoints (append-only) ---
from typing import Any, Callable, Dict, Optional
from fastapi import Query
from fastapi.responses import JSONResponse

# Probe endpoint for builder/API connectivity
@router.get("/__action_probe")
def __action_probe() -> Dict[str, Any]:
    return {"ok": True, "path": "/__action_probe"}

# --- Legacy shim: keep old behavior available without overriding the main route ---
# NOTE: Do NOT re-declare `router` here; we reuse the existing router from above.
# If you still need the legacy, direct call, expose it on a different path.
try:
    from app.services.indicator_service import build_country_payload as _legacy_build_country_payload
except Exception:
    _legacy_build_country_payload = None  # optional; legacy handler below will guard

@router.get("/country-data-legacy")
def country_data_legacy(
    country: str = Query(..., description="Full country name, e.g., Germany")
):
    """
    Legacy endpoint that calls the old build_country_payload(country) signature directly.
    Kept only to avoid breaking old references. Prefer /country-data with series/keep.
    """
    if _legacy_build_country_payload is None:
        return JSONResponse(
            {"ok": False, "error": "legacy build_country_payload is unavailable"},
            status_code=500,
        )
    try:
        payload = _legacy_build_country_payload(country)  # old signature
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
    if not isinstance(payload, dict):
        payload = {"result": payload}
    payload.setdefault("country", country)
    return payload

# ---------------------- append-only below ----------------------
from typing import Any
from fastapi.responses import JSONResponse

# Simple probe so Actions/curl can confirm wiring
@router.get("/__action_probe")
def __action_probe() -> dict[str, Any]:
    return {"ok": True, "path": "/__action_probe"}

# Latest-only compact bundle for GPT reliability
@router.get("/v1/country-lite")
def country_lite(country: str = Query(..., description="Full country name, e.g., Germany")):
    """
    Tries a lite builder if present; otherwise falls back to the existing
    full builder `build_country_payload(country)` and returns that.
    """
    try:
        from app.services import indicator_service as _svc
        # Prefer lite-style builders if available (safe if missing)
        for name in (
            "get_country_lite",
            "country_lite",
            "assemble_country_lite",
            "build_country_lite",
            "get_country_compact",
            "country_compact",
        ):
            f = getattr(_svc, name, None)
            if callable(f):
                try:
                    # some lite builders accept series="none"; others only (country)
                    try:
                        payload = f(country=country, series="none")
                    except TypeError:
                        payload = f(country)
                except Exception as e:
                    return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
                break
        else:
            # Fallback to your known full builder
            payload = build_country_payload(country)
    except Exception:
        # Final fallback: call full builder directly
        try:
            payload = build_country_payload(country)
        except Exception as e2:
            return JSONResponse({"ok": False, "error": f"{e2}"}, status_code=500)

    if not isinstance(payload, dict):
        payload = {"result": payload}
    payload.setdefault("country", country)
    return JSONResponse(payload)
# -------------------- end append-only block --------------------
