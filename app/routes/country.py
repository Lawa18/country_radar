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

from typing import Dict, Any, Literal
from fastapi import HTTPException, Query

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
    Full macro bundle. Prefer a modern monthly-first builder in indicator_service.
    Pass `series` and `keep` when supported; fall back to legacy (country) signature.
    """
    # Lazy import to avoid circulars/stale imports
    try:
        from app.services import indicator_service as _svc  # type: ignore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"country-data import error: {e}")

    # Try modern names first, then older ones; first callable wins.
    CANDIDATES = (
        # strongly preferred modern builders (add your actual name here when you have it)
        "build_country_payload_v2",
        "assemble_country_payload",
        "assemble_country_data_v2",
        "build_country_data_v2",
        # plausible alternates
        "get_country_data_v2",
        "country_data_v2",
        "get_country_bundle",
        "build_country_bundle",
        # legacy last (this is your current export)
        "build_country_payload",
        "build_country_data",
        "assemble_country_data",
        "get_country_data",
        "make_country_data",
    )

    fn = None
    chosen = None
    for name in CANDIDATES:
        cand = getattr(_svc, name, None)
        if callable(cand):
            fn, chosen = cand, name
            break

    if fn is None:
        raise HTTPException(
            status_code=500,
            detail="No suitable country-data builder found on indicator_service. "
                   "Expected one of: " + ", ".join(CANDIDATES),
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

    # Diagnostics: show which builder/file executed
    try:
        import inspect
        dbg = payload.setdefault("_debug", {})
        dbg["builder"] = {
            "name": chosen,
            "indicator_service_file": getattr(_svc, "__file__", None),
            "builder_file": inspect.getsourcefile(fn),
            "signature": str(inspect.signature(fn)),
        }
    except Exception:
        pass

    payload.setdefault("country", country)
    payload.setdefault("series_mode", series)
    return payload

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
