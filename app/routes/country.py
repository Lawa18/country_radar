# app/routes/country.py — robust /country-data with builder v2 preference + safe fallbacks
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, Callable, Mapping, Literal
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
import inspect

router = APIRouter()


# ----------------------------- utilities -------------------------------------

def _safe_import(path: str):
    try:
        return __import__(path, fromlist=["*"])
    except Exception:
        return None

def _introspect_builder(fn: Callable[..., Any]) -> Dict[str, Any]:
    try:
        mod = inspect.getmodule(fn)
        sig = str(inspect.signature(fn))
        return {
            "name": getattr(fn, "__name__", None),
            "module": getattr(mod, "__name__", None) if mod else None,
            "file": getattr(mod, "__file__", None) if mod else None,
            "signature": sig,
        }
    except Exception:
        return {"name": getattr(fn, "__name__", None)}

def _resolve_country_builder() -> Tuple[Optional[Callable[..., Any]], str]:
    """
    Try to resolve a modern builder first; then older ones.
    Returns (callable, mode) where mode is 'v2' | 'v1' | 'legacy' | 'none'
    """
    ind = _safe_import("app.services.indicator_service")
    if not ind:
        return None, "none"

    # Preferred modern interface
    for name, mode in (
        ("build_country_payload_v2", "v2"),
        ("build_country_payload", "v1"),  # may accept (country, series, keep) in your latest code
    ):
        fn = getattr(ind, name, None)
        if callable(fn):
            return fn, mode

    return None, "none"

def _maybe_merge_debt(payload: Dict[str, Any], country: str, debug: bool) -> None:
    """
    Enrich payload with debt blocks if they are missing or empty.
    Tries debt_bundle first, then legacy debt route module (both expose compute_debt_payload in our rebuild).
    Mutates payload in place; adds _debug.debt if debug=true.
    """
    need_debt = False
    for k in ("government_debt", "nominal_gdp", "debt_to_gdp"):
        if k not in payload or not isinstance(payload.get(k), Mapping) or not payload[k].get("latest"):
            need_debt = True
            break
    if not need_debt:
        return

    dbg: Dict[str, Any] = {}
    debt_mod = _safe_import("app.routes.debt_bundle") or _safe_import("app.routes.debt")
    fn = getattr(debt_mod, "compute_debt_payload", None) if debt_mod else None
    if callable(fn):
        try:
            debt = fn(country=country)
            if isinstance(debt, Mapping):
                payload.setdefault("government_debt", debt.get("government_debt", {}))
                payload.setdefault("nominal_gdp", debt.get("nominal_gdp", {}))
                payload.setdefault("debt_to_gdp", debt.get("debt_to_gdp", {}))
                payload.setdefault("debt_to_gdp_series", debt.get("debt_to_gdp_series", {}))
            if debug:
                dbg["used"] = True
                dbg["source_module"] = getattr(debt_mod, "__name__", None)
        except Exception as e:
            if debug:
                dbg["error"] = str(e)
    else:
        if debug:
            dbg["used"] = False
            dbg["reason"] = "compute_debt_payload not found"
    if debug:
        payload.setdefault("_debug", {})
        payload["_debug"]["debt"] = dbg


# -------------------------------- route --------------------------------------

@router.get("/country-data", tags=["country"], summary="Country Data")
def country_data(
    country: str = Query(..., description="Full country name, e.g., Sweden"),
    series: Literal["none", "mini", "full"] = Query(
        "mini", description='Timeseries size (none = latest only, "mini" ~ 5y)'
    ),
    keep: int = Query(
        60, ge=0, le=20000, description="Trim timeseries length (points to keep)"
    ),
    debug: bool = Query(False, description="Include builder/debug metadata under _debug"),
) -> Dict[str, Any]:
    """
    Full macro bundle. Prefers a modern monthly-first builder in indicator_service,
    falls back gracefully to older interfaces. If debt blocks are missing, enriches
    with compute_debt_payload from the debt router (if available).

    Returns a JSON object suited for the Country Radar client.
    """
    out: Dict[str, Any] = {}
    debug_block: Dict[str, Any] = {"builder": {}, "notes": []} if debug else {}

    builder, mode = _resolve_country_builder()
    if builder is None:
        # Hard fallback: return a minimal skeleton so client doesn't fail
        out = {
            "country": country,
            "iso_codes": {"name": country, "iso_alpha_2": None, "iso_alpha_3": None, "iso_numeric": None},
            "latest": {"year": None, "value": None, "source": None},
            "series": {},
            "source": None,
            "government_debt": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
            "nominal_gdp": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
            "debt_to_gdp": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
            "debt_to_gdp_series": {},
        }
        if debug:
            debug_block["notes"].append("indicator_service not found; returned minimal skeleton")
            out["_debug"] = debug_block
        return JSONResponse(content=out)

    # Call the builder with the best signature available
    try:
        if mode == "v2":
            out = builder(country=country, series=series, keep=keep)  # type: ignore[call-arg]
        else:
            # Attempt (country, series, keep) first; then legacy (country)
            try:
                out = builder(country=country, series=series, keep=keep)  # type: ignore[call-arg]
            except TypeError:
                out = builder(country)  # type: ignore[misc]
    except Exception as e:
        # Safe failure response
        out = {
            "country": country,
            "iso_codes": {"name": country, "iso_alpha_2": None, "iso_alpha_3": None, "iso_numeric": None},
            "latest": {"year": None, "value": None, "source": None},
            "series": {},
            "source": None,
            "government_debt": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
            "nominal_gdp": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
            "debt_to_gdp": {"latest": {"value": None, "date": None, "source": None}, "series": {}},
            "debt_to_gdp_series": {},
            "_error": str(e),
        }
        if debug:
            debug_block["notes"].append(f"builder raised: {e!s}")

    # Attach builder introspection (only when debug=true)
    if debug and callable(builder):
        debug_block["builder"] = _introspect_builder(builder)
        debug_block["builder"]["mode"] = mode

    # Enrich with debt if missing
    _maybe_merge_debt(out, country=country, debug=debug)

    # Ensure a stable shape
    out.setdefault("country", country)
    out.setdefault("debt_to_gdp_series", out.get("debt_to_gdp_series") or {})

    if debug:
        out.setdefault("_debug", {})
        out["_debug"].update(debug_block)

    return JSONResponse(content=out)
