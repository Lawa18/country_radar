# app/routes/probe.py — TINY, startup-safe
from __future__ import annotations

from typing import Any, Dict, Mapping, Optional
import inspect

from fastapi import APIRouter, Query
from fastapi.responses import Response

router = APIRouter(tags=["probe"])


def _safe_import(module: str):
    try:
        return __import__(module, fromlist=["*"])
    except Exception:
        return None


@router.get("/__action_probe", summary="Connectivity probe")
def action_probe_get() -> Dict[str, Any]:
    # must be instant
    return {"ok": True, "path": "/__action_probe"}


@router.options("/__action_probe", include_in_schema=False)
def action_probe_options() -> Response:
    return Response(status_code=204)


@router.get("/__codes", summary="Show resolved ISO codes for a country")
def show_codes(country: str = "Mexico"):
    # keep this defensive
    cc_mod = _safe_import("app.utils.country_codes")
    codes: Optional[Mapping[str, Any]] = None
    if cc_mod and hasattr(cc_mod, "get_country_codes"):
        try:
            codes = cc_mod.get_country_codes(country) or None
        except Exception:
            codes = None
    return {"country": country, "codes": codes}


@router.get("/__provider_fns", summary="List callables exported by a provider module")
def provider_fns(module: str):
    mod = _safe_import(module)
    if not mod:
        return {"ok": False, "module": module, "error": "import_failed"}
    fns = []
    for name, obj in vars(mod).items():
        if name.startswith("_"):
            continue
        if callable(obj):
            try:
                sig = str(inspect.signature(obj))
            except Exception:
                sig = "(?)"
            fns.append({"name": name, "signature": sig})
    return {"ok": True, "module": module, "count": len(fns), "functions": sorted(fns, key=lambda x: x["name"])}
