# app/routes/country_lite.py
from __future__ import annotations
from copy import deepcopy
from fastapi import APIRouter, Query, HTTPException
from app.services.indicator_service import build_country_payload

router = APIRouter()

@router.get("/v1/country-lite", summary="Latest-only macro bundle (tiny response)")
def country_lite(country: str = Query(..., min_length=2, description="Full country name, e.g., Mexico")):
    name = (country or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Country must be provided.")
    payload = build_country_payload(name)
    if isinstance(payload, dict) and payload.get("error"):
        raise HTTPException(status_code=400, detail=str(payload["error"]))

    # Strip series without mutating cached objects
    resp = deepcopy(payload)
    if isinstance(resp, dict):
        inds = resp.get("indicators") or {}
        for block in inds.values():
            if isinstance(block, dict):
                block["series"] = {}
        debt = resp.get("debt")
        if isinstance(debt, dict):
            debt["series"] = {}
    return resp
