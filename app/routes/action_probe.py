# app/routes/action_probe.py
from fastapi import APIRouter, Request

router = APIRouter(tags=["country"])  # tag doesn't matter

@router.get("/__action_probe", summary="Tiny endpoint for connector reachability")
def action_probe(request: Request):
    # Return tiny, deterministic JSON and echo a couple of request details
    return {
        "ok": True,
        "path": str(request.url.path),
        "query": str(request.url.query),
        "ua": request.headers.get("user-agent", ""),
    }
