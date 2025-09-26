# app/main.py
from __future__ import annotations

import os
import time
from typing import Any, Dict, Set

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from starlette.middleware.gzip import GZipMiddleware

# Routers (only the ones we truly use)
from app.routes import country, debt
from app.routes import probe as probe_routes  # must export `router`

APP_TITLE = "Country Radar API"
APP_VERSION = "1.0.4"

app = FastAPI(title=APP_TITLE, version=APP_VERSION, openapi_url="/openapi.json")

# --- Middleware ---

# CORS (permissive during development; tighten for prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# GZip to shrink JSON responses
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Quiet health-check noise; add light tracing for GPT endpoints
@app.middleware("http")
async def log_requests(request: Request, call_next):
    path = request.url.path
    ua = request.headers.get("user-agent", "")
    skip = path == "/ping" or ua.startswith("Render/")
    start = time.time()
    resp: Response = await call_next(request)

    if path.startswith("/__action_probe") or path.startswith("/v1/country-lite") or path.startswith("/country-data"):
        try:
            ip = request.client.host if request.client else "-"
        except Exception:
            ip = "-"
        print(f"[trace] {request.method} {path}?{request.query_params} ua={ua} ip={ip} -> {resp.status_code}")

    if not skip:
        dur_ms = (time.time() - start) * 1000.0
        print(f"[req] {request.method} {path}?{request.query_params} ua={ua} -> {resp.status_code} {dur_ms:.1f}ms")
    return resp

# --- Routes ---

# Mount the routers you actually use
app.include_router(country.router, tags=["country"])
app.include_router(debt.router, tags=["debt"])
app.include_router(probe_routes.router, tags=["probe"])

@app.get("/ping")
def ping():
    return {"status": "ok"}

@app.get("/")
def root():
    return {
        "ok": True,
        "service": "country-radar",
        "docs": "/docs",
        "openapi": "/openapi.json",
        "health": "/ping",
    }

# --- OpenAPI sanitizer (strict validators / GPT Actions friendly) ---

def _fix_parameter_schemas(spec: Dict[str, Any]) -> None:
    """Strip illegal schema.examples maps and ensure schemas exist for params."""
    paths = spec.get("paths") or {}
    for _p, item in paths.items():
        if not isinstance(item, dict):
            continue
        for _m, op in item.items():
            if not isinstance(op, dict):
                continue
            params = op.get("parameters")
            if not isinstance(params, list):
                continue
            for p in params:
                sch = p.get("schema")
                if isinstance(sch, dict) and isinstance(sch.get("examples"), dict):
                    sch.pop("examples", None)
                if sch is None:
                    p["schema"] = {"type": "string"}

def _force_response_schema_object(spec: Dict[str, Any], path: str, method: str = "get") -> None:
    """Force responses[200].content['application/json'].schema to be a minimal object."""
    paths = spec.get("paths") or {}
    op = (paths.get(path) or {}).get(method.lower())
    if not isinstance(op, dict):
        return
    responses = op.get("responses") or {}
    resp_200 = responses.get("200")
    if not isinstance(resp_200, dict):
        return
    content = (resp_200.get("content") or {}).get("application/json")
    if not isinstance(content, dict):
        return
    content["schema"] = {
        "type": "object",
        "properties": {},
        "additionalProperties": True,
        "title": f"Response {path} {method.upper()}",
    }

def _mounted_paths() -> Set[str]:
    return {getattr(r, "path", "") for r in app.routes}

def _sanitize_openapi_for_actions(spec: Dict[str, Any]) -> Dict[str, Any]:
    # Servers (so Actions call the correct base URL)
    base = os.getenv("COUNTRY_RADAR_BASE_URL") or os.getenv("RENDER_EXTERNAL_URL") or "http://127.0.0.1:8000"
    spec.setdefault("servers", [{"url": base, "description": "Server"}])

    # Remove HEAD /country-data if a validator chokes on it
    paths = spec.get("paths") or {}
    cd = paths.get("/country-data")
    if isinstance(cd, dict) and "head" in cd:
        cd.pop("head", None)

    # Parameter schema fixes
    _fix_parameter_schemas(spec)

    # Force simple response schemas only for actually-mounted endpoints
    mounted = _mounted_paths()
    for p in ["/country-data", "/v1/debt", "/ping", "/"]:
        if p in mounted:
            _force_response_schema_object(spec, p, "get")
    for optional in ["/v1/country-lite", "/__action_probe", "/__probe_series"]:
        if optional in mounted:
            _force_response_schema_object(spec, optional, "get")

    # Normalize spec version
    spec["openapi"] = "3.1.1"
    return spec

def custom_openapi():
    raw = get_openapi(
        title=app.title,
        version=app.version,
        routes=app.routes,
        description="Macroeconomic data API",
    )
    return _sanitize_openapi_for_actions(raw)

app.openapi = custom_openapi
