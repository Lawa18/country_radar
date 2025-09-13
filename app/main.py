# app/main.py
import os
import time
from typing import Any, Dict

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from starlette.middleware.gzip import GZipMiddleware

from app.routes import country, debt

app = FastAPI(title="Country Radar API", version="1.0.4", openapi_url="/openapi.json")

# CORS (permissive while testing)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# GZip to shrink responses
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Routers
app.include_router(country.router, tags=["country"])
app.include_router(debt.router, tags=["debt"])

@app.get("/ping")
def ping():
    return {"status": "ok"}

@app.get("/")
def root():
    return {"ok": True, "service": "country-radar", "docs": "/docs", "openapi": "/openapi.json", "health": "/ping"}

# Quiet health-check noise in logs
@app.middleware("http")
async def log_requests(request: Request, call_next):
    path = request.url.path
    ua = request.headers.get("user-agent", "")
    skip = path == "/ping" or ua.startswith("Render/")
    start = time.time()
    resp = await call_next(request)
    if not skip:
        dur_ms = (time.time() - start) * 1000.0
        print(f"[req] {request.method} {path}?{request.query_params} ua={ua} -> {resp.status_code} {dur_ms:.1f}ms")
    return resp

def _sanitize_openapi_for_actions(schema: Dict[str, Any]) -> Dict[str, Any]:
    # Ensure servers
    base = os.getenv("COUNTRY_RADAR_BASE_URL", "https://country-radar.onrender.com")
    schema.setdefault("servers", [{"url": base, "description": "Production"}])
    # Remove HEAD for /country-data if present
    paths = schema.get("paths") or {}
    cd = paths.get("/country-data") or {}
    if "head" in cd:
        cd.pop("head", None)
        paths["/country-data"] = cd
    # Fix parameter schemas: strip invalid schema.examples objects
    for _path, item in (paths or {}).items():
        for _method, op in (item or {}).items():
            if not isinstance(op, dict):
                continue
            params = op.get("parameters")
            if not isinstance(params, list):
                continue
            for p in params:
                sch = p.get("schema")
                # If FastAPI embedded a dict of examples inside the schema, remove it (or move to parameter-level)
                if isinstance(sch, dict) and isinstance(sch.get("examples"), dict):
                    # safest: drop it
                    sch.pop("examples", None)
                # Guarantee a simple schema exists for query params
                if sch is None:
                    p["schema"] = {"type": "string"}
    # Normalize spec version
    schema["openapi"] = "3.1.1"
    return schema

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(title=app.title, version=app.version, routes=app.routes, description="Macroeconomic data API")
    schema = _sanitize_openapi_for_actions(schema)
    app.openapi_schema = schema
    return app.openapi_schema

app.openapi = custom_openapi
