# app/main.py
from __future__ import annotations

import os
import time
from typing import Any, Dict

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from starlette.middleware.gzip import GZipMiddleware

# Core routers
from app.routes import country, debt

APP_TITLE = "Country Radar API"
APP_VERSION = "1.0.4"

app = FastAPI(title=APP_TITLE, version=APP_VERSION, openapi_url="/openapi.json")

# --- Middleware --------------------------------------------------------------

# CORS (permissive while testing; tighten for prod if needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# GZip to shrink JSON responses
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Quiet health-check noise + lightweight tracing for Actions-relevant endpoints
@app.middleware("http")
async def log_requests(request: Request, call_next):
    path = request.url.path
    ua = request.headers.get("user-agent", "")
    start = time.time()

    resp: Response = await call_next(request)

    # Trace endpoints relevant to Actions to prove requests are landing
    if path.startswith("/__action_probe") or path.startswith("/v1/country-lite") or path.startswith("/country-data"):
        try:
            ip = request.client.host if request.client else "-"
        except Exception:
            ip = "-"
        print(f"[trace] {request.method} {path}?{request.query_params} ua={ua} ip={ip} -> {resp.status_code}")

    # Silence noisy health checks
    if path != "/ping" and not ua.startswith("Render/"):
        dur_ms = (time.time() - start) * 1000.0
        print(f"[req] {request.method} {path}?{request.query_params} ua={ua} -> {resp.status_code} {dur_ms:.1f}ms")
    return resp

# --- Routers -----------------------------------------------------------------

app.include_router(country.router, tags=["country"])
app.include_router(debt.router, tags=["debt"])

# Optional routers: import if present (won’t crash app if missing)
HAVE_COUNTRY_LITE = False
HAVE_ACTION_PROBE = False
try:
    from app.routes import country_lite  # type: ignore
    app.include_router(country_lite.router, tags=["country"])  # type: ignore
    HAVE_COUNTRY_LITE = True
except Exception:
    print("[init] country_lite router not found; skipping /v1/country-lite")

try:
    from app.routes import action_probe  # type: ignore
    app.include_router(action_probe.router, tags=["probe"])  # type: ignore
    HAVE_ACTION_PROBE = True
except Exception:
    print("[init] action_probe router not found; skipping /__action_probe")

# --- Force-load and mount probe router with explicit diagnostics -------------
import importlib

try:
    probe_mod = importlib.import_module("app.routes.probe")
    if hasattr(probe_mod, "router"):
        app.include_router(probe_mod.router, tags=["probe"])
        print("[init] probe router mounted from:", getattr(probe_mod, "__file__", "<unknown>"))
        try:
            print("[init] probe routes:", [(r.path, sorted(getattr(r, "methods", []) or [])) for r in probe_mod.router.routes])
        except Exception as _e:
            print("[init] failed to enumerate probe routes:", _e)
    else:
        print("[init] probe module imported but has no `router` attribute:", getattr(probe_mod, "__file__", "<unknown>"))
except Exception as e:
    print("[init] probe router import FAILED:", repr(e))
# -----------------------------------------------------------------------------

# --- Health & Root -----------------------------------------------------------

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

# --- OpenAPI sanitizer (to satisfy strict validators & GPT Actions) ----------

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
    """Force responses[200].content['application/json'].schema to a simple object."""
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

def _sanitize_openapi_for_actions(spec: Dict[str, Any]) -> Dict[str, Any]:
    # Servers
    base = (
        os.getenv("COUNTRY_RADAR_BASE_URL")           # explicit override if set
        or os.getenv("RENDER_EXTERNAL_URL")           # Render auto-sets this
        or "http://127.0.0.1:8000"                    # local fallback
    )
    spec.setdefault("servers", [{"url": base, "description": "Server"}])

    # Remove HEAD /country-data from spec if present
    paths = spec.get("paths") or {}
    cd = paths.get("/country-data")
    if isinstance(cd, dict) and "head" in cd:
        cd.pop("head", None)

    # Parameter schema fixes
    _fix_parameter_schemas(spec)

    # Force minimal object schemas where strict validators look
    for p in ["/country-data", "/v1/debt", "/ping", "/"]:
        _force_response_schema_object(spec, p, "get")

    # Only force schemas for optional endpoints if they are actually mounted
    if HAVE_COUNTRY_LITE:
        _force_response_schema_object(spec, "/v1/country-lite", "get")
    if HAVE_ACTION_PROBE:
        _force_response_schema_object(spec, "/__action_probe", "get")

    # If your probe router is mounted, cover its endpoints too
    for maybe in ("/__probe_series",):
        if maybe in (spec.get("paths") or {}).keys():
            _force_response_schema_object(spec, maybe, "get")

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
# ---------------------------------------------------------------------------
