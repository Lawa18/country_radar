# app/main.py
from __future__ import annotations

import os
import time
import logging
from typing import Any, Dict, List

from fastapi import FastAPI, Request, Response, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from starlette.middleware.gzip import GZipMiddleware

# Routers (always present)
from app.routes import country, debt
from app.routes import probe as probe_routes

# Optional routers (present in some branches)
HAVE_COUNTRY_LITE = False
HAVE_ACTION_PROBE = False
try:
    from app.routes import country_lite  # type: ignore
    HAVE_COUNTRY_LITE = True
except Exception:
    pass

try:
    from app.routes import action_probe  # type: ignore
    HAVE_ACTION_PROBE = True
except Exception:
    pass

APP_TITLE = "Country Radar API"
APP_VERSION = os.getenv("CR_VERSION", "1.0.5")

app = FastAPI(title=APP_TITLE, version=APP_VERSION, openapi_url="/openapi.json")

# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------
# CORS (permissive while testing; tighten for prod if needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# GZip to shrink JSON responses
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Quiet health-check noise in logs, with lightweight tracing for key endpoints
@app.middleware("http")
async def log_requests(request: Request, call_next):
    path = request.url.path
    ua = request.headers.get("user-agent", "")
    # Silence Render health checks and explicit /ping
    skip = path == "/ping" or ua.startswith("Render/")
    start = time.time()
    resp: Response = await call_next(request)

    # Trace endpoints relevant to Actions to prove requests are landing
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

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
# Existing routers
app.include_router(country.router, tags=["country"])
app.include_router(debt.router, tags=["debt"])
app.include_router(probe_routes.router)

# Optional routers
if HAVE_COUNTRY_LITE:
    app.include_router(country_lite.router, tags=["country"])  # type: ignore
else:
    print("[init] country_lite router not found; skipping /v1/country-lite")

if HAVE_ACTION_PROBE:
    app.include_router(action_probe.router, tags=["country"])  # type: ignore
else:
    print("[init] action_probe router not found; using app-level fallback /__action_probe")

# Health & root
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

@app.get("/healthz")
def healthz():
    return {"status": "ok", "version": APP_VERSION}

# ---------------------------------------------------------------------------
# OpenAPI sanitizer (strict validators & GPT Actions compatibility)
# ---------------------------------------------------------------------------

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
    """
    Force responses[200].content['application/json'].schema to be a simple object with 'properties': {}.
    """
    content = (
        spec.setdefault("paths", {})
        .setdefault(path, {})
        .setdefault(method, {})
        .setdefault("responses", {})
        .setdefault("200", {})
        .setdefault("content", {})
        .setdefault("application/json", {})
    )
    content["schema"] = {
        "type": "object",
        "properties": {},
        "additionalProperties": True,
        "title": f"Response {path} {method.upper()}",
    }


def _sanitize_openapi_for_actions(spec: Dict[str, Any]) -> Dict[str, Any]:
    # Servers override: prefer OPENAPI_SERVER_URLS, fallback to COUNTRY_RADAR_BASE_URL
    servers_env = os.getenv("OPENAPI_SERVER_URLS")
    if servers_env:
        servers = [{"url": u.strip()} for u in servers_env.split(",") if u.strip()]
    else:
        base = os.getenv("COUNTRY_RADAR_BASE_URL", "http://localhost:8000")
        servers = [{"url": base, "description": "Default"}]
    spec["servers"] = servers

    # Remove HEAD /country-data from spec if present
    paths = spec.get("paths") or {}
    cd = paths.get("/country-data")
    if isinstance(cd, dict):
        cd.pop("head", None)

    # Fix parameter schemas
    _fix_parameter_schemas(spec)

    # Only force schemas for optional endpoints if they are actually mounted
    _force_response_schema_object(spec, "/country-data", "get")
    if HAVE_COUNTRY_LITE:
        _force_response_schema_object(spec, "/v1/country-lite", "get")
    if HAVE_ACTION_PROBE:
        _force_response_schema_object(spec, "/__action_probe", "get")

    # Normalize spec version
    spec["openapi"] = "3.1.1"
    return spec


def custom_openapi():
    # Regenerate each call while iterating (avoid stale cache).
    raw = get_openapi(
        title=app.title,
        version=app.version,
        routes=app.routes,
        description="Macroeconomic data API",
    )
    spec = _sanitize_openapi_for_actions(raw)
    return spec


app.openapi = custom_openapi

# ---------------------------------------------------------------------------
# Fallback endpoints (defined ONLY if optional routers are missing)
# ---------------------------------------------------------------------------

# Fallback /__action_probe only if router missing
if not HAVE_ACTION_PROBE:
    @app.get("/__action_probe")
    def __action_probe():
        # Always-on probe even if a router import fails
        return {"ok": True, "path": "/__action_probe"}

# Fallback /v1/country-lite only if router missing
if not HAVE_COUNTRY_LITE:
    @app.get("/v1/country-lite")
    def __country_lite_passthrough(country: str = Query(..., description="Full country name, e.g., Germany")):
        """
        Tries lite builders if present; falls back to your full builder.
        """
        try:
            from app.services import indicator_service as _svc  # type: ignore
        except Exception as e:
            return JSONResponse({"ok": False, "error": f"indicator_service import failed: {e}"}, status_code=500)

        payload = None
        # Prefer lite-style builders if available (safe if missing), then fall back to full builders
        for name in (
            "get_country_lite","country_lite","assemble_country_lite",
            "build_country_lite","get_country_compact","country_compact",
            # fallbacks: full builders (common legacy names)
            "country_data","build_country_data","assemble_country_data","get_country_data","make_country_data",
            "build_country_payload",
        ):
            f = getattr(_svc, name, None)
            if callable(f):
                try:
                    try:
                        payload = f(country=country, series="none")  # some accept series
                    except TypeError:
                        payload = f(country)  # some only accept (country)
                except Exception as e:
                    return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
                break

        if payload is None:
            return JSONResponse({"ok": False, "error": "No lite/full builder found in indicator_service."}, status_code=500)

        if not isinstance(payload, dict):
            payload = {"result": payload}
        payload.setdefault("country", country)
        return JSONResponse(payload)

# ---------------------------------------------------------------------------
# Startup diagnostics & global error handler
# ---------------------------------------------------------------------------

def _list_routes() -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for r in app.routes:
        methods = sorted(getattr(r, "methods", {"GET"}))
        entries.append({"path": r.path, "methods": methods})
    entries.sort(key=lambda x: x["path"])
    return entries


@app.on_event("startup")
async def _on_startup() -> None:
    app.state.startup_diagnostics = {
        "version": APP_VERSION,
        "routes": _list_routes(),
        "have_country_lite": HAVE_COUNTRY_LITE,
        "have_action_probe": HAVE_ACTION_PROBE,
    }
    logging.getLogger(__name__).info("Startup diagnostics: %s", app.state.startup_diagnostics)


@app.exception_handler(Exception)
async def _unhandled_exc_handler(_, exc: Exception):
    logging.exception("Unhandled exception: %s", exc)
    return JSONResponse(
        status_code=500,
        content={
            "error": "internal_error",
            "message": str(exc),
            "version": APP_VERSION,
        },
    )
