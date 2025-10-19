# app/main.py — clean boot with diagnostics, OpenAPI servers, and safe router mounting
from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Tuple

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi
from starlette.middleware.gzip import GZipMiddleware

APP_TITLE = "Country Radar API"
APP_VERSION = os.getenv("CR_VERSION", "2025.10.14-step1")
APP_DESC = "Macroeconomic data API"

# -----------------------------------------------------------------------------
# App
# -----------------------------------------------------------------------------
app = FastAPI(
    title=APP_TITLE,
    version=APP_VERSION,
    description=APP_DESC,
    openapi_url="/openapi.json",
)

# -----------------------------------------------------------------------------
# Middleware
# -----------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # tighten later
    allow_credentials=False,
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=["*"],
    max_age=600,
)
app.add_middleware(GZipMiddleware, minimum_size=1000)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    path = request.url.path
    ua = request.headers.get("user-agent", "")
    start = time.time()
    resp: Response = await call_next(request)

    # Light tracing for key endpoints
    if path in ("/__action_probe", "/v1/country-lite", "/country-data", "/v1/debt", "/v1/debt-bundle"):
        ip = request.client.host if request.client else "-"
        print(f"[trace] {request.method} {path}?{request.query_params} ua={ua} ip={ip} -> {resp.status_code}")

    dur_ms = (time.time() - start) * 1000.0
    if not ua.startswith("Render/") and path not in ("/ping",):
        print(f"[req] {request.method} {path}?{request.query_params} -> {resp.status_code} {dur_ms:.1f}ms")
    return resp

# -----------------------------------------------------------------------------
# Router mounting (safe / non-fatal)
# -----------------------------------------------------------------------------
def _safe_include(label: str, import_path: str, attr: str = "router") -> Tuple[bool, List[Tuple[str, List[str]]]]:
    """Import a module.router and include it; return (ok, routes)."""
    try:
        module = __import__(import_path, fromlist=[attr])
        router = getattr(module, attr)
        app.include_router(router, tags=[label])

        routes = []
        for r in getattr(router, "routes", []):
            if getattr(r, "path", None) and getattr(r, "methods", None):
                routes.append((r.path, sorted(list(r.methods))))
        print(f"[init] {label} router mounted from: {getattr(module, '__file__', import_path)}")
        print(f"[init] {label} routes: {routes}")
        return True, routes
    except Exception as e:
        print(f"[init] WARNING: Failed to mount {label} router from {import_path}: {e}")
        return False, []

mounted: Dict[str, Any] = {}
mounted["probe"]   = _safe_include("probe",   "app.routes.probe")
mounted["country"] = _safe_include("country", "app.routes.country")
mounted["debt"]    = _safe_include("debt",    "app.routes.debt")
# Optional new full-bundle debt router; OK if not present yet
mounted["debt_bundle"] = _safe_include("debt", "app.routes.debt_bundle")

# -----------------------------------------------------------------------------
# Health / Root
# -----------------------------------------------------------------------------
@app.get("/ping")
def ping() -> Dict[str, Any]:
    return {"ok": True, "ts": int(time.time())}

@app.get("/healthz")
def healthz() -> Dict[str, Any]:
    return {"status": "ok", "version": APP_VERSION}

@app.get("/")
def root() -> Dict[str, Any]:
    route_rows = []
    for r in app.routes:
        path = getattr(r, "path", None)
        methods = sorted(list(getattr(r, "methods", []))) if getattr(r, "methods", None) else []
        route_rows.append({"path": path, "methods": methods})
    # Collect any mounts that failed
    import_errors = []
    for k, v in mounted.items():
        if isinstance(v, tuple) and v[0] is False:
            import_errors.append(k)
    return {
        "ok": True,
        "service": "country-radar",
        "version": APP_VERSION,
        "routes": route_rows,
        "import_errors": import_errors,
    }

# -----------------------------------------------------------------------------
# OpenAPI servers (so GPT hits the right base URL on Render)
# -----------------------------------------------------------------------------
def _server_list() -> List[Dict[str, str]]:
    override = os.getenv("CR_OPENAPI_SERVER_URLS", "")
    servers: List[Dict[str, str]] = []
    if override.strip():
        for url in [u.strip() for u in override.split(",") if u.strip()]:
            servers.append({"url": url})
    else:
        servers.append({"url": os.getenv("CR_BASE_URL", "http://localhost:8000")})
        rd = os.getenv("RENDER_EXTERNAL_URL")
        if rd:
            servers.append({"url": rd})
    return servers

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=APP_TITLE,
        version=APP_VERSION,
        description=APP_DESC,
        routes=app.routes,
    )
    schema["servers"] = _server_list()
    app.openapi_schema = schema
    return app.openapi_schema

app.openapi = custom_openapi  # type: ignore

# -----------------------------------------------------------------------------
# Startup diagnostics
# -----------------------------------------------------------------------------
@app.on_event("startup")
async def _startup_diag():
    rows = []
    for r in app.routes:
        path = getattr(r, "path", None)
        methods = sorted(list(getattr(r, "methods", []))) if getattr(r, "methods", None) else []
        ep = getattr(r, "endpoint", None)
        mod = getattr(ep, "__module__", None) if ep else None
        fn  = getattr(ep, "__name__", None) if ep else None
        rows.append({"path": path, "methods": methods, "module": mod, "func": fn})
    app.state.startup_diagnostics = {
        "version": APP_VERSION,
        "servers": _server_list(),
        "routes": rows,
    }
    print("[startup] diagnostics ready")
