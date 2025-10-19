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
APP_VERSION = os.getenv("CR_VERSION", "2025.10.18")
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
    """
    Lightweight access log + resilient error guard:
    - Always logs key endpoints
    - Never lets an exception tear down the connection (returns JSON 500)
    """
    path = request.url.path
    ua = request.headers.get("user-agent", "")
    start = time.time()
    ip = request.client.host if request.client else "-"

    try:
        resp: Response = await call_next(request)
    except Exception as e:
        dur_ms = (time.time() - start) * 1000.0
        # Emit a compact line that will show up in Render logs
        print(f"[req-err] {request.method} {path}?{request.query_params} ua={ua} ip={ip} -> 500 {dur_ms:.1f}ms err={type(e).__name__}: {e}")
        # Return a JSON 500 rather than bubbling, so Actions see a structured error
        return JSONResponse(
            {"ok": False, "error": f"{type(e).__name__}", "detail": str(e), "path": path},
            status_code=500
        )

    # Light tracing for key endpoints
    if path in ("/__action_probe", "/v1/country-lite", "/country-data", "/v1/debt", "/v1/debt-bundle"):
        print(f"[trace] {request.method} {path}?{request.query_params} ua={ua} ip={ip} -> {resp.status_code}")

    dur_ms = (time.time() - start) * 1000.0
    # Avoid noisy health checks
    if not ua.startswith("Render/") and path not in ("/ping", "/healthz"):
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
        # Do not override tags inside included router; we add a tag on include for grouping,
        # but routes retain their own declared tags too.
        app.include_router(router, tags=[label])

        routes: List[Tuple[str, List[str]]] = []
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
mounted["probe"]        = _safe_include("probe",   "app.routes.probe")
mounted["country"]      = _safe_include("country", "app.routes.country")
mounted["debt"]         = _safe_include("debt",    "app.routes.debt")
# Optional new full-bundle debt router; OK if not present yet
mounted["debt_bundle"]  = _safe_include("debt",    "app.routes.debt_bundle")

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
# OpenAPI servers (Render-first so Actions don't hit localhost)
# -----------------------------------------------------------------------------
def _server_list() -> List[Dict[str, str]]:
    """
    Order of precedence:
      1) RENDER_EXTERNAL_URL (Render sets this, HTTPS)
      2) CR_OPENAPI_SERVER_URLS (comma-separated overrides)
      3) CR_BASE_URL (manual override)
      4) http://localhost:8000 (dev fallback)
    Only include distinct, non-empty URLs; prefer HTTPS for Actions.
    """
    servers: List[str] = []

    rd = os.getenv("RENDER_EXTERNAL_URL", "").strip()
    if rd:
        servers.append(rd)

    override = os.getenv("CR_OPENAPI_SERVER_URLS", "").strip()
    if override:
        servers.extend([u.strip() for u in override.split(",") if u.strip()])

    base = os.getenv("CR_BASE_URL", "").strip()
    if base:
        servers.append(base)

    # Always include localhost as last fallback
    servers.append("http://localhost:8000")

    # De-dupe, filter to plausible URLs, prefer https first in ordering above
    out: List[Dict[str, str]] = []
    seen = set()
    for url in servers:
        if not url or url in seen:
            continue
        seen.add(url)
        out.append({"url": url})
    return out

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=APP_TITLE,
        version=APP_VERSION,
        description=APP_DESC,
        routes=app.routes,
    )
    # Put Render URL first so GPT Actions use it
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
