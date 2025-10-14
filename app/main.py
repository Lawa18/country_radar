# app/main.py — consolidated, non-destructive, and de-duped
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

# --- App metadata
APP_TITLE = "Country Radar API"
APP_VERSION = os.getenv("CR_VERSION", "2025.10.07-step1b")

# --- Create app
app = FastAPI(title=APP_TITLE, version=APP_VERSION, openapi_url="/openapi.json")

# ----------------------------------------------------------------------------
# Middleware
# ----------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    path = request.url.path
    ua = request.headers.get("user-agent", "")
    # Silence Render health checks and /ping
    skip = path == "/ping" or ua.startswith("Render/")
    start = time.time()
    resp: Response = await call_next(request)

    # Trace key endpoints to prove traffic
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

# ----------------------------------------------------------------------------
# Routers (safe include) — avoids hard failures and records problems
# ----------------------------------------------------------------------------
import_errors: List[str] = []

def _include_router_safely(label: str, import_path: str, attr: str = "router") -> None:
    try:
        module = __import__(import_path, fromlist=[attr])
        router = getattr(module, attr)
        app.include_router(router)
        print(f"[init] {label} router mounted from: {module.__file__}")
        # list a few of its routes
        try:
            routes = []
            for r in router.routes:  # type: ignore[attr-defined]
                methods = sorted(getattr(r, "methods", {"GET"}))
                routes.append((r.path, methods))
            print(f"[init] {label} routes: {routes}")
        except Exception:
            pass
    except Exception as e:
        msg = f"Failed to include {label} ({import_path}.{attr}): {e}"
        logging.exception(msg)
        import_errors.append(msg)

# Always try these first; if probe fails, we'll add fallbacks later.
_include_router_safely("probe", "app.routes.probe")
_include_router_safely("country", "app.routes.country")
_include_router_safely("debt", "app.routes.debt")

# ----------------------------------------------------------------------------
# Helpers to inspect/ensure routes (prevents duplicates)
# ----------------------------------------------------------------------------

def _list_routes() -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for r in app.routes:
        methods = sorted(getattr(r, "methods", {"GET"}))
        entries.append({"path": r.path, "methods": methods})
    entries.sort(key=lambda x: x["path"])
    return entries


def _route_exists(path: str, method: str = "GET") -> bool:
    m = method.upper()
    for r in app.routes:
        if getattr(r, "path", None) == path and m in getattr(r, "methods", {"GET"}):
            return True
    return False

# ----------------------------------------------------------------------------
# Core meta endpoints
# ----------------------------------------------------------------------------
@app.get("/ping", tags=["meta"])  # simple health
def ping():
    return {"status": "ok"}

@app.get("/healthz", tags=["meta"])  # k8s-style health
def healthz():
    return {"status": "ok", "version": APP_VERSION}

@app.get("/", tags=["meta"])  # banner + quick diagnostics
def root():
    return {
        "ok": True,
        "service": "country-radar",
        "version": APP_VERSION,
        "routes": _list_routes(),
        "import_errors": import_errors,
    }

# ----------------------------------------------------------------------------
# Ensure critical diagnostics exist (only if missing after router mounts)
# ----------------------------------------------------------------------------
if not _route_exists("/__action_probe", "GET"):
    @app.get("/__action_probe", tags=["diagnostics"])  # fallback only if missing
    def __action_probe_fallback():
        return {"ok": True, "version": APP_VERSION, "source": "app.main:fallback"}

if not _route_exists("/v1/country-lite", "GET"):
    @app.get("/v1/country-lite", tags=["diagnostics"])  # fallback only if missing
    def __country_lite_fallback(country: str = Query(..., description="Full country name, e.g., Germany")):
        try:
            from app.services import indicator_service as _svc  # lazy import, optional
        except Exception as e:
            return JSONResponse({"ok": False, "error": f"indicator_service import failed: {e}"}, status_code=500)

        payload = None
        for name in (
            # lite-style names
            "get_country_lite","country_lite","assemble_country_lite","build_country_lite","get_country_compact","country_compact",
            # legacy full builders fallbacks
            "country_data","build_country_data","assemble_country_data","get_country_data","make_country_data","build_country_payload",
        ):
            f = getattr(_svc, name, None)
            if callable(f):
                try:
                    try:
                        payload = f(country=country, series="none")
                    except TypeError:
                        payload = f(country)
                except Exception as ex:
                    return JSONResponse({"ok": False, "error": str(ex)}, status_code=500)
                break
        if payload is None:
            return JSONResponse({"ok": False, "error": "No lite/full builder found in indicator_service."}, status_code=500)
        if not isinstance(payload, dict):
            payload = {"result": payload}
        payload.setdefault("country", country)
        return JSONResponse(payload)

# ----------------------------------------------------------------------------
# OpenAPI sanitization for strict validators and GPT Actions
# ----------------------------------------------------------------------------

def _fix_parameter_schemas(spec: Dict[str, Any]) -> None:
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
    servers_env = os.getenv("OPENAPI_SERVER_URLS")
    if servers_env:
        servers = [{"url": u.strip()} for u in servers_env.split(",") if u.strip()]
    else:
        base = os.getenv("COUNTRY_RADAR_BASE_URL", "http://localhost:8000")
        servers = [{"url": base, "description": "Default"}]
    spec["servers"] = servers

    # Remove HEAD /country-data if present
    paths = spec.get("paths") or {}
    cd = paths.get("/country-data")
    if isinstance(cd, dict):
        cd.pop("head", None)

    _fix_parameter_schemas(spec)
    _force_response_schema_object(spec, "/country-data", "get")
    if _route_exists("/v1/country-lite", "GET"):
        _force_response_schema_object(spec, "/v1/country-lite", "get")
    if _route_exists("/__action_probe", "GET"):
        _force_response_schema_object(spec, "/__action_probe", "get")

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

# ----------------------------------------------------------------------------
# Startup diagnostics & global JSON error handler
# ----------------------------------------------------------------------------
@app.on_event("startup")
async def _on_startup() -> None:
    app.state.startup_diagnostics = {
        "version": APP_VERSION,
        "routes": _list_routes(),
        "import_errors": import_errors,
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
# ===== Country Radar additive diagnostics (append to end of app/main.py) =====
import os, logging
from typing import Any, Dict, List
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi

CR_APP_VERSION = os.getenv("CR_VERSION", "2025.10.07-step1c")
_import_errors: List[str] = []  # populated if you add safe-import later

def _cr_list_routes(_app) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for r in _app.routes:
        methods = sorted(getattr(r, "methods", {"GET"}))
        entries.append({"path": r.path, "methods": methods})
    entries.sort(key=lambda x: x["path"])
    return entries

def _cr_route_exists(_app, path: str, method: str = "GET") -> bool:
    m = method.upper()
    for r in _app.routes:
        if getattr(r, "path", None) == path and m in getattr(r, "methods", {"GET"}):
            return True
    return False

# /healthz and root banner (you didn't have them yet)
@app.get("/healthz", tags=["meta"])
def _cr_healthz():
    return {"status": "ok", "version": CR_APP_VERSION}

@app.get("/", tags=["meta"])
def _cr_root():
    return {
        "ok": True,
        "service": "country-radar",
        "version": CR_APP_VERSION,
        "routes": _cr_list_routes(app),
        "import_errors": _import_errors,
    }

# Fallback probe ONLY if a router didn't already define it
if not _cr_route_exists(app, "/__action_probe", "GET"):
    @app.get("/__action_probe", tags=["diagnostics"])
    def _cr_action_probe():
        return {"ok": True, "version": CR_APP_VERSION, "source": "app.main:additive"}

# Startup diagnostics captured inside the running Uvicorn process
@app.on_event("startup")
async def _cr_on_startup() -> None:
    app.state.startup_diagnostics = {
        "version": CR_APP_VERSION,
        "routes": _cr_list_routes(app),
        "import_errors": _import_errors,
    }
    logging.getLogger(__name__).info("Startup diagnostics: %s", app.state.startup_diagnostics)

# OpenAPI wrapper: keep your current generator, just tweak servers/schema/version
try:
    _cr_orig_openapi = app.openapi  # whatever you defined earlier
except Exception:
    _cr_orig_openapi = lambda: get_openapi(title=app.title, version=app.version, routes=app.routes)  # noqa: E731

def _cr_force_object_schema(spec: Dict[str, Any], path: str, method: str = "get") -> None:
    try:
        method = method.lower()
        node = spec.setdefault("paths", {}).setdefault(path, {}).setdefault(method, {})
        node = node.setdefault("responses", {}).setdefault("200", {}).setdefault("content", {}).setdefault("application/json", {})
        node["schema"] = {"type": "object", "additionalProperties": True}
    except Exception:
        pass

def _cr_openapi():
    spec = _cr_orig_openapi()
    # Prefer explicit servers from env (works great behind proxies / GPT)
    servers_env = os.getenv("OPENAPI_SERVER_URLS")
    if servers_env:
        spec["servers"] = [{"url": u.strip()} for u in servers_env.split(",") if u.strip()]
    else:
        base = os.getenv("COUNTRY_RADAR_BASE_URL", "http://localhost:8000")
        spec["servers"] = [{"url": base, "description": "Default"}]
    # Normalize and make schemas lenient for key endpoints if present
    spec["openapi"] = "3.1.1"
    for p in ("/country-data", "/v1/country-lite", "/__action_probe", "/healthz", "/"):
        if p in (spec.get("paths") or {}):
            _cr_force_object_schema(spec, p, "get")
    return spec

app.openapi = _cr_openapi
# ===== End additive diagnostics =================================================
