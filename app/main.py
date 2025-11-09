# app/main.py
from __future__ import annotations

import importlib
import logging
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.openapi.utils import get_openapi

logger = logging.getLogger("country-radar")
logging.basicConfig(level=logging.INFO)

# --- keep operation_id stable (avoid FastAPI auto-dedupe renaming) ----------
def _fixed_unique_id(route: APIRoute) -> str:
    # Use the explicit operation_id if set on the route; otherwise fall back.
    return route.operation_id or f"{route.name}_{route.path}".strip("/").replace("/", "_")

app = FastAPI(
    title="Country Radar API",
    description="Macroeconomic data API",
    version="2025.10.19",
    generate_unique_id_function=_fixed_unique_id,
)

# --- inject a single servers URL in the OpenAPI (for GPT connector) ----------
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    # Important: provide exactly one public URL so the connector doesn't get confused.
    schema["servers"] = [{"url": "https://country-radar.onrender.com"}]
    app.openapi_schema = schema
    return app.openapi_schema

app.openapi = custom_openapi  # override


def _safe_include(prefix: str, module_path: str) -> bool:
    """Import a router module and include its `router` if present."""
    try:
        mod = importlib.import_module(module_path)
    except Exception as e:
        logger.error("failed to import %s: %s", module_path, e)
        return False

    router = getattr(mod, "router", None)
    if router is None:
        logger.error("module %s has no `router`", module_path)
        return False

    app.include_router(router)
    logger.info("[init] %s router mounted from: %s", prefix, module_path)
    return True


# ---------------------------------------------------------------------
# STARTUP: mount ONLY the light/cheap router(s)
# ---------------------------------------------------------------------
# ✅ probe is light and contains /v1/country-lite (with operation_id='country_lite_get')
_safe_include("probe", "app.routes.probe")

# ❌ DO NOT mount the legacy/alternate country_lite router anywhere.
#    That caused duplicate paths & operationId renames in OpenAPI.


@app.get("/")
def root():
    return {
        "ok": True,
        "routers_now": ["probe"],
        "routers_later": [
            "country",
            "debt_bundle",
            "debt",
            # "country-lite"  # intentionally NOT mounted anywhere
        ],
        "hint": "call /__load_heavy after deploy to mount the rest (not country-lite)",
    }


@app.get("/healthz")
def healthz():
    # keep this super fast
    return {"status": "ok"}


@app.get("/__load_heavy")
def load_heavy():
    """
    Call this endpoint manually AFTER the service is up.
    This mounts the slow/big routers (EXCEPT country-lite to avoid duplication).
    """
    mounted = {
        "country": _safe_include("country", "app.routes.country"),
        "debt_bundle": _safe_include("debt_bundle", "app.routes.debt_bundle"),
        "debt": _safe_include("debt", "app.routes.debt"),
        # "country-lite": _safe_include("country-lite", "app.routes.country_lite"),  # <-- leave disabled
    }
    return {"ok": True, "mounted": mounted}

# ⚠️ No delayed includes. That previously re-mounted an alternate /v1/country-lite.
