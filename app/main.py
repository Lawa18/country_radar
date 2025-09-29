from __future__ import annotations

import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi

from app.routes import country, debt

app = FastAPI(title="Country Radar API", version="1.0.0")

# CORS (relax now; tighten later if you want)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(country.router, tags=["country"])
app.include_router(debt.router, tags=["debt"])

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

# Health
@app.get("/ping")
def ping():
    return {"status": "ok"}

# OpenAPI: advertise your public base URL (great for GPT Actions)
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(title=app.title, version=app.version, routes=app.routes)
    base = (
        os.getenv("COUNTRY_RADAR_BASE_URL")           # optional explicit override
        or os.getenv("RENDER_EXTERNAL_URL")           # Render auto-sets this
        or "http://127.0.0.1:8000"                    # local fallback
    )
    schema["servers"] = [{"url": base}]
    app.openapi_schema = schema
    return app.openapi_schema

app.openapi = custom_openapi
