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

