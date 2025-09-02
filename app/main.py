import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi

from app.routes import country, debt

app = FastAPI(title="Country Radar API", version="1.0.0")

# CORS: keep permissive while testing; lock down later.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(country.router, tags=["country"])
app.include_router(debt.router, tags=["debt"])

@app.get("/ping")
def ping():
    return {"status": "ok"}

# Friendly root so a bare GET / doesn’t 404
@app.get("/")
def root():
    return {
        "ok": True,
        "service": "country-radar",
        "docs": "/docs",
        "openapi": "/openapi.json",
        "health": "/ping",
    }

# Inject server URL into OpenAPI so GPT Actions know where to call
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(title=app.title, version=app.version, routes=app.routes)
    base = os.getenv("COUNTRY_RADAR_BASE_URL", "https://country-radar.onrender.com")
    schema["servers"] = [{"url": base}]
    app.openapi_schema = schema
    return app.openapi_schema

app.openapi = custom_openapi
