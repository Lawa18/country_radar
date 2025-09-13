# app/main.py
import os
import time
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi

from app.routes import country, debt

from starlette.middleware.gzip import GZipMiddleware
app.add_middleware(GZipMiddleware, minimum_size=1000)

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

# Optional: tiny request logger to aid debugging connector calls
@app.middleware("http")
async def log_requests(request: Request, call_next):
    path = request.url.path
    ua = request.headers.get("user-agent", "")
    skip = path == "/ping" or ua.startswith("Render/")  # skip health checks

    start = time.time()
    resp = await call_next(request)
    if not skip:
        dur_ms = (time.time() - start) * 1000.0
        print(f"[req] {request.method} {path}?{request.query_params} ua={ua} -> {resp.status_code} {dur_ms:.1f}ms")
    return resp

# Inject server URL into OpenAPI so GPT Actions know where to call
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(title=app.title, version=app.version, routes=app.routes)
    base = os.getenv("COUNTRY_RADAR_BASE_URL", "https://country-radar.onrender.com")
    schema["servers"] = [{"url": base}]
    # Normalize to 3.1.1 for finicky clients
    schema["openapi"] = "3.1.1"
    app.openapi_schema = schema
    return app.openapi_schema

app.openapi = custom_openapi
