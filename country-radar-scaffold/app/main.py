from fastapi import FastAPI
from app.routes.country import router as country_router
from app.routes.debt import router as debt_router

app = FastAPI(title="Country Radar API")
app.include_router(country_router, tags=["country"])
app.include_router(debt_router, tags=["debt"])

@app.get("/ping")
def ping():
    return {"status": "ok"}


