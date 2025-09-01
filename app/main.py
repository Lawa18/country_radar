from fastapi import FastAPI
from app.routes import country, debt

app = FastAPI(title="Country Radar API")

# Include routers
app.include_router(country.router)
app.include_router(debt.router)

@app.get("/ping")
def ping():
    return {"status": "ok"}
