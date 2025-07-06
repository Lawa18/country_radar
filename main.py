import os
import requests
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import openai

app = FastAPI()

# Allow all origins (for MVP simplicity)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

openai.api_key = os.getenv("OPENAI_API_KEY")
OPENEX_API_KEY = os.getenv("OPENEX_API_KEY")

class RiskRequest(BaseModel):
    country_code: str  # e.g. "NG"

@app.post("/risk-report")
async def generate_risk_report(req: RiskRequest):
    country_code = req.country_code.upper()

    # 1. Get FX Rate
    fx_url = f"https://openexchangerates.org/api/latest.json?app_id={OPENEX_API_KEY}&symbols={country_code}&base=USD"
    fx_resp = requests.get(fx_url).json()
    fx_rate = fx_resp.get("rates", {}).get(country_code, "N/A")

    # 2. Get IMF Inflation (CPI)
    imf_url = f"https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/M.{country_code}.PCPI_IX.?startPeriod=2022"
    imf_resp = requests.get(imf_url).json()
    try:
        inflation = imf_resp["CompactData"]["DataSet"]["Series"]["Obs"][-1]["@OBS_VALUE"]
    except:
        inflation = "N/A"

    # 3. Get World Bank External Debt
    wb_url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/GC.DOD.TOTL.GD.ZS?format=json&per_page=100"
    wb_resp = requests.get(wb_url).json()
    try:
        external_debt = wb_resp[1][0]["value"]
    except:
        external_debt = "N/A"

    # 4. Create GPT prompt
    summary = f"""
    Country: {country_code}
    FX rate (USD/{country_code}): {fx_rate}
    Inflation (YoY): {inflation}%
    External Debt to GNI: {external_debt}%
    """

    prompt = f"""
    Based on the following macro data, generate a receivables risk summary for credit insurers and exporters.

    {summary}

    Include:
    - FX volatility impact
    - Payment behavior implications
    - Sovereign or systemic risk tier (Low, Med, High)
    """

    try:
        completion = openai.ChatCompletion.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}]
        )
        gpt_reply = completion.choices[0].message["content"]
    except Exception as e:
        gpt_reply = f"Error generating GPT summary: {str(e)}"

    return {
        "fx_rate": fx_rate,
        "inflation": inflation,
        "external_debt": external_debt,
        "gpt_summary": gpt_reply
    }