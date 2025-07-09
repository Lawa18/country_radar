from fastapi import FastAPI, Query
from typing import Dict, Any
import requests
import pycountry

app = FastAPI()

# --- Country Name → ISO code resolver ---
def resolve_country_codes(name: str):
    try:
        country = pycountry.countries.lookup(name)
        return {
            "iso_alpha_2": country.alpha_2,
            "iso_alpha_3": country.alpha_3
        }
    except LookupError:
        return None

# --- IMF IFS indicators ---
IMF_INDICATORS = {
    "CPI": "PCPI_IX",
    "FX Rate": "ENDE_XDC_USD_RATE",
    "Interest Rate": "FIDSR",
    "Reserves (USD)": "TRESEGUSD",
    "GDP Nominal": "NGDPD"
}

# --- World Bank WDI indicators ---
WB_INDICATORS = {
    "GDP (USD)": "NY.GDP.MKTP.CD",
    "Inflation (%)": "FP.CPI.TOTL.ZG",
    "Unemployment (%)": "SL.UEM.TOTL.ZS",
    "Debt to GDP (%)": "GC.DOD.TOTL.GD.ZS",
    "Current Account Balance (% of GDP)": "BN.CAB.XOKA.GD.ZS"
}

# --- IMF fetch ---
def fetch_imf_data(iso_alpha_3: str) -> Dict[str, Any]:
    base_url = "https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS"
    results = {}
    for label, code in IMF_INDICATORS.items():
        url = f"{base_url}/{iso_alpha_3}.{code}"
        try:
            r = requests.get(url)
            r.raise_for_status()
            results[label] = r.json()
        except Exception as e:
            results[label] = {"error": str(e)}
    return results

# --- World Bank fetch ---
def fetch_worldbank_data(iso_alpha_2: str) -> Dict[str, Any]:
    base_url = "http://api.worldbank.org/v2/country"
    results = {}
    for label, code in WB_INDICATORS.items():
        url = f"{base_url}/{iso_alpha_2}/indicator/{code}?format=json&per_page=100"
        try:
            r = requests.get(url)
            r.raise_for_status()
            results[label] = r.json()
        except Exception as e:
            results[label] = {"error": str(e)}
    return results

# --- Main API endpoint ---
@app.get("/country-data")
def get_country_data(country: str = Query(..., description="Full country name, e.g., Sweden")):
    codes = resolve_country_codes(country)
    if not codes:
        return {"error": "Invalid country name"}

    imf_data = fetch_imf_data(codes["iso_alpha_3"])
    wb_data = fetch_worldbank_data(codes["iso_alpha_2"])

    return {
        "country": country,
        "iso_codes": codes,
        "imf_data": imf_data,
        "world_bank_data": wb_data
    }
