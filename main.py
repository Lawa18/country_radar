from fastapi import FastAPI, Query, Response
from typing import Dict, Any
import requests
import pycountry
import io
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime

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

# --- Chart endpoint ---
@app.get("/chart")
def get_chart(country: str, type: str):
    codes = resolve_country_codes(country)
    if not codes:
        return Response(content="Invalid country name", media_type="text/plain", status_code=400)

    iso_alpha_2 = codes["iso_alpha_2"]
    if type == "inflation":
        indicator = "FP.CPI.TOTL.ZG"
        label = "Inflation (%)"
    elif type == "fx_rate":
        indicator = "ENDE_XDC_USD_RATE"
        label = "Exchange Rate (to USD)"
        iso_alpha_3 = codes["iso_alpha_3"]
        url = f"https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/{iso_alpha_3}.{indicator}"
        try:
            r = requests.get(url)
            r.raise_for_status()
            data = r.json()
            series = data['CompactData']['DataSet']['Series']
            obs = series['Obs']
            dates = [datetime.strptime(o['@TIME_PERIOD'], "%Y-%m") for o in obs]
            values = [float(o['@OBS_VALUE']) for o in obs]
        except Exception:
            return Response(content="Failed to fetch IMF FX data", media_type="text/plain", status_code=500)
    else:
        url = f"http://api.worldbank.org/v2/country/{iso_alpha_2}/indicator/{indicator}?format=json&per_page=100"
        try:
            r = requests.get(url)
            r.raise_for_status()
            raw = r.json()
            entries = raw[1]
            entries = [e for e in entries if e["value"] is not None]
            dates = [datetime.strptime(e["date"], "%Y") for e in entries]
            values = [float(e["value"]) for e in entries]
        except Exception:
            return Response(content="Failed to fetch World Bank data", media_type="text/plain", status_code=500)

    # Create chart
    plt.figure(figsize=(8, 4))
    plt.plot(dates, values, marker='o')
    plt.title(f"{label} – {country}")
    plt.xlabel("Date")
    plt.ylabel(label)
    plt.grid(True)
    plt.tight_layout()

    # Return image
    img_bytes = io.BytesIO()
    plt.savefig(img_bytes, format="png")
    plt.close()
    img_bytes.seek(0)
    return Response(content=img_bytes.read(), media_type="image/png")
