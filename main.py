from fastapi import FastAPI, Query, Response
from typing import Dict, Any
import requests
import pycountry
import io
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime

app = FastAPI()

@app.get("/ping")
def ping():
    return {"status": "ok"}

def resolve_country_codes(name: str):
    try:
        country = pycountry.countries.lookup(name)
        return {
            "iso_alpha_2": country.alpha_2,
            "iso_alpha_3": country.alpha_3
        }
    except LookupError:
        return None

IMF_INDICATORS = {
    "CPI": "PCPI_IX",
    "FX Rate": "ENDE_XDC_USD_RATE",
    "Interest Rate": "FIDSR",
    "Reserves (USD)": "TRESEGUSD",
    "GDP Nominal": "NGDPD"
}

WB_INDICATORS = {
    "GDP (USD)": "NY.GDP.MKTP.CD",
    "Inflation (%)": "FP.CPI.TOTL.ZG",
    "Unemployment (%)": "SL.UEM.TOTL.ZS",
    "Debt to GDP (%)": "GC.DOD.TOTL.GD.ZS",
    "Current Account Balance (% of GDP)": "BN.CAB.XOKA.GD.ZS"
}

def fetch_imf_datamapper_cpi(iso_alpha_3: str) -> Dict[str, Any]:
    indicator_code = "PCPI_IX"
    url = f"https://www.imf.org/external/datamapper/api/v1/IFS/{iso_alpha_3}/{indicator_code}"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        values = data.get(iso_alpha_3, {}).get(indicator_code, {})
        return {"CPI": values}
    except Exception as e:
        print(f"IMF DataMapper CPI fetch error: {e}")
        return {"CPI": {"error": str(e)}}

def fetch_worldbank_data(iso_alpha_2: str) -> Dict[str, Any]:
    base_url = "http://api.worldbank.org/v2/country"
    results = {}
    for label, code in WB_INDICATORS.items():
        url = f"{base_url}/{iso_alpha_2}/indicator/{code}?format=json&per_page=100"
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            results[label] = r.json()
        except Exception as e:
            print(f"World Bank fetch error for {label}: {e}")
            results[label] = {"error": str(e)}
    return results

def fetch_imf_series(iso_alpha_3: str, indicator_code: str, label: str, years: int = 20):
    url = f"https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/{iso_alpha_3}.{indicator_code}"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        series = data['CompactData']['DataSet']['Series']
        obs = series['Obs']
        current_year = datetime.today().year
        filtered = [
            {
                "date": o["@TIME_PERIOD"],
                "value": float(o["@OBS_VALUE"])
            }
            for o in obs
            if int(o["@TIME_PERIOD"][:4]) >= current_year - years
        ]
        return {label: filtered}
    except Exception as e:
        print(f"IMF series fetch error for {label}: {e}")
        return {label: {"error": str(e)}}

@app.get("/country-data")
@app.head("/country-data")
def get_country_data(country: str = Query(..., description="Full country name, e.g., Sweden")):
    try:
        codes = resolve_country_codes(country)
        if not codes:
            return {"error": "Invalid country name"}

        iso_alpha_2 = codes["iso_alpha_2"]
        iso_alpha_3 = codes["iso_alpha_3"]

        imf_data = fetch_imf_datamapper_cpi(iso_alpha_3)
        wb_data = fetch_worldbank_data(iso_alpha_2)

        history = {}
        # Only keep IMF chart series for now, using legacy endpoint until replaced
        history.update(fetch_imf_series(iso_alpha_3, "PCPI_IX", "CPI"))
        history.update(fetch_imf_series(iso_alpha_3, "ENDE_XDC_USD_RATE", "FX Rate"))
        history.update(fetch_imf_series(iso_alpha_3, "FIDSR", "Interest Rate"))

        return {
            "country": country,
            "iso_codes": codes,
            "imf_data": imf_data,
            "world_bank_data": wb_data,
            "history": history
        }
    except Exception as e:
        print(f"/country-data endpoint error: {e}")
        return {"error": f"Server error: {str(e)}"}

@app.get("/chart")
@app.head("/chart")
def get_chart(country: str, type: str, years: int = 5):
    codes = resolve_country_codes(country)
    if not codes:
        return Response(content="Invalid country name", media_type="text/plain", status_code=400)

    iso_alpha_3 = codes["iso_alpha_3"]
    end_year = datetime.today().year
    start_year = end_year - years

    if type == "fx_rate":
        indicator = "ENDE_XDC_USD_RATE"
        label = "Exchange Rate (to USD)"
    elif type == "interest_rate":
        indicator = "FIDSR"
        label = "Interest Rate (%)"
    else:
        indicator = "PCPI_IX"
        label = "Inflation (%)"

    url = f"https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/{iso_alpha_3}.{indicator}"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        series = data['CompactData']['DataSet']['Series']
        obs = series['Obs']
        dates = [datetime.strptime(o['@TIME_PERIOD'], "%Y-%m") for o in obs]
        values = [float(o['@OBS_VALUE']) for o in obs]
        combined = [(d, v) for d, v in zip(dates, values) if d.year >= start_year]
        dates, values = zip(*combined) if combined else ([], [])
    except Exception as e:
        print(f"/chart endpoint error: {e}")
        return Response(content=f"Failed to fetch IMF {label} data", media_type="text/plain", status_code=500)

    plt.figure(figsize=(10, 5))
    plt.plot(dates, values, marker='o', linewidth=2)
    plt.title(f"{label} – {country} ({start_year}–{end_year})")
    plt.xlabel("Date")
    plt.ylabel(label)
    plt.grid(True)
    plt.tight_layout()

    img_bytes = io.BytesIO()
    plt.savefig(img_bytes, format="png")
    plt.close()
    img_bytes.seek(0)
    return Response(content=img_bytes.read(), media_type="image/png")
