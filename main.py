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

def fetch_imf_datamapper(iso_alpha_3: str) -> Dict[str, Any]:
    indicators = ["PCPI_IX", "ENDE_XDC_USD_RATE", "FIDSR", "TRESEGUSD"]
    base_url = "https://www.imf.org/external/datamapper/api/v1/IFS"
    result = {}
    for code in indicators:
        url = f"{base_url}/{iso_alpha_3}/{code}"
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            data = r.json()
            result[code] = data.get(iso_alpha_3, {}).get(code, {})
        except Exception as e:
            print(f"IMF DataMapper fetch error for {code}: {e}")
            result[code] = {"error": str(e)}
    return {
        "CPI": result.get("PCPI_IX", {}),
        "FX Rate": result.get("ENDE_XDC_USD_RATE", {}),
        "Interest Rate": result.get("FIDSR", {}),
        "Reserves (USD)": result.get("TRESEGUSD", {})
    }

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

@app.get("/country-data")
@app.head("/country-data")
def get_country_data(country: str = Query(..., description="Full country name, e.g., Sweden")):
    try:
        codes = resolve_country_codes(country)
        if not codes:
            return {"error": "Invalid country name"}

        iso_alpha_2 = codes["iso_alpha_2"]
        iso_alpha_3 = codes["iso_alpha_3"]

        # --- Step 1: Try IMF ---
        imf_data = fetch_imf_datamapper(iso_alpha_3)

        # --- Step 2: Fetch World Bank once ---
        wb_data = fetch_worldbank_data(iso_alpha_2)

        # --- Step 3: Fallback logic ---
        wb_fallbacks = {
            "CPI": "FP.CPI.TOTL.ZG",
            "FX Rate": "PA.NUS.FCRF",
            "Interest Rate": "FR.INR.RINR",
            "Reserves (USD)": "FI.RES.TOTL.CD"
        }

        for label, wb_code in wb_fallbacks.items():
            imf_value = imf_data.get(label, {})
            if not isinstance(imf_value, dict) or not any(isinstance(v, (float, int)) for v in imf_value.values()):
                print(f"[INFO] Using World Bank fallback for {label}")
                try:
                    raw = wb_data.get(label) or wb_data.get(wb_code)
                    if isinstance(raw, list) and len(raw) > 1:
                        entries = raw[1]
                        entries = [e for e in entries if e["value"] is not None]
                        if entries:
                            latest = max(entries, key=lambda x: x["date"])
                            imf_data[label] = {
                                "value": latest["value"],
                                "date": latest["date"],
                                "source": "World Bank"
                            }
                except Exception as e:
                    print(f"[WB fallback error] for {label}: {e}")
                    imf_data[label] = {"error": "Fallback failed"}

        return {
            "country": country,
            "iso_codes": codes,
            "imf_data": imf_data,
            "world_bank_data": wb_data
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

    iso_alpha_2 = codes["iso_alpha_2"]
    iso_alpha_3 = codes["iso_alpha_3"]
    end_year = datetime.today().year
    start_year = end_year - years

    indicator_map = {
        "inflation": ("PCPI_IX", "Inflation (%)", "FP.CPI.TOTL.ZG"),
        "fx_rate": ("ENDE_XDC_USD_RATE", "Exchange Rate (to USD)", "PA.NUS.FCRF"),
        "interest_rate": ("FIDSR", "Interest Rate (%)", "FR.INR.RINR")
    }

    if type not in indicator_map:
        return Response(content="Invalid chart type", media_type="text/plain", status_code=400)

    imf_code, label, wb_code = indicator_map[type]

    # === Step 1: Try IMF Data ===
    url_imf = f"https://www.imf.org/external/datamapper/api/v1/IFS/{iso_alpha_3}/{imf_code}"
    try:
        r = requests.get(url_imf, timeout=10)
        r.raise_for_status()
        data = r.json()
        series = data.get(iso_alpha_3, {}).get(imf_code, {})
        records = [(int(year), val) for year, val in series.items() if isinstance(val, (int, float))]
    except Exception as e:
        print(f"[IMF fallback] error for {label}: {e}")
        records = []

    # === Step 2: Fallback to World Bank if no data ===
    if not records:
        print(f"[INFO] No IMF data found for {type}, trying World Bank.")
        url_wb = f"http://api.worldbank.org/v2/country/{iso_alpha_2}/indicator/{wb_code}?format=json&per_page=1000"
        try:
            r = requests.get(url_wb, timeout=10)
            r.raise_for_status()
            raw = r.json()
            entries = raw[1]
            entries = [e for e in entries if e["value"] is not None and int(e["date"]) >= start_year]
            records = [(int(e["date"]), float(e["value"])) for e in entries]
        except Exception as e:
            print(f"[WB fallback] error for {label}: {e}")
            records = []

    if not records:
        print(f"[DEBUG] No data found for {label} in {country}")
        return Response(content="No data available", media_type="text/plain", status_code=404)

    records.sort()
    filtered = [(datetime(year, 1, 1), val) for year, val in records if year >= start_year]
    dates, values = zip(*filtered) if filtered else ([], [])

    plt.figure(figsize=(10, 5))
    plt.plot(dates, values, marker='o', linewidth=2)
    plt.title(f"{label} – {country} ({dates[0].year}–{dates[-1].year})")
    plt.xlabel("Year")
    plt.ylabel(label)
    plt.grid(True)
    plt.tight_layout()

    img_bytes = io.BytesIO()
    plt.savefig(img_bytes, format="png")
    plt.close()
    img_bytes.seek(0)
    return Response(content=img_bytes.read(), media_type="image/png")

@app.get("/test-imf-series")
def test_imf_series(country: str, indicator: str):
    codes = resolve_country_codes(country)
    if not codes:
        return {"error": "Invalid country name"}
    iso_alpha_3 = codes["iso_alpha_3"]
    url = f"https://www.imf.org/external/datamapper/api/v1/IFS/{iso_alpha_3}/{indicator}"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        series = data.get(iso_alpha_3, {}).get(indicator, {})
        return JSONResponse(content=series)
    except Exception as e:
        return {"error": str(e)}
