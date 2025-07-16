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

def fetch_imf_sdmx_series(iso_alpha_2: str) -> Dict[str, Any]:
    indicator_map = {
        "Inflation (%)": "PCPIPCH",
        "Exchange Rate (to USD)": "ENDA_XDC_USD_RATE",
        "Interest Rate (%)": "FIMM_PA"
    }

    base_url = "http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS"
    results = {}

    for label, code in indicator_map.items():
        url = f"{base_url}/M.{iso_alpha_2}.{code}"
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            data = r.json()
            series = data.get("CompactData", {}).get("DataSet", {}).get("Series", {})
            obs = series.get("Obs", [])

            # Extract latest value (most recent by date)
            parsed = []
            for entry in obs:
                try:
                    date = entry["@TIME_PERIOD"]
                    value = float(entry["@OBS_VALUE"])
                    parsed.append((date, value))
                except:
                    continue

            parsed.sort(reverse=True)
            latest = parsed[0] if parsed else ("N/A", "N/A")
            results[label] = {"date": latest[0], "value": latest[1]}

        except Exception as e:
            print(f"[IMF SDMX ERROR] {label}: {e}")
            results[label] = {"error": str(e)}

    return results

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

        # --- Step 1: IMF fetch ---
        raw_imf = fetch_imf_datamapper(iso_alpha_3)

        # --- Step 2: World Bank fetch ---
        raw_wb = fetch_worldbank_data(iso_alpha_2)

        # --- Step 3: Normalize values ---
        def extract_latest_numeric_entry(entry_dict):
            try:
                pairs = [(int(year), float(val)) for year, val in entry_dict.items() if isinstance(val, (float, int, str)) and str(val).replace('.', '', 1).isdigit()]
                if not pairs:
                    return None
                latest = max(pairs, key=lambda x: x[0])
                return {
                    "value": latest[1],
                    "date": str(latest[0]),
                    "source": "IMF"
                }
            except Exception:
                return None

        def extract_wb_entry(entries):
            try:
                if isinstance(entries, list) and len(entries) > 1:
                    valid = [e for e in entries[1] if e.get("value") is not None]
                    if not valid:
                        return None
                    latest = max(valid, key=lambda x: x["date"])
                    return {
                        "value": latest["value"],
                        "date": latest["date"],
                        "source": "World Bank"
                    }
            except Exception:
                return None

        indicators = {
            "CPI": ("FP.CPI.TOTL.ZG", "PCPI_IX"),
            "FX Rate": ("PA.NUS.FCRF", "ENDE_XDC_USD_RATE"),
            "Interest Rate": ("FR.INR.RINR", "FIDSR"),
            "Reserves (USD)": ("FI.RES.TOTL.CD", "TRESEGUSD"),
        }

        imf_data = {}

        for label, (wb_code, imf_code) in indicators.items():
            imf_entry = extract_latest_numeric_entry(raw_imf.get(label, {}))
            if imf_entry:
                imf_data[label] = imf_entry
            else:
                wb_entry = extract_wb_entry(raw_wb.get(label) or raw_wb.get(wb_code))
                if wb_entry:
                    imf_data[label] = wb_entry
                else:
                    imf_data[label] = {"value": None, "date": None, "source": None}

        return {
            "country": country,
            "iso_codes": codes,
            "imf_data": imf_data,
            "world_bank_data": raw_wb  # optional for full trace
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

    iso_alpha_2 = codes["iso_alpha_2"]  # e.g. "MX" for Mexico
    end_year = datetime.today().year
    start_year = end_year - years

    # Mapping for IMF SDMX monthly indicators
    indicator_map = {
        "inflation": ("PCPIPCH", "Inflation (%)"),
        "fx_rate": ("ENDA_XDC_USD_RATE", "Exchange Rate (to USD)"),
        "interest_rate": ("FIMM_PA", "Interest Rate (%)")
    }

    if type not in indicator_map:
        return Response(content="Invalid chart type", media_type="text/plain", status_code=400)

    indicator_code, label = indicator_map[type]
    sdmx_url = f"http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/M.{iso_alpha_2}.{indicator_code}"

    try:
        r = requests.get(sdmx_url, timeout=15)
        r.raise_for_status()
        data = r.json()
        series = data.get("CompactData", {}).get("DataSet", {}).get("Series", {})
        obs = series.get("Obs", [])

        print(f"[DEBUG] {country} {indicator_code} entries found: {len(obs)}")

        # Parse the observation list
        records = []
        for entry in obs:
            try:
                date_str = entry["@TIME_PERIOD"]
                value = float(entry["@OBS_VALUE"])
                date = datetime.strptime(date_str, "%Y-%m")
                if date.year >= start_year:
                    records.append((date, value))
            except Exception as e:
                continue

        if not records:
            return Response(content="No data available", media_type="text/plain", status_code=404)

        # Sort and unzip
        records.sort()
        dates, values = zip(*records)

    except Exception as e:
        print(f"/chart error for {country} {indicator_code}: {e}")
        return Response(content="Failed to fetch chart data", media_type="text/plain", status_code=500)

    # Plot the chart
    plt.figure(figsize=(10, 5))
    plt.plot(dates, values, marker='o', linewidth=2)
    plt.title(f"{label} – {country} ({dates[0].year}–{dates[-1].year})")
    plt.xlabel("Date")
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
