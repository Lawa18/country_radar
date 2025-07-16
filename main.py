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
def get_chart(
    country: str,
    type: str,
    years: int = 5,
    format: str = Query(default="png", description="png or json")
):
    codes = resolve_country_codes(country)
    if not codes:
        return Response(content="Invalid country name", media_type="text/plain", status_code=400)

    iso_alpha_3 = codes["iso_alpha_3"]

    indicator_map = {
        "inflation": [("PCPI_IX", "Inflation Index"), ("PCPIPCH", "Inflation (%)")],
        "fx_rate": [("ENDE_XDC_USD_RATE", "Exchange Rate (to USD)")],
        "interest_rate": [("FIDSR", "Interest Rate (%)")]
    }

    if type not in indicator_map:
        return Response(content="Invalid chart type", media_type="text/plain", status_code=400)

    for indicator_code, label in indicator_map[type]:
        url = f"https://www.imf.org/external/datamapper/api/v1/IFS/{iso_alpha_3}/{indicator_code}"

        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            data = r.json()
            series = (
                data.get("values", {})
                .get(indicator_code, {})
                .get(iso_alpha_3, {})
            )

            print(f"[DEBUG] Fetched {indicator_code} for {country}: {list(series.items())[:3]}")

            records = []
            for year_str, val in series.items():
                try:
                    year = int(year_str)
                    value = float(val)
                    records.append((year, value))
                except:
                    continue

            records.sort()
            current_year = datetime.today().year
            filtered = [(datetime(y, 1, 1), v) for y, v in records if y >= current_year - years]
            if not filtered:
                continue

            dates, values = zip(*filtered)

            if format == "json":
                return {
                    "label": label,
                    "country": country,
                    "start_year": dates[0].year,
                    "end_year": dates[-1].year,
                    "source": "IMF DataMapper",
                    "series": [{"year": d.year, "value": v} for d, v in zip(dates, values)]
                }

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

        except Exception as e:
            print(f"[DEBUG] Failed fetching {indicator_code} for {country}: {e}")
            continue

    return Response(content="No data available", media_type="text/plain", status_code=404)

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
