from fastapi import FastAPI, Query, Response
from fastapi.responses import JSONResponse
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

INTEREST_RATE_CODES = {
    "FIMM_PA": "Money Market Rate",
    "FIDSR": "Discount Rate",
    "FILR_PA": "Lending Rate",
    "FIRR_PA": "Deposit Rate",
    "FINT_PA": "Treasury Bill Rate",
    "FISN_PA": "Interbank Rate"
}

def get_interest_rate(country_code):
    """
    Attempts to fetch the latest interest rate for a given country using multiple IMF series.
    Tries each code until one returns valid data.

    Returns:
        dict: {
            "value": float,
            "source": str  # Label for the type of interest rate
        }
        or None if no data found
    """
    for code, label in INTEREST_RATE_CODES.items():
        series_key = f"M.{country_code}.{code}"
        try:
            url = f"http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/{series_key}"
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            data = r.json()
            series = data.get("CompactData", {}).get("DataSet", {}).get("Series", {})
            obs = series.get("Obs", [])

            if obs:
                latest_entry = sorted(obs, key=lambda x: x["@TIME_PERIOD"], reverse=True)[0]
                value = float(latest_entry["@OBS_VALUE"])
                return {
                    "value": value,
                    "source": label
                }
        except Exception:
            continue
    return None

def fetch_imf_sdmx_series(iso_alpha_2: str) -> Dict[str, Dict[str, float]]:
    indicator_map = {
        "CPI": "PCPIPCH",
        "FX Rate": "ENDA_XDC_USD_RATE",
        "Interest Rate": "FIMM_PA"
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

            parsed = {}
            for entry in obs:
                try:
                    date = entry["@TIME_PERIOD"]
                    value = float(entry["@OBS_VALUE"])
                    year = date.split("-")[0]
                    parsed[year] = value
                except:
                    continue

            results[label] = parsed

        except Exception as e:
            print(f"[IMF SDMX ERROR] {label}: {e}")
            results[label] = {}

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

        raw_imf = fetch_imf_sdmx_series(iso_alpha_2)
        raw_wb = fetch_worldbank_data(iso_alpha_2)

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

        def get_debt_to_gdp(wb_data):
            try:
                entries = wb_data.get("Debt to GDP (%)", [])
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
            except Exception as e:
                print(f"[Debt-to-GDP] Parsing error: {e}")
            return None

        imf_data = {}

        # 1. CPI
        cpi_entry = extract_latest_numeric_entry(raw_imf.get("CPI", {}))
        if cpi_entry:
            imf_data["CPI"] = cpi_entry
        else:
            wb_entry = extract_wb_entry(raw_wb.get("Inflation (%)"))
            imf_data["CPI"] = wb_entry or {"value": None, "date": None, "source": None}

        # 2. FX Rate
        fx_entry = extract_latest_numeric_entry(raw_imf.get("FX Rate", {}))
        if fx_entry:
            imf_data["FX Rate"] = fx_entry
        else:
            wb_entry = extract_wb_entry(raw_wb.get("PA.NUS.FCRF"))
            imf_data["FX Rate"] = wb_entry or {"value": None, "date": None, "source": None}

        # 3. Interest Rate – uses new get_interest_rate()
        interest_result = get_interest_rate(iso_alpha_2)
        if interest_result:
            imf_data["Interest Rate"] = {
                "value": interest_result["value"],
                "source": f"IMF ({interest_result['source']})"
            }
        else:
            wb_entry = extract_wb_entry(raw_wb.get("FR.INR.RINR"))
            imf_data["Interest Rate"] = wb_entry or {"value": None, "date": None, "source": None}

        # 4. Reserves
        reserves_entry = extract_latest_numeric_entry(raw_imf.get("Reserves (USD)", {}))
        if reserves_entry:
            imf_data["Reserves (USD)"] = reserves_entry
        else:
            # Fallback to World Bank reserves
            wb_raw = raw_wb.get("FI.RES.TOTL.CD")
            wb_entry = extract_wb_entry(wb_raw)

            if wb_entry:
                imf_data["Reserves (USD)"] = wb_entry
            else:
                print(f"[WARN] Reserves missing: {country} - Raw:", wb_raw)
                imf_data["Reserves (USD)"] = {
                    "value": "Not reported",
                    "date": None,
                    "source": "World Bank"
                }
            
        # 5. Debt-to-GDP
        debt_to_gdp = get_debt_to_gdp(raw_wb)

        return {
            "country": country,
            "iso_codes": codes,
            "imf_data": imf_data,
            "debt_to_gdp": debt_to_gdp or {"value": None, "date": None, "source": None},
            "world_bank_data": raw_wb
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
    end_year = datetime.today().year
    start_year = end_year - years

    indicator_map = {
        "inflation": [("PCPIPCH", "Inflation (%)"), ("PCPIEPCH", "Inflation (Alt)")],
        "fx_rate": [("ENDA_XDC_USD_RATE", "Exchange Rate (to USD)")],
        "interest_rate": [("FIMM_PA", "Interest Rate (%)"), ("FIDSR", "Interest Rate (Alt)")]
    }

    if type not in indicator_map:
        return Response(content="Invalid chart type", media_type="text/plain", status_code=400)

    fallback_list = indicator_map[type]

    for indicator_code, label in fallback_list:
        sdmx_url = f"http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/M.{iso_alpha_2}.{indicator_code}"

        try:
            r = requests.get(sdmx_url, timeout=15)
            r.raise_for_status()
            data = r.json()
            series = data.get("CompactData", {}).get("DataSet", {}).get("Series", {})
            obs = series.get("Obs", [])

            print(f"[DEBUG] {country} {indicator_code} entries found: {len(obs)}")

            records = []
            for entry in obs:
                try:
                    date_str = entry["@TIME_PERIOD"]
                    value = float(entry["@OBS_VALUE"])
                    date = datetime.strptime(date_str, "%Y-%m")
                    if date.year >= start_year:
                        records.append((date, value))
                except:
                    continue

            if records:
                break  # Stop at first indicator with data

        except Exception as e:
            print(f"/chart error for {country} {indicator_code}: {e}")
            continue

    if not records:
        return Response(content="No data available", media_type="text/plain", status_code=404)

    records.sort()
    dates, values = zip(*records)

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
