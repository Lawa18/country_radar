from fastapi import FastAPI, Query, Response
from fastapi.responses import JSONResponse
from typing import Dict, Any
import requests
import pycountry
import io
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
from functools import lru_cache

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
    "NY.GDP.MKTP.CD": "GDP (USD)",
    "FP.CPI.TOTL.ZG": "Inflation (%)",
    "SL.UEM.TOTL.ZS": "Unemployment (%)",
    "GC.DOD.TOTL.GD.ZS": "Debt to GDP (%)",
    "BN.CAB.XOKA.GD.ZS": "Current Account Balance (% of GDP)",
    "FI.RES.TOTL.CD": "Reserves (USD)",
    "NE.RSB.GNFS.CD": "Trade Balance",
    "GC.BAL.CASH.GD.ZS": "Fiscal Balance",
    "NY.GDP.MKTP.KD.ZG": "GDP Growth (%)",
    "FS.AST.DOMS.GD.ZS": "Banking Assets",
    "FS.AST.PRVT.GD.ZS": "Credit Growth"
}

ADDITIONAL_INDICATORS = [
    "BN.CAB.XOKA.GD.ZS",  # Current Account
    "NE.RSB.GNFS.CD",     # Trade Balance
    "GC.BAL.CASH.GD.ZS",  # Fiscal Balance
    "NY.GDP.MKTP.KD.ZG",  # GDP Growth
    "FS.AST.DOMS.GD.ZS",  # Banking Assets
    "FS.AST.PRVT.GD.ZS"    # Credit Growth
]

INTEREST_RATE_CODES = {
    "FIMM_PA": "Money Market Rate",
    "FIDSR": "Discount Rate",
    "FILR_PA": "Lending Rate",
    "FIRR_PA": "Deposit Rate",
    "FINT_PA": "Treasury Bill Rate",
    "FISN_PA": "Interbank Rate"
}

def get_interest_rate(country_code):
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

@lru_cache(maxsize=128)
def fetch_imf_sdmx_series(iso_alpha_2: str) -> Dict[str, Dict[str, float]]:
    indicator_map = {
        "CPI": "PCPIPCH",
        "FX Rate": "ENDA_XDC_USD_RATE",
        "Interest Rate": "FIMM_PA",
        "Reserves (USD)": "TRESEGUSD",
        "GDP Nominal": "NGDPD"
    }

    base_url = "http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS"
    results = {}

    for label, code in indicator_map.items():
        url = f"{base_url}/M.{iso_alpha_2}.{code}"
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()

            # 🚫 Ensure it's JSON
            if "application/json" not in r.headers.get("Content-Type", ""):
                raise ValueError(f"Non-JSON response from IMF for {label}")

            data = r.json()
            series = data.get("CompactData", {}).get("DataSet", {}).get("Series", {})
            obs = series.get("Obs", [])

            parsed = {}
            for entry in obs:
                try:
                    date = entry["@TIME_PERIOD"]
                    year = int(date.split("-")[0])
                    if year >= datetime.today().year - 20:
                        value = float(entry["@OBS_VALUE"])
                        parsed[str(year)] = value
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
    for code, label in WB_INDICATORS.items():
        url = f"{base_url}/{iso_alpha_2}/indicator/{code}?format=json&per_page=100"
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            results[code] = r.json()
        except Exception as e:
            print(f"World Bank fetch error for {label}: {e}")
            results[code] = {"error": str(e)}

    return results

@app.get("/country-data")
@app.head("/country-data")
def get_country_data(country: str = Query(..., description="Full country name, e.g., Sweden")):
    try:
        codes = resolve_country_codes(country)
        if not codes:
            return {"error": "Invalid country name"}

        iso_alpha_2 = codes["iso_alpha_2"]
        raw_imf = fetch_imf_sdmx_series(iso_alpha_2)
        raw_wb = fetch_worldbank_data(iso_alpha_2)

        def extract_latest_and_series(entry_dict):
            try:
                pairs = [(int(year), float(val)) for year, val in entry_dict.items() if str(val).replace('.', '', 1).isdigit()]
                if not pairs:
                    return None
                latest = max(pairs, key=lambda x: x[0])
                return {
                    "latest": {
                        "value": latest[1],
                        "date": str(latest[0]),
                        "source": "IMF"
                    },
                    "series": {str(year): val for year, val in sorted(pairs, reverse=True)}
                }
            except:
                return None

        def extract_wb_series(entries):
            try:
                if isinstance(entries, list) and len(entries) > 1:
                    series = {}
                    for e in entries[1]:
                        year = e.get("date")
                        val = e.get("value")
                        if year and val is not None:
                            series[year] = val
                    if not series:
                        return None
                    latest_year = max(series.keys())
                    return {
                        "latest": {
                            "value": series[latest_year],
                            "date": latest_year,
                            "source": "World Bank"
                        },
                        "series": dict(sorted(series.items(), reverse=True))
                    }
            except:
                return None

        def get_debt_to_gdp(wb_data):
            try:
                entries = wb_data.get("GC.DOD.TOTL.GD.ZS", [])
                return extract_wb_series(entries)
            except:
                return None

        imf_data = {}
        indicators = {
            "CPI": ("CPI", "FP.CPI.TOTL.ZG"),
            "FX Rate": ("FX Rate", "PA.NUS.FCRF"),
            "Interest Rate": ("Interest Rate", "FR.INR.RINR"),
            "Reserves (USD)": ("Reserves (USD)", "FI.RES.TOTL.CD")
        }

        for key, (imf_key, wb_code) in indicators.items():
            imf_entry = extract_latest_and_series(raw_imf.get(imf_key, {}))
            wb_entry = extract_wb_series(raw_wb.get(wb_code))
            imf_data[key] = imf_entry or wb_entry or {"latest": {"value": None, "date": None, "source": None}, "series": {}}

        debt_to_gdp = get_debt_to_gdp(raw_wb)

        additional = {}
        for code in ADDITIONAL_INDICATORS:
            entries = raw_wb.get(code)
            if entries:
                parsed = extract_wb_series(entries)
                if parsed:
                    label = WB_INDICATORS.get(code, code)
                    additional[label] = parsed

        return {
            "country": country,
            "iso_codes": codes,
            "imf_data": imf_data,
            "debt_to_gdp": debt_to_gdp or {"latest": {"value": None, "date": None, "source": None}, "series": {}},
            "additional_indicators": additional
        }

    except Exception as e:
        print(f"/country-data error: {e}")
        return {"error": str(e)}

# @app.get("/chart")
# @app.head("/chart")
# def get_chart(country: str, type: str, years: int = 5):
#     codes = resolve_country_codes(country)
#     if not codes:
#         return Response(content="Invalid country name", media_type="text/plain", status_code=400)
# 
#     iso_alpha_3 = codes["iso_alpha_3"]
#     end_year = datetime.today().year
#     start_year = end_year - years
# 
#     datamapper_codes = {
#         "inflation": ["PCPIPCH", "PCPIEPCH"],
#         "fx_rate": ["ENDA_XDC_USD_RATE"],
#         "interest_rate": ["FIMM_PA", "FIDSR", "FILR_PA"]
#     }
# 
#     if type not in datamapper_codes:
#         return Response(content="Invalid chart type", media_type="text/plain", status_code=400)
# 
#     for indicator_code in datamapper_codes[type]:
#         url = f"https://www.imf.org/external/datamapper/api/v1/IFS/{iso_alpha_3}/{indicator_code}"
#         try:
#             r = requests.get(url, timeout=10)
#             r.raise_for_status()
#             data = r.json()
# 
#             values = data.get(indicator_code, {}).get(iso_alpha_3)
#             if not values:
#                 continue
# 
#             records = []
#             for year_str, val in values.items():
#                 try:
#                     year = int(year_str)
#                     if start_year <= year <= end_year and isinstance(val, (int, float, str)) and str(val).replace('.', '', 1).isdigit():
#                         records.append((datetime(year, 1, 1), float(val)))
#                 except:
#                     continue
# 
#             if records:
#                 records.sort()
#                 dates, values = zip(*records)
# 
#                 plt.figure(figsize=(10, 5))
#                 plt.plot(dates, values, marker='o', linewidth=2)
#                 plt.title(f"{indicator_code} – {country} ({dates[0].year}–{dates[-1].year})")
#                 plt.xlabel("Date")
#                 plt.ylabel(indicator_code)
#                 plt.grid(True)
#                 plt.tight_layout()
# 
#                 img_bytes = io.BytesIO()
#                 plt.savefig(img_bytes, format="png")
#                 plt.close()
#                 img_bytes.seek(0)
#                 return Response(content=img_bytes.read(), media_type="image/png")
# 
#         except Exception as e:
#             print(f"/chart error for {country} {indicator_code}: {e}")
#             continue
# 
#     return Response(content="No data available", media_type="text/plain", status_code=404)
#     
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

if __name__ == "__main__":
    import uvicorn
    import os

    port = int(os.environ.get("PORT", 8000))  # Use Render's assigned port or fallback to 8000
    uvicorn.run("main:app", host="0.0.0.0", port=port)
