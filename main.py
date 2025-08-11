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
from typing import Optional
from fastapi import Query
from fastapi import Request


app = FastAPI()


def is_recent_year(year_str, threshold=2020):
    try:
        return int(year_str) >= threshold
    except:
        return False

def extract_latest_recent_entry(entry_dict):
    try:
        pairs = [(int(year), float(val)) for year, val in entry_dict.items()
                 if is_recent_year(year) and str(val).replace('.', '', 1).isdigit()]
        if pairs:
            return max(pairs, key=lambda x: x[0])
        # If no recent, fallback to oldest available
        all_pairs = [(int(year), float(val)) for year, val in entry_dict.items()
                     if str(val).replace('.', '', 1).isdigit()]
        return max(all_pairs, key=lambda x: x[0]) if all_pairs else None
    except:
        return None


def extract_latest_numeric_entry(entry_dict):
    try:
        pairs = [(int(year), float(val)) for year, val in entry_dict.items()
                 if isinstance(val, (float, int, str)) and str(val).replace('.', '', 1).isdigit()]
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


# ---- Additive Debt-to-GDP helpers (safe patch) ----
@lru_cache(maxsize=256)
def _fetch_imf_weo_series(iso_alpha_3: str, indicators: list[str]) -> dict:
    """
    IMF DataMapper WEO fetch. Returns {indicator: {year: value}}.
    """
    results = {}
    for code in indicators:
        url = f"https://www.imf.org/external/datamapper/api/v1/WEO/{iso_alpha_3}/{code}"
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            data = r.json()
            series = data.get(iso_alpha_3, {}).get(code, {})
            cleaned = {}
            for y, v in series.items():
                try:
                    if v is None:
                        continue
                    yr = int(str(y))
                    if yr >= datetime.today().year - 25:
                        cleaned[str(yr)] = float(v)
                except Exception:
                    continue
            results[code] = cleaned
        except Exception:
            results[code] = {}
    return results

def _latest_common_year_pair(a: dict, b: dict):
    try:
        ya = {int(y): float(v) for y, v in a.items() if isinstance(v, (int, float, str)) and str(v).replace('.', '', 1).isdigit()}
        yb = {int(y): float(v) for y, v in b.items() if isinstance(v, (int, float, str)) and str(v).replace('.', '', 1).isdigit()}
        common = sorted(set(ya) & set(yb))
        if not common:
            return None
        y = common[-1]
        return (y, ya[y], yb[y])
    except Exception:
        return None
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
    "Unemployment": ["LUR_PT", "LUR_PER"],
    "CPI": "PCPI_IX",
    "FX Rate": "ENDE_XDC_USD_RATE",
    "Interest Rate": "FIDSR",
    "Reserves (USD)": "TRESEGUSD",
    "GDP Nominal": "NGDPD",
    "GDP Growth (%)": "NGDP_RPCH",
    "Debt to GDP (%)": "GGXWDG_NGDP",
    "Unemployment (%)": "LUR",
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
    # Ensure LCU component series for compute fallback are fetched too
    for forced_code in ["GC.DOD.TOTL.CN", "NY.GDP.MKTP.CN"]:
        if forced_code not in results:
            url = f"{base_url}/{iso_alpha_2}/indicator/{forced_code}?format=json&per_page=100"
            try:
                r = requests.get(url, timeout=10)
                r.raise_for_status()
                results[forced_code] = r.json()
            except Exception as e:
                results[forced_code] = {"error": str(e)}


@app.get("/country-data")
@app.head("/country-data")

@app.get("/country-data")
@app.head("/country-data")
def get_country_data(country: str = Query(..., description="Full country name, e.g., Sweden")):
    try:
        codes = resolve_country_codes(country)
        if not codes:
            return {"error": "Invalid country name", "country": country}

        iso_alpha_2 = codes["iso_alpha_2"]
        raw_imf = fetch_imf_sdmx_series(iso_alpha_2)
        raw_wb = fetch_worldbank_data(iso_alpha_2)

        # ---------- helpers (local to this route) ----------
        def extract_latest_and_series(entry_dict):
            try:
                pairs = [(int(y), float(v)) for y, v in entry_dict.items()
                         if isinstance(v, (float, int, str)) and str(v).replace('.', '', 1).isdigit()]
                if not pairs:
                    return None
                latest_y, latest_v = max(pairs, key=lambda x: x[0])
                return {
                    "latest": {"value": latest_v, "date": str(latest_y), "source": "IMF"},
                    "series": {str(y): v for y, v in sorted(pairs, reverse=True)}
                }
            except:
                return None

        def extract_wb_series(entries):
            try:
                if isinstance(entries, list) and len(entries) > 1:
                    series = {}
                    for e in entries[1]:
                        y = e.get("date")
                        v = e.get("value")
                        if y and v is not None:
                            series[str(y)] = float(v)
                    if not series:
                        return None
                    latest_y = max(series.keys())
                    return {
                        "latest": {"value": series[latest_y], "date": latest_y, "source": "World Bank"},
                        "series": dict(sorted(series.items(), reverse=True))
                    }
            except:
                return None

        def extract_wb_entry(entries):
            parsed = extract_wb_series(entries)
            if not parsed:
                return None
            return {"value": parsed["latest"]["value"], "date": parsed["latest"]["date"], "source": parsed["latest"]["source"]}

        def extract_latest_numeric_entry(entry_dict):
            try:
                pairs = [(int(y), float(v)) for y, v in entry_dict.items()
                         if isinstance(v, (float, int, str)) and str(v).replace('.', '', 1).isdigit()]
                if not pairs:
                    return None
                latest_y, latest_v = max(pairs, key=lambda x: x[0])
                return {"value": latest_v, "date": str(latest_y), "source": "IMF"}
            except:
                return None

        def get_wb_debt_ratio_series(wb_dict):
            try:
                entries = wb_dict.get("GC.DOD.TOTL.GD.ZS", [])
                return extract_wb_series(entries)
            except:
                return None
        # ---------------------------------------------------

        # IMF-first indicators with WB fallbacks (unchanged)
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
            imf_data[key] = imf_entry or wb_entry or {
                "latest": {"value": None, "date": None, "source": None},
                "series": {}
            }

        # Additional indicators (unchanged)
        additional = {}
        for code in ADDITIONAL_INDICATORS:
            entries = raw_wb.get(code)
            if entries:
                parsed = extract_wb_series(entries)
                if parsed:
                    label = WB_INDICATORS.get(code, code)
                    additional[label] = parsed

        # GDP Growth (%) – prefer IMF, else WB
        gdp_growth_entry = extract_latest_numeric_entry(raw_imf.get("GDP Growth (%)", {}))
        imf_data["GDP Growth (%)"] = gdp_growth_entry or extract_wb_entry(raw_wb.get("NY.GDP.MKTP.KD.ZG")) or {
            "value": None, "date": None, "source": None
        }

        # WB ratio series for history
        wb_debt_ratio_series = get_wb_debt_ratio_series(raw_wb)

        # ---------- Merge in computed Debt bundle from /v1/debt ----------
        gov_debt = {"value": None, "date": None, "source": None, "government_type": None}
        nom_gdp  = {"value": None, "date": None, "source": None}
        debt_pct = {"value": None, "date": None, "source": None, "government_type": None}
        try:
            bundle = v1_debt(country)  # use same in-process computation
            if isinstance(bundle, dict):
                if isinstance(bundle.get("government_debt"), dict):
                    gd = bundle["government_debt"]
                    gov_debt.update({k: gd.get(k) for k in gov_debt.keys()})
                if isinstance(bundle.get("nominal_gdp"), dict):
                    ng = bundle["nominal_gdp"]
                    nom_gdp.update({k: ng.get(k) for k in nom_gdp.keys() if k in ng})
                if isinstance(bundle.get("debt_to_gdp"), dict):
                    dp = bundle["debt_to_gdp"]
                    debt_pct.update({k: dp.get(k) for k in debt_pct.keys()})
        except Exception as e:
            print(f"[country-data] v1_debt merge failed: {e}")

        # If computed ratio missing, use WB latest ratio for display & history
        if (not debt_pct["value"]) and wb_debt_ratio_series:
            debt_pct.update({
                "value": wb_debt_ratio_series["latest"]["value"],
                "date": wb_debt_ratio_series["latest"]["date"],
                "source": wb_debt_ratio_series["latest"]["source"],
            })
            debt_series_out = wb_debt_ratio_series.get("series", {})
        else:
            debt_series_out = wb_debt_ratio_series.get("series", {}) if wb_debt_ratio_series else {}

        return {
            "country": country,
            "iso_codes": codes,
            "imf_data": imf_data,
            "government_debt": {"latest": {**gov_debt}, "series": {}},
            "nominal_gdp":     {"latest": {**nom_gdp}, "series": {}},
            "debt_to_gdp":     {"latest": {**debt_pct}, "series": debt_series_out},
            "additional_indicators": additional
        }

    except Exception as e:
        print(f"/country-data error: {e}")
        return {"error": str(e), "country": country}

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


@app.get("/v1/debt")
def v1_debt(country: str = Query(..., description="Full country name, e.g., Mexico")):
    """
    Additive endpoint: returns government_debt, nominal_gdp, and debt_to_gdp (%).
    IMF WEO preferred (GGXWDG + NGDP), fallback to World Bank (GC.DOD.TOTL.CN + NY.GDP.MKTP.CN).
    """
    codes = resolve_country_codes(country)
    if not codes:
        return {"error": "Invalid country name", "country": country}

    iso2 = codes["iso_alpha_2"]
    iso3 = codes["iso_alpha_3"]

    debt_bundle = None

    # 1) IMF WEO preferred path
    try:
        weo = _fetch_imf_weo_series(iso3, ["GGXWDG", "NGDP"])
        pair = _latest_common_year_pair(weo.get("GGXWDG", {}), weo.get("NGDP", {}))
        if pair and pair[2] != 0:
            y, debt_val, gdp_val = pair
            debt_bundle = {
                "debt_value": debt_val,
                "gdp_value": gdp_val,
                "year": y,
                "debt_to_gdp": round((debt_val / gdp_val) * 100, 2),
                "source": "IMF WEO",
                "government_type": "General Government",
            }
    except Exception:
        pass

    # 2) World Bank fallback (LCU)
    if not debt_bundle:
        try:
            wb = fetch_worldbank_data(iso2)
            debt_wb = wb.get("GC.DOD.TOTL.CN", {})
            gdp_wb  = wb.get("NY.GDP.MKTP.CN", {})

            def _wb_series_to_dict(entries):
                out = {}
                if isinstance(entries, list) and len(entries) > 1:
                    for e in entries[1]:
                        year = e.get("date")
                        val = e.get("value")
                        if year and val is not None:
                            try:
                                out[str(year)] = float(val)
                            except Exception:
                                continue
                return out

            d = _wb_series_to_dict(debt_wb)
            g = _wb_series_to_dict(gdp_wb)
            pair = _latest_common_year_pair(d, g)
            if pair and pair[2] != 0:
                y, debt_val, gdp_val = pair
                debt_bundle = {
                    "debt_value": debt_val,
                    "gdp_value": gdp_val,
                    "year": y,
                    "debt_to_gdp": round((debt_val / gdp_val) * 100, 2),
                    "source": "World Bank WDI",
                    "government_type": "Central Government",
                }
        except Exception:
            pass

    return {
        "country": country,
        "iso_codes": codes,
        "government_debt": (
            {"value": debt_bundle["debt_value"], "date": str(debt_bundle["year"]), "source": debt_bundle["source"], "government_type": debt_bundle["government_type"]}
            if debt_bundle else {"value": None, "date": None, "source": None, "government_type": None}
        ),
        "nominal_gdp": (
            {"value": debt_bundle["gdp_value"], "date": str(debt_bundle["year"]), "source": debt_bundle["source"]}
            if debt_bundle else {"value": None, "date": None, "source": None}
        ),
        "debt_to_gdp": (
            {"value": debt_bundle["debt_to_gdp"], "date": str(debt_bundle["year"]), "source": debt_bundle["source"], "government_type": debt_bundle["government_type"]}
            if debt_bundle else {"value": None, "date": None, "source": None}
        ),
    }

