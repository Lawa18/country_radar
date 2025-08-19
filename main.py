from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from functools import lru_cache
from typing import Dict, Any, Optional
from datetime import datetime
import unicodedata
import requests
import pycountry

# --- ISO-2 -> currency code used for display when values are LCU ---
CURRENCY_CODE = {
    "MX": "MXN",
    "NG": "NGN",
    # Extend as needed: "US": "USD", "SE": "SEK", "GB": "GBP", "JP": "JPY",
    # "BR": "BRL", "IN": "INR", "ZA": "ZAR", "CN": "CNY",
}

@lru_cache(maxsize=512)
def resolve_currency_code(iso_alpha_2: str) -> Optional[str]:
    try:
        url = f"http://api.worldbank.org/v2/country/{iso_alpha_2}?format=json"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list) and data[1]:
            node = data[1][0]
            code = (node.get("currency") or {}).get("id") or node.get("currencyCode")
            if code and isinstance(code, str) and len(code.strip()) == 3:
                return code.strip().upper()
    except Exception:
        pass
    return CURRENCY_CODE.get(iso_alpha_2)

app = FastAPI()

# ------------------ Country normalization ------------------

ALIASES = {
    "united mexican states": "mexico",
    "u.s.": "united states",
    "usa": "united states",
    "u.s.a.": "united states",
    "uk": "united kingdom",
    "u.k.": "united kingdom",
}

def normalize_country_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    s = s.strip().lower()
    return ALIASES.get(s, s)

def resolve_country_codes(name: str) -> Optional[Dict[str, str]]:
    try:
        nm = normalize_country_name(name)
        country = pycountry.countries.lookup(nm or name)
        return {"iso_alpha_2": country.alpha_2, "iso_alpha_3": country.alpha_3}
    except LookupError:
        return None

@lru_cache(maxsize=512)
def get_currency_code_wb(iso2: str) -> str | None:
    try:
        url = f"http://api.worldbank.org/v2/country/{iso2}?format=json"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list) and data[1]:
            node = data[1][0]
            cur = node.get("currency") or {}
            code = (cur.get("id") or cur.get("iso2code") or
                    node.get("currencyIso2") or node.get("currencyCode"))
            if code and isinstance(code, str):
                code = code.strip().upper()
                if len(code) in (2, 3):
                    return code
    except Exception as e:
        print(f"[currency] WB lookup failed for {iso2}: {e}")
    return None

# ------------------ Helpers ------------------

def latest_common_year_pair(a: dict, b: dict) -> Optional[tuple[int, float, float]]:
    try:
        ya = {int(y): float(v) for y, v in a.items()
              if isinstance(v, (float, int, str)) and str(v).replace('.', '', 1).replace('-', '', 1).isdigit()}
        yb = {int(y): float(v) for y, v in b.items()
              if isinstance(v, (float, int, str)) and str(v).replace('.', '', 1).replace('-', '', 1).isdigit()}
        common = sorted(set(ya) & set(yb))
        if not common:
            return None
        y = common[-1]
        return (y, ya[y], yb[y])
    except Exception:
        return None

def extract_latest_numeric_entry(entry_dict: dict, source_label: str = "IMF") -> Optional[Dict[str, Any]]:
    try:
        pairs = [(int(y), float(v)) for y, v in entry_dict.items()
                 if isinstance(v, (float, int, str)) and str(v).replace('.', '', 1).replace('-', '', 1).isdigit()]
        if not pairs:
            return None
        y, v = max(pairs, key=lambda x: x[0])
        return {"value": v, "date": str(y), "source": source_label}
    except Exception:
        return None

def wb_year_dict_from_raw(entries) -> Dict[str, float]:
    try:
        if isinstance(entries, list) and len(entries) > 1:
            out = {}
            for row in entries[1]:
                y = row.get("date")
                v = row.get("value")
                if y and v is not None:
                    try:
                        out[str(y)] = float(v)
                    except Exception:
                        continue
            return out
    except Exception:
        pass
    return {}

def wb_series(entries) -> Optional[Dict[str, Any]]:
    d = wb_year_dict_from_raw(entries)
    if not d:
        return None
    latest_y = max(d.keys())
    return {
        "latest": {"value": d[latest_y], "date": latest_y, "source": "World Bank"},
        "series": dict(sorted(d.items(), reverse=True))
    }

def wb_entry(entries) -> Optional[Dict[str, Any]]:
    parsed = wb_series(entries)
    if not parsed:
        return None
    return {"value": parsed["latest"]["value"], "date": parsed["latest"]["date"], "source": parsed["latest"]["source"]}

# ------------------ External fetchers ------------------

def _wb_fetch_code_any_iso(iso2: str, code: str, iso3: Optional[str] = None):
    base = "http://api.worldbank.org/v2/country"
    for iso in ([iso3] if iso3 else []) + [iso2]:
        if not iso:
            continue
        url = f"{base}/{iso}/indicator/{code}?format=json&per_page=100"
        try:
            r = requests.get(url, timeout=12)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list) and len(data) > 1:
                return data
        except Exception:
            continue
    return {"error": f"no data for {code} in {iso3 or iso2}"}

@lru_cache(maxsize=256)
def fetch_worldbank_data(iso_alpha_2: str, iso_alpha_3: Optional[str] = None) -> Dict[str, Any]:
    WB_CODES = [
        "FP.CPI.TOTL.ZG",
        "PA.NUS.FCRF",
        "FR.INR.RINR",
        "FI.RES.TOTL.CD",
        "NY.GDP.MKTP.KD.ZG",
        "GC.DOD.TOTL.GD.ZS",
        "SL.UEM.TOTL.ZS",
        "BN.CAB.XOKA.GD.ZS",
        "GE.EST",
        "GC.DOD.TOTL.CN",
        "NY.GDP.MKTP.CN",
        "GC.DOD.TOTL.CD",
        "NY.GDP.MKTP.CD",
    ]
    results: Dict[str, Any] = {}
    for code in WB_CODES:
        results[code] = _wb_fetch_code_any_iso(iso_alpha_2, code, iso_alpha_3)
    for forced_code in ["GC.DOD.TOTL.CN", "NY.GDP.MKTP.CN"]:
        if forced_code not in results or not (isinstance(results[forced_code], list) and len(results[forced_code]) > 1):
            results[forced_code] = _wb_fetch_code_any_iso(iso_alpha_2, forced_code, iso_alpha_3)
    return results

# ------------------ IMF DataMapper API (for debt-to-gdp) ------------------

IMF_BASE_DM = "https://www.imf.org/external/datamapper/api/v1"
@lru_cache(maxsize=256)
def fetch_imf_datamapper(indicator: str, iso_alpha3: str) -> dict:
    url = f"{IMF_BASE_DM}/{indicator}/{iso_alpha3}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            j = r.json()
            values = j.get("values", {}).get(indicator, {}).get(iso_alpha3, {})
            return {int(k): float(v) for k, v in values.items() if v not in (None, "NaN")}
    except Exception:
        return {}
    return {}

# ------------------ IMF SDMX for macro indicators ------------------

IMF_BASE_SDMX = "https://dataservices.imf.org/SDMX/REST"
@lru_cache(maxsize=256)
def fetch_imf_sdmx_series(iso_alpha2: str) -> dict:
    indicators = {
        "inflation": ["PCPIPCH", "PCPIEPCH"],
        "interest_rate": ["FIDR", "INTDSR"],
        "fx_rate": ["ENDA_XDC_USD_RATE"],
        "gdp_growth": ["NGDP_RPCH"],
        "reserves": ["RESIDE"],
        "unemployment": ["LUR"],
        "current_account": ["BCA"],
    }
    out = {}
    for key, codes in indicators.items():
        series = None
        for ind in codes:
            url = f"{IMF_BASE_SDMX}/CompactData/WEO/{ind}.{iso_alpha2}.?startPeriod=2000"
            try:
                r = requests.get(url, timeout=10)
                if r.status_code == 200 and "<Obs" in r.text:
                    import xml.etree.ElementTree as ET
                    root = ET.fromstring(r.text)
                    data = {}
                    for obs in root.findall(".//{*}Obs"):
                        time = obs.find("{*}Time").text
                        val = obs.find("{*}ObsValue").attrib.get("value")
                        if val is not None:
                            try:
                                data[int(time)] = float(val)
                            except ValueError:
                                pass
                    if data:
                        series = data
                        break
            except Exception:
                continue
        out[key] = series
    return out

# ------------------ Eurostat helpers ------------------

EUROSTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
EU_ISO3 = {
    "AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC","HUN",
    "IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK","SVN","ESP",
    "SWE","ISL","NOR","LIE","CHE","GBR"
}
@lru_cache(maxsize=256)
def fetch_eurostat_jsonstat(dataset: str, **filters) -> Optional[dict]:
    try:
        params = "&".join([f"{k}={v}" for k, v in filters.items() if v is not None])
        url = f"{EUROSTAT_BASE}/{dataset}?{params}" if params else f"{EUROSTAT_BASE}/{dataset}"
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[eurostat] fetch failed {dataset} {filters}: {e}")
        return None

def parse_jsonstat_to_series(js: dict) -> dict:
    try:
        value = js.get("value")
        dims = js.get("dimension", {})
        time_key = None
        for k, v in dims.items():
            if isinstance(v, dict) and (v.get("role") == "time" or k.lower() == "time"):
                time_key = k
                break
        if time_key is None:
            keys = [k for k in dims.keys() if k not in ("id","size")]
            if keys: time_key = keys[-1]
        time_cat = dims.get(time_key, {}).get("category", {})
        time_index = time_cat.get("index", {})
        time_label = time_cat.get("label", {})
        idx_to_period = {}
        for k, idx in time_index.items():
            idx_to_period[int(idx)] = time_label.get(k, k)
        series = {}
        if isinstance(value, list):
            for i, v in enumerate(value):
                if v is None: continue
                period = idx_to_period.get(i)
                if period is None: continue
                series[str(period)] = float(v)
        elif isinstance(value, dict):
            for k, v in value.items():
                try: i = int(k)
                except: continue
                if v is None: continue
                period = idx_to_period.get(i)
                if period is None: continue
                series[str(period)] = float(v)
        return series
    except Exception as e:
        print(f"[eurostat] parse failed: {e}")
        return {}

# ------------------ NEW: Unified debt-to-gdp best-of logic ------------------

def fetch_debt_to_gdp_bestof(iso2, iso3, country):
    # 1. Try Eurostat for ratio and components
    eurostat = None
    try:
        ratio_js = fetch_eurostat_jsonstat("gov_10_dd_edpt1", geo=iso2, unit="PC_GDP", sector="S13")
        ratio_series = parse_jsonstat_to_series(ratio_js) if ratio_js else {}
        if ratio_series:
            years = sorted([y for y in ratio_series if ratio_series[y] is not None], reverse=True)
            if years:
                period = years[0]
                eurostat = {
                    "debt_to_gdp": round(float(ratio_series[period]), 2),
                    "period": period,
                    "source": "Eurostat (debt-to-GDP ratio)",
                    "government_type": "General Government"
                }
    except Exception: pass

    # 2. Try IMF ratio and components
    imf = None
    try:
        ratio = fetch_imf_datamapper("GGXWDG_NGDP", iso3)
        if ratio:
            years = sorted([y for y in ratio if ratio[y] is not None], reverse=True)
            if years:
                period = years[0]
                imf = {
                    "debt_to_gdp": round(float(ratio[period]), 2),
                    "period": period,
                    "source": "IMF WEO (ratio)",
                    "government_type": "General Government"
                }
    except Exception: pass

    # 3. Try WB ratio and components
    wb = None
    try:
        wb_data = fetch_worldbank_data(iso2, iso3)
        ratio_raw = wb_data.get("GC.DOD.TOTL.GD.ZS")
        ratio_dict = wb_year_dict_from_raw(ratio_raw)
        if ratio_dict:
            years = sorted([y for y in ratio_dict if ratio_dict[y] is not None], reverse=True)
            if years:
                year = years[0]
                wb = {
                    "debt_to_gdp": round(float(ratio_dict[year]), 2),
                    "period": year,
                    "source": "World Bank WDI (ratio)",
                    "government_type": "Central Government"
                }
    except Exception: pass

    # ---- Pick the best/newest ----
    all_results = [r for r in [eurostat, imf, wb] if r and r.get("debt_to_gdp") is not None and r.get("period") is not None]
    if not all_results:
        return None
    all_results.sort(key=lambda r: (int(str(r.get("period"))[:4]), ["Eurostat", "IMF", "World Bank"].index(r["source"].split()[0]) if r.get("source") else 99), reverse=True)
    best = all_results[0]
    return best

# ------------------ Endpoints (all preserved) ------------------

@app.get("/ping")
def ping():
    return {"status": "ok"}

@app.get("/v1/debt")
def v1_debt(country: str = Query(..., description="Full country name, e.g., Mexico")):
    codes = resolve_country_codes(country)
    if not codes:
        return {"error": "Invalid country name", "country": country}
    iso2, iso3 = codes["iso_alpha_2"], codes["iso_alpha_3"]
    best = fetch_debt_to_gdp_bestof(iso2, iso3, country)
    return {
        "country": country,
        "iso_codes": codes,
        "debt_to_gdp": best if best else {"value": None, "date": None, "source": None, "government_type": None}
    }

@app.get("/debug/debt")
def debug_debt(country: str = Query(...)):
    codes = resolve_country_codes(country)
    if not codes:
        return {"error": "invalid country", "country": country}
    iso2, iso3 = codes["iso_alpha_2"], codes["iso_alpha_3"]
    wb = fetch_worldbank_data(iso2, iso3)
    debt_years = sorted(map(int, wb_year_dict_from_raw(wb.get("GC.DOD.TOTL.CN")).keys()))
    gdp_years  = sorted(map(int, wb_year_dict_from_raw(wb.get("NY.GDP.MKTP.CN")).keys()))
    ratio_years = sorted(map(int, wb_year_dict_from_raw(wb.get("GC.DOD.TOTL.GD.ZS")).keys()))
    common_comp = sorted(set(debt_years) & set(gdp_years))
    common_ratio = sorted(set(ratio_years) & set(gdp_years))
    return {
        "iso2": iso2, "iso3": iso3,
        "debt_years": debt_years[-10:], "gdp_years": gdp_years[-10:], "ratio_years": ratio_years[-10:],
        "latest_common_components": (common_comp[-1] if common_comp else None),
        "latest_common_ratio": (common_ratio[-1] if common_ratio else None),
    }

@app.get("/country-data")
def country_data(country: str = Query(..., description="Full country name, e.g., Germany")):
    codes = resolve_country_codes(country)
    if not codes:
        return {"error": "Invalid country name", "country": country}
    iso2, iso3 = codes["iso_alpha_2"], codes["iso_alpha_3"]

    # IMF + World Bank macro indicators
    imf = fetch_imf_sdmx_series(iso2)
    wb = fetch_worldbank_data(iso2, iso3)

    def imf_series_block(label: str, wb_code: str):
        imf_block = None
        try:
            vals = imf.get(label, {})
            pairs = [(int(y), float(v)) for y, v in (vals or {}).items()
                     if isinstance(v, (float, int, str)) and str(v).replace('.', '', 1).replace('-', '', 1).isdigit()]
            if pairs:
                y, v = max(pairs, key=lambda x: x[0])
                imf_block = {"latest": {"value": v, "date": str(y), "source": "IMF"},
                             "series": {str(yy): vv for yy, vv in sorted(pairs, reverse=True)}}
        except Exception:
            pass
        wb_block = wb_series(wb.get(wb_code))
        return imf_block or wb_block or {"latest": {"value": None, "date": None, "source": None}, "series": {}}

    imf_data = {
        "CPI": imf_series_block("inflation", "FP.CPI.TOTL.ZG"),
        "FX Rate": imf_series_block("fx_rate", "PA.NUS.FCRF"),
        "Interest Rate": imf_series_block("interest_rate", "FR.INR.RINR"),
        "Reserves (USD)": imf_series_block("reserves", "FI.RES.TOTL.CD"),
    }

    # GDP Growth (%) – prefer IMF, fallback to WB
    gdp_growth_imf = extract_latest_numeric_entry(imf.get("gdp_growth", {}), "IMF")
    imf_data["GDP Growth (%)"] = gdp_growth_imf or wb_entry(wb.get("NY.GDP.MKTP.KD.ZG")) or {
        "value": None, "date": None, "source": None
    }

    # Unemployment, CAB, Government Effectiveness (WB)
    imf_data["Unemployment (%)"] = wb_entry(wb.get("SL.UEM.TOTL.ZS")) or {"value": None, "date": None, "source": None}
    imf_data["Current Account Balance (% of GDP)"] = wb_entry(wb.get("BN.CAB.XOKA.GD.ZS")) or {"value": None, "date": None, "source": None}
    imf_data["Government Effectiveness"] = wb_entry(wb.get("GE.EST")) or {"value": None, "date": None, "source": None}

    # Debt/GDP best-of
    best = fetch_debt_to_gdp_bestof(iso2, iso3, country)
    debt_to_gdp = best if best else {"value": None, "date": None, "source": None, "government_type": None}
    # Series: try Eurostat, then WB ratio series
    ratio_js = fetch_eurostat_jsonstat("gov_10_dd_edpt1", geo=iso2, unit="PC_GDP", sector="S13")
    ratio_series = parse_jsonstat_to_series(ratio_js) if ratio_js else {}
    if not ratio_series:
        ratio_series = wb_year_dict_from_raw(wb.get("GC.DOD.TOTL.GD.ZS"))
    # Compose government_debt and nominal_gdp blocks (showing World Bank as available)
    gov_debt = wb_year_dict_from_raw(wb.get("GC.DOD.TOTL.CN"))
    nom_gdp = wb_year_dict_from_raw(wb.get("NY.GDP.MKTP.CN"))
    usd_gov_debt = wb_year_dict_from_raw(wb.get("GC.DOD.TOTL.CD"))
    usd_nom_gdp = wb_year_dict_from_raw(wb.get("NY.GDP.MKTP.CD"))

    def pick_latest(d: dict, src: str, currency: str, iso2: str):
        if not d:
            return {"value": None, "date": None, "source": None, "currency": currency, "currency_code": None}
        y = max(d)
        return {
            "value": d[y], "date": y, "source": src, "currency": currency, "currency_code": resolve_currency_code(iso2) if currency == "LCU" else "USD"
        }

    government_debt = pick_latest(gov_debt, "World Bank WDI", "LCU", iso2) if gov_debt else pick_latest(usd_gov_debt, "World Bank WDI", "USD", iso2)
    nominal_gdp = pick_latest(nom_gdp, "World Bank WDI", "LCU", iso2) if nom_gdp else pick_latest(usd_nom_gdp, "World Bank WDI", "USD", iso2)

    return JSONResponse(content={
        "country": country,
        "iso_codes": codes,
        "imf_data": imf_data,
        "government_debt": government_debt,
        "nominal_gdp": nominal_gdp,
        "debt_to_gdp": debt_to_gdp,
        "debt_to_gdp_series": dict(sorted(ratio_series.items(), reverse=True)),
        "additional_indicators": {}
    })
