from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from functools import lru_cache
from typing import Dict, Any, Optional
from datetime import datetime
import unicodedata
import requests
import pycountry

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

# ------------------ Helpers ------------------

def latest_common_year_pair(a: dict, b: dict) -> Optional[tuple[int, float, float]]:
    """Return (year, a_val, b_val) for the latest common year between two {year: value} dicts."""
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
    """Convert a {year: value} dict to {value, date, source} using latest year."""
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
    """
    Parse raw World Bank JSON [metadata, [{date, value}, ...]] into { 'YYYY': float(value) }.
    Safe if entries is None or not list-shaped.
    """
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
    """Return {'latest': {...}, 'series': {...}} from raw WB list response."""
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
    """
    Try ISO-3 first (if provided), then ISO-2 for a given World Bank indicator code.
    Returns raw WB JSON ([metadata, data...]) or {"error": "..."} if nothing succeeded.
    """
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
    """
    World Bank WDI fetch for the indicators we need.
    Ensures LCU component series exist: GC.DOD.TOTL.CN (debt), NY.GDP.MKTP.CN (GDP).
    Returns {indicator_code: raw_json_payload}
    """
    WB_CODES = [
        "FP.CPI.TOTL.ZG",     # Inflation (%)
        "PA.NUS.FCRF",        # FX Rate (LCU per USD)
        "FR.INR.RINR",        # Interest Rate
        "FI.RES.TOTL.CD",     # Reserves (USD)
        "NY.GDP.MKTP.KD.ZG",  # GDP Growth (%)
        "GC.DOD.TOTL.GD.ZS",  # Debt to GDP (%) ratio (display/ratio-assisted)
        "SL.UEM.TOTL.ZS",     # Unemployment (%)
        "BN.CAB.XOKA.GD.ZS",  # Current Account Balance (% of GDP)
        "GE.EST",             # Government Effectiveness (WGI)
        # LCU components for compute fallback:
        "GC.DOD.TOTL.CN",     # Central Gov Debt (LCU)
        "NY.GDP.MKTP.CN",     # Nominal GDP (LCU)
    ]
    results: Dict[str, Any] = {}
    for code in WB_CODES:
        results[code] = _wb_fetch_code_any_iso(iso_alpha_2, code, iso_alpha_3)

    # Redundant safety for LCU components
    for forced_code in ["GC.DOD.TOTL.CN", "NY.GDP.MKTP.CN"]:
        if forced_code not in results or not (isinstance(results[forced_code], list) and len(results[forced_code]) > 1):
            results[forced_code] = _wb_fetch_code_any_iso(iso_alpha_2, forced_code, iso_alpha_3)

    return results

@lru_cache(maxsize=256)
def fetch_imf_sdmx_series(iso_alpha_2: str) -> Dict[str, Dict[str, float]]:
    """
    IMF IFS SDMX (CompactData) monthly series aggregated to yearly latest.
    Returns a dict of label -> {year: value}.
    """
    indicator_map = {
        "CPI": "PCPIPCH",                  # CPI inflation, % yoy
        "FX Rate": "ENDA_XDC_USD_RATE",    # End-of-period LCU per USD
        "Interest Rate": "FIMM_PA",        # Money market rate, % pa
        "Reserves (USD)": "TRESEGUSD",     # International reserves, USD
        "GDP Growth (%)": "NGDP_RPCH",     # Real GDP growth %, proxy
    }
    base = "http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS"
    out: Dict[str, Dict[str, float]] = {}
    for label, code in indicator_map.items():
        url = f"{base}/M.{iso_alpha_2}.{code}"
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            if "application/json" not in r.headers.get("Content-Type", ""):
                out[label] = {}
                continue
            data = r.json()
            series = data.get("CompactData", {}).get("DataSet", {}).get("Series", {})
            obs = series.get("Obs", [])
            parsed: Dict[str, float] = {}
            for e in obs:
                try:
                    date = e["@TIME_PERIOD"]
                    year = int(str(date).split("-")[0])
                    if year >= datetime.today().year - 25:
                        parsed[str(year)] = float(e["@OBS_VALUE"])
                except Exception:
                    continue
            out[label] = parsed
        except Exception:
            out[label] = {}
    return out

@lru_cache(maxsize=256)
def fetch_imf_weo_series(iso_alpha_3: str, indicators: list[str]) -> dict:
    """IMF DataMapper WEO series: returns {indicator: {year: value}}."""
    res = {}
    for code in indicators:
        url = f"https://www.imf.org/external/datamapper/api/v1/WEO/{iso_alpha_3}/{code}"
        try:
            r = requests.get(url, timeout=12)
            r.raise_for_status()
            data = r.json()
            series = data.get(iso_alpha_3, {}).get(code, {})
            parsed = {}
            for y, v in series.items():
                try:
                    if v is None:
                        continue
                    yr = int(str(y))
                    if yr >= datetime.today().year - 25:
                        parsed[str(yr)] = float(v)
                except Exception:
                    continue
            res[code] = parsed
        except Exception:
            res[code] = {}
    return res

# ------------------ Routes ------------------

@app.get("/ping")
def ping():
    return {"status": "ok"}

# Debug helper to inspect WB component coverage & common year
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

@app.get("/v1/debt")
def v1_debt(country: str = Query(..., description="Full country name, e.g., Mexico")):
    """
    Compute Debt-to-GDP from components (same currency) using latest common year.
    Priority: IMF WEO (GGXWDG + NGDP) -> WB LCU (GC.DOD.TOTL.CN + NY.GDP.MKTP.CN) -> WB ratio-assisted.
    """
    codes = resolve_country_codes(country)
    if not codes:
        return {"error": "Invalid country name", "country": country}
    iso2, iso3 = codes["iso_alpha_2"], codes["iso_alpha_3"]

    bundle = None

    # 1) IMF WEO preferred
    try:
        weo = fetch_imf_weo_series(iso3, ["GGXWDG", "NGDP"])
        pair = latest_common_year_pair(weo.get("GGXWDG", {}), weo.get("NGDP", {}))
        if pair and pair[2] != 0:
            y, debt_v, gdp_v = pair
            bundle = {
                "debt_value": debt_v,
                "gdp_value": gdp_v,
                "year": y,
                "debt_to_gdp": round((debt_v / gdp_v) * 100, 2),
                "source": "IMF WEO",
                "government_type": "General Government",
                "currency": "LCU",
            }
    except Exception as e:
        print(f"[v1_debt] IMF step failed: {e}")

    # 2) World Bank fallback (LCU components, then ratio-assisted)
    if not bundle:
        try:
            wb = fetch_worldbank_data(iso2, iso3)
            # Raw WB payloads (list-shaped)
            debt_raw = wb.get("GC.DOD.TOTL.CN")
            gdp_raw  = wb.get("NY.GDP.MKTP.CN")

            # Parse into { 'YYYY': float(value) }
            debt_dict = wb_year_dict_from_raw(debt_raw)
            gdp_dict  = wb_year_dict_from_raw(gdp_raw)

            # A) Pure compute from components (preferred)
            pair = latest_common_year_pair(debt_dict, gdp_dict)
            if pair and pair[2] != 0:
                y, debt_v, gdp_v = pair
                bundle = {
                    "debt_value": debt_v,
                    "gdp_value": gdp_v,
                    "year": y,
                    "debt_to_gdp": round((debt_v / gdp_v) * 100, 2),
                    "source": "World Bank WDI",
                    "government_type": "Central Government",
                    "currency": "LCU",
                }
            else:
                # B) Ratio-assisted compute when LCU debt is missing
                ratio_raw  = wb.get("GC.DOD.TOTL.GD.ZS")
                ratio_dict = wb_year_dict_from_raw(ratio_raw)
                pair_ratio = latest_common_year_pair(ratio_dict, gdp_dict)
                if pair_ratio and pair_ratio[2] != 0:
                    y, ratio_pct, gdp_v = pair_ratio
                    debt_v = (ratio_pct / 100.0) * gdp_v
                    bundle = {
                        "debt_value": debt_v,
                        "gdp_value": gdp_v,
                        "year": y,
                        "debt_to_gdp": round(ratio_pct, 2),
                        "source": "World Bank WDI (ratio-assisted)",
                        "government_type": "Central Government",
                        "currency": "LCU",
                    }
        except Exception as e:
            print(f"[v1_debt] WB step failed: {e}")
            # leave bundle as None

    # 3) World Bank USD-components fallback (currency-invariant ratio)
    if not bundle:
        try:
            debt_usd_raw = wb.get("GC.DOD.TOTL.CD")
            gdp_usd_raw  = wb.get("NY.GDP.MKTP.CD")

            debt_usd_dict = wb_year_dict_from_raw(debt_usd_raw)
            gdp_usd_dict  = wb_year_dict_from_raw(gdp_usd_raw)

            pair_usd = latest_common_year_pair(debt_usd_dict, gdp_usd_dict)
            if pair_usd and pair_usd[2] != 0:
                y, debt_v, gdp_v = pair_usd
                bundle = {
                    "debt_value": debt_v,
                    "gdp_value": gdp_v,
                    "year": y,
                    "debt_to_gdp": round((debt_v / gdp_v) * 100, 2),
                    "source": "World Bank WDI (USD components)",
                    "government_type": "Central Government",
                    "currency": "USD", 
                }
        except Exception as e:
            print(f"[Debt USD Fallback] Error: {e}")
            pass
    
    return {
        "country": country,
        "iso_codes": codes,
        "government_debt": (
            {"value": bundle["debt_value"], "date": str(bundle["year"]), 
             "source": bundle["source"], "government_type": bundle["government_type"],
             "currency": bundle.get("currency")} 
            if bundle else {"value": None, "date": None, "source": None, "government_type": None}
        ),
        "nominal_gdp": (
            {"value": bundle["gdp_value"], "date": str(bundle["year"]), "source": bundle["source"],
             "source": bundle["source"], "currency": bundle.get("currency")} 
            if bundle else {"value": None, "date": None, "source": None}
        ),
        "debt_to_gdp": (
            {"value": bundle["debt_to_gdp"], "date": str(bundle["year"]), 
             "source": bundle["source"], "government_type": bundle["government_type"]}
            if bundle else {"value": None, "date": None, "source": None, "government_type": None}
        ),
    }

@app.get("/country-data")
def country_data(country: str = Query(..., description="Full country name, e.g., Mexico")):
    codes = resolve_country_codes(country)
    if not codes:
        return {"error": "Invalid country name", "country": country}

    iso2, iso3 = codes["iso_alpha_2"], codes["iso_alpha_3"]
    imf = fetch_imf_sdmx_series(iso2)
    wb  = fetch_worldbank_data(iso2, iso3)

    # IMF-first indicators with WB fallback
    def imf_series_block(label: str, wb_code: str):
        imf_block = None
        try:
            vals = imf.get(label, {})
            pairs = [(int(y), float(v)) for y, v in vals.items()
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
        "CPI": imf_series_block("CPI", "FP.CPI.TOTL.ZG"),
        "FX Rate": imf_series_block("FX Rate", "PA.NUS.FCRF"),
        "Interest Rate": imf_series_block("Interest Rate", "FR.INR.RINR"),
        "Reserves (USD)": imf_series_block("Reserves (USD)", "FI.RES.TOTL.CD"),
    }

    # GDP Growth (%) – prefer IMF, fallback to WB
    gdp_growth_imf = extract_latest_numeric_entry(imf.get("GDP Growth (%)", {}), "IMF")
    imf_data["GDP Growth (%)"] = gdp_growth_imf or wb_entry(wb.get("NY.GDP.MKTP.KD.ZG")) or {
        "value": None, "date": None, "source": None
    }

    # Unemployment, CAB, Government Effectiveness (WB)
    imf_data["Unemployment (%)"] = wb_entry(wb.get("SL.UEM.TOTL.ZS")) or {"value": None, "date": None, "source": None}
    imf_data["Current Account Balance (% of GDP)"] = wb_entry(wb.get("BN.CAB.XOKA.GD.ZS")) or {"value": None, "date": None, "source": None}
    imf_data["Government Effectiveness"] = wb_entry(wb.get("GE.EST")) or {"value": None, "date": None, "source": None}

    # WB ratio series for historical charting
    wb_debt_ratio_hist = wb_series(wb.get("GC.DOD.TOTL.GD.ZS"))

    # Merge computed trio from /v1/debt
    debt_bundle = v1_debt(country)
    gov_debt = debt_bundle.get("government_debt", {"value": None, "date": None, "source": None, "government_type": None})
    nom_gdp  = debt_bundle.get("nominal_gdp", {"value": None, "date": None, "source": None})
    debt_pct = debt_bundle.get("debt_to_gdp", {"value": None, "date": None, "source": None, "government_type": None})

    # If computed ratio missing, use WB latest ratio; keep WB series for charts
    if (not debt_pct.get("value")) and wb_debt_ratio_hist:
        debt_pct.update({
            "value": wb_debt_ratio_hist["latest"]["value"],
            "date": wb_debt_ratio_hist["latest"]["date"],
            "source": wb_debt_ratio_hist["latest"]["source"],
        })

        # --- Merge computed trio from /v1/debt and preserve currency ---
    debt_bundle = v1_debt(country)  # call the function directly; it returns a dict
    
    # 1) Define safe defaults for the 'latest' blocks
    gov_debt_latest = {
        "value": None, "date": None, "source": None,
        "government_type": None, "currency": None
    }
    nom_gdp_latest = {
        "value": None, "date": None, "source": None,
        "currency": None
    }
    debt_pct_latest = {
        "value": None, "date": None, "source": None,
        "government_type": None
    }
    
    # 2) Overlay values from the compute result, if present
    try:
        if isinstance(debt_bundle, dict):
            gd = debt_bundle.get("government_debt")
            if isinstance(gd, dict):
                for k in gov_debt_latest.keys():
                    if k in gd and gd[k] is not None:
                        gov_debt_latest[k] = gd[k]
    
            ng = debt_bundle.get("nominal_gdp")
            if isinstance(ng, dict):
                for k in nom_gdp_latest.keys():
                    if k in ng and ng[k] is not None:
                        nom_gdp_latest[k] = ng[k]
    
            dp = debt_bundle.get("debt_to_gdp")
            if isinstance(dp, dict):
                for k in debt_pct_latest.keys():
                    if k in dp and dp[k] is not None:
                        debt_pct_latest[k] = dp[k]
    except Exception as e:
        print(f"[/country-data] merge debt bundle failed: {e}")
    
    # 3) If computed ratio is missing, fall back to WB ratio (for display only)
    # You likely already built 'wb_debt_ratio_hist' earlier via wb_series(raw_wb.get("GC.DOD.TOTL.GD.ZS"))
    # If not, compute it here:
    #   wb_debt_ratio_hist = wb_series(wb.get("GC.DOD.TOTL.GD.ZS"))
    
    if (debt_pct_latest["value"] is None) and wb_debt_ratio_hist:
        debt_pct_latest.update({
            "value":  wb_debt_ratio_hist["latest"]["value"],
            "date":   wb_debt_ratio_hist["latest"]["date"],
            "source": wb_debt_ratio_hist["latest"]["source"],
            # government_type remains as-is (None) since WB ratio % is central-gov; set if you want:
            # "government_type": "Central Government"
        })
    
    # 4) Build the exact schema your GPT expects (latest + series)
    government_debt_out = {"latest": gov_debt_latest, "series": {}}
    nominal_gdp_out     = {"latest": nom_gdp_latest, "series": {}}
    debt_to_gdp_out     = {
        "latest": debt_pct_latest,
        "series": (wb_debt_ratio_hist.get("series") if wb_debt_ratio_hist else {})
    }
    
    # 5) Use these in your final return:
    # "government_debt": government_debt_out,
    # "nominal_gdp": nominal_gdp_out,
    # "debt_to_gdp": debt_to_gdp_out,
        
    return JSONResponse(content={
        "country": country,
        "iso_codes": codes,
        "imf_data": imf_data,
        "government_debt": government_debt_out,
        "nominal_gdp": nominal_gdp_out,
        "debt_to_gdp": {
            "latest": debt_to_gdp_out,
            "series": wb_debt_ratio_hist.get("series") if wb_debt_ratio_hist else {}
        },
        "additional_indicators": {}
    })
