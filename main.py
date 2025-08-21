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
    ]
    results: Dict[str, Any] = {}
    for code in WB_CODES:
        results[code] = _wb_fetch_code_any_iso(iso_alpha_2, code, iso_alpha_3)
    for forced_code in ["GC.DOD.TOTL.CN", "NY.GDP.MKTP.CN"]:
        if forced_code not in results or not (isinstance(results[forced_code], list) and len(results[forced_code]) > 1):
            results[forced_code] = _wb_fetch_code_any_iso(iso_alpha_2, forced_code, iso_alpha_3)
    return results

@lru_cache(maxsize=256)
def fetch_imf_sdmx_series(iso_alpha_2: str) -> Dict[str, Dict[str, float]]:
    indicator_map = {
        "CPI": "PCPIPCH",
        "FX Rate": "ENDA_XDC_USD_RATE",
        "Interest Rate": "FIMM_PA",
        "Reserves (USD)": "TRESEGUSD",
        "GDP Growth (%)": "NGDP_RPCH",
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
def v1_debt(country: str = Query(..., description="Full country name, e.g., Germany")):
    try:
        codes = resolve_country_codes(country)
        if not codes:
            return {"error": "Invalid country name"}
        iso2 = codes["iso_alpha_2"]
        iso3 = codes["iso_alpha_3"]
        path_used = None

        eurostat_series = {}
        best = None

        # 1) Eurostat annual ratio (EU/EEA/UK) - ratio first
        try:
            eurostat_series = eurostat_debt_to_gdp_annual(iso2) or {}
            if eurostat_series:
                y = max(int(k) for k in eurostat_series.keys() if str(k).isdigit())
                best = {
                    "source": "Eurostat (debt-to-GDP ratio)",
                    "period": str(y),
                    "debt_to_gdp": float(eurostat_series[str(y)]),
                    "government_type": "General Government",
                }
                path_used = "EUROSTAT_ANNUAL_RATIO"
        except Exception:
            eurostat_series = {}

        # 2) IMF WEO annual ratio
        if best is None:
            try:
                imf_series = imf_debt_to_gdp_annual(iso3) or {}
                if imf_series:
                    y = max(int(k) for k in imf_series.keys() if str(k).isdigit())
                    best = {
                        "source": "IMF WEO (ratio)",
                        "period": str(y),
                        "debt_to_gdp": float(imf_series[str(y)]),
                        "government_type": "General Government",
                    }
                    path_used = "IMF_ANNUAL_RATIO"
            except Exception:
                pass

        # 3) World Bank WDI annual ratio
        if best is None:
            try:
                wb = fetch_worldbank_data(iso2, iso3)
                ratio_raw = wb.get("GC.DOD.TOTL.GD.ZS")
                ratio_dict = wb_year_dict_from_raw(ratio_raw)
                if ratio_dict:
                    years = sorted([int(y) for y, v in ratio_dict.items() if v is not None])
                    if years:
                        y = years[-1]
                        best = {
                            "source": "World Bank WDI (ratio)",
                            "period": str(y),
                            "debt_to_gdp": round(float(ratio_dict[y]), 2),
                            "government_type": "Central Government",
                        }
                        path_used = "WB_ANNUAL_RATIO"
            except Exception:
                pass

        # 4) Compute from annual levels only if no ratio found
        government_debt = None
        nominal_gdp = None
        if best is None:
            try:
                wb = fetch_worldbank_data(iso2, iso3)
                debt_lcu = wb_year_dict_from_raw(wb.get("GC.DOD.TOTL.CN")) or {}
                gdp_lcu = wb_year_dict_from_raw(wb.get("NY.GDP.MKTP.CN")) or {}
                common_years = sorted(
                    set(int(y) for y in debt_lcu if debt_lcu[y] is not None)
                    & set(int(y) for y in gdp_lcu if gdp_lcu[y] is not None)
                )
                if common_years:
                    y = common_years[-1]
                    d = float(debt_lcu[y])
                    g = float(gdp_lcu[y])
                    if g != 0:
                        best = {
                            "source": "World Bank WDI (computed)",
                            "period": str(y),
                            "debt_to_gdp": round((d / g) * 100, 2),
                            "government_type": "Central Government",
                        }
                        path_used = "WB_ANNUAL_COMPUTED"
                        government_debt = {
                            "value": d,
                            "date": str(y),
                            "source": "World Bank WDI",
                            "government_type": "Central Government",
                            "currency": "LCU",
                            "currency_code": resolve_currency_code(iso2),
                        }
                        nominal_gdp = {
                            "value": g,
                            "date": str(y),
                            "source": "World Bank WDI",
                            "currency": "LCU",
                            "currency_code": resolve_currency_code(iso2),
                        }
                if best is None:
                    debt_usd = wb_year_dict_from_raw(wb.get("GC.DOD.TOTL.CD")) or {}
                    gdp_usd = wb_year_dict_from_raw(wb.get("NY.GDP.MKTP.CD")) or {}
                    common_years = sorted(
                        set(int(y) for y in debt_usd if debt_usd[y] is not None)
                        & set(int(y) for y in gdp_usd if gdp_usd[y] is not None)
                    )
                    if common_years:
                        y = common_years[-1]
                        d = float(debt_usd[y])
                        g = float(gdp_usd[y])
                        if g != 0:
                            best = {
                                "source": "World Bank WDI (computed USD)",
                                "period": str(y),
                                "debt_to_gdp": round((d / g) * 100, 2),
                                "government_type": "Central Government",
                            }
                            path_used = "WB_ANNUAL_COMPUTED_USD"
                            government_debt = {
                                "value": d,
                                "date": str(y),
                                "source": "World Bank WDI",
                                "government_type": "Central Government",
                                "currency": "USD",
                                "currency_code": "USD",
                            }
                            nominal_gdp = {
                                "value": g,
                                "date": str(y),
                                "source": "World Bank WDI",
                                "currency": "USD",
                                "currency_code": "USD",
                            }
            except Exception:
                pass

        # If still nothing, return empty ratio but keep series if we have Eurostat
        if best is None:
            return {
                "country": country,
                "iso_codes": codes,
                "debt_to_gdp": {"value": None, "date": None, "source": None, "government_type": None},
                "debt_to_gdp_series": eurostat_series if isinstance(eurostat_series, dict) else {},
                "path_used": path_used,
            }

        # Choose the historical series aligned to the chosen source
        series = {}
        if path_used == "EUROSTAT_ANNUAL_RATIO":
            series = eurostat_series
        elif path_used == "IMF_ANNUAL_RATIO":
            try:
                series = imf_debt_to_gdp_annual(iso3) or {}
            except Exception:
                series = {}
        elif path_used and path_used.startswith("WB_"):
            try:
                wb = fetch_worldbank_data(iso2, iso3)
                ratio_raw = wb.get("GC.DOD.TOTL.GD.ZS")
                series = wb_year_dict_from_raw(ratio_raw) or {}
                series = {str(k): v for k, v in series.items() if v is not None}
            except Exception:
                series = {}

        resp = {
            "country": country,
            "iso_codes": codes,
            "debt_to_gdp": {
                "value": best["debt_to_gdp"],
                "date": best["period"],
                "source": best["source"],
                "government_type": best["government_type"],
            },
            "debt_to_gdp_series": series,
            "path_used": path_used,
        }
        if government_debt:
            resp["government_debt"] = government_debt
        if nominal_gdp:
            resp["nominal_gdp"] = nominal_gdp
        return resp
    except Exception as e:
        return {"error": str(e)}

@app.get("/country-data")
def country_data(country: str = Query(..., description="Full country name, e.g., Germany")):
    codes = resolve_country_codes(country)
    if not codes:
        return {"error": "Invalid country name", "country": country}
    iso2, iso3 = codes["iso_alpha_2"], codes["iso_alpha_3"]
    imf = fetch_imf_sdmx_series(iso2)
    wb  = fetch_worldbank_data(iso2, iso3)

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

    wb_debt_ratio_hist = wb_series(wb.get("GC.DOD.TOTL.GD.ZS"))
    # Eurostat annual ratio
    ratio_es = eurostat_debt_to_gdp_annual(iso2)
    debt_bundle = v1_debt(country)

    gov_debt_latest = {
        "value": None, "date": None, "source": None,
        "government_type": None, "currency": None, "currency_code": None,
    }
    nom_gdp_latest = {
        "value": None, "date": None, "source": None,
        "currency": None, "currency_code": None,
    }
    debt_pct_latest = {
        "value": None, "date": None, "source": None,
        "government_type": None
    }

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

    # --- Eurostat: Prefer Eurostat series for eligible EU/EEA/UK countries ---
    # v1_debt now attaches 'eurostat_series' for these.
    eurostat_series = debt_bundle.get("eurostat_series", {}) if isinstance(debt_bundle, dict) else {}
    es_gd = eurostat_series.get("government_debt_series", {})
    es_gdp = eurostat_series.get("nominal_gdp_series", {})

    # If we have Eurostat series, use for history and prefer for "latest" if most recent
    if es_gd and es_gdp:
        # Find latest common Eurostat period
        common_periods = sorted(set(es_gd) & set(es_gdp), reverse=True)
        if common_periods:
            latest_period = common_periods[0]
            try:
                gov_debt_latest.update({
                    "value": es_gd[latest_period],
                    "date": latest_period,
                    "source": "Eurostat",
                    "government_type": "General Government",
                    "currency": "LCU",
                    "currency_code": resolve_currency_code(iso2),
                })
                nom_gdp_latest.update({
                    "value": es_gdp[latest_period],
                    "date": latest_period,
                    "source": "Eurostat",
                    "currency": "LCU",
                    "currency_code": resolve_currency_code(iso2),
                })
                # Calculate Eurostat ratio
                if es_gdp[latest_period]:
                    eurostat_debt_pct = round(es_gd[latest_period] / es_gdp[latest_period] * 100, 2)
                    debt_pct_latest.update({
                        "value": eurostat_debt_pct,
                        "date": latest_period,
                        "source": "Eurostat",
                        "government_type": "General Government",
                    })
            except Exception as e:
                print(f"[Eurostat merge] failed: {e}")

    # Eurostat historical series for charts (if available)
    government_debt_out = {"latest": gov_debt_latest, "series": es_gd if es_gd else {}}
    nominal_gdp_out     = {"latest": nom_gdp_latest, "series": es_gdp if es_gdp else {}}

    # Historical ratio series (Eurostat if available, fallback WB)
    debt_to_gdp_series = {}
    if es_gd and es_gdp:
        for period in set(es_gd) & set(es_gdp):
            gdp = es_gdp[period]
            if gdp:
                debt_to_gdp_series[period] = round(es_gd[period] / gdp * 100, 2)
    if not debt_to_gdp_series and wb_debt_ratio_hist:
        debt_to_gdp_series = wb_debt_ratio_hist.get("series", {})
    debt_to_gdp_out = {
        "latest": debt_pct_latest,
        "series": debt_to_gdp_series
    }

    # Fill in currency_code if missing
    try:
        if government_debt_out["latest"].get("currency") == "LCU" and not government_debt_out["latest"].get("currency_code"):
            government_debt_out["latest"]["currency_code"] = resolve_currency_code(iso2)
        if nominal_gdp_out["latest"].get("currency") == "LCU" and not nominal_gdp_out["latest"].get("currency_code"):
            nominal_gdp_out["latest"]["currency_code"] = resolve_currency_code(iso2)
        if government_debt_out["latest"].get("currency") == "USD" and not government_debt_out["latest"].get("currency_code"):
            government_debt_out["latest"]["currency_code"] = "USD"
        if nominal_gdp_out["latest"].get("currency") == "USD" and not nominal_gdp_out["latest"].get("currency_code"):
            nominal_gdp_out["latest"]["currency_code"] = "USD"
    except Exception:
        pass

    return JSONResponse(content={
        "country": country,
        "iso_codes": codes,
        "imf_data": imf_data,
        "government_debt": government_debt_out,
        "nominal_gdp": nominal_gdp_out,
        "debt_to_gdp": debt_to_gdp_out,
        "additional_indicators": {}
    })

# ------------------ Eurostat (free) JSON-stat 2.0 helpers ------------------
EUROSTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"


@lru_cache(maxsize=256)
def imf_debt_to_gdp_annual(iso3: str) -> dict:
    """IMF WEO: General government gross debt, percent of GDP (GGXWDG_NGDP). Returns {YYYY: float}."""
    try:
        if 'fetch_imf_datamapper' in globals():
            data = fetch_imf_datamapper("GGXWDG_NGDP", iso3) or {}
            return {str(int(y)): float(v) for y, v in data.items()}
        else:
            return {}
    except Exception as e:
        print(f"[IMF] ratio annual failed {iso3}: {e}")
        return {}




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


def parse_jsonstat_to_series(js: dict) -> Dict[str, float]:
    """Parse Eurostat JSON‑stat into {period: value} with correct handling of multi‑dimensional arrays.
    We vary only the time dimension and hold all other dimensions at index 0.
    """
    try:
        dims = js.get("dimension", {})
        ids = js.get("id") or [k for k in dims.keys() if k not in ("id", "size")]
        sizes = js.get("size")
        if not sizes:
            sizes = []
            for d in ids:
                dd = dims.get(d, {})
                sz = dd.get("size") if isinstance(dd, dict) else None
                sizes.append(int(sz or 1))

        # Identify time dimension and its position
        time_key = None
        for k in ids:
            d = dims.get(k, {})
            if isinstance(d, dict) and (d.get("role") == "time" or k.lower() == "time"):
                time_key = k
                break
        if not time_key and ids:
            time_key = ids[-1]
        tpos = ids.index(time_key)

        # Build time positions -> labels
        tcat = dims.get(time_key, {}).get("category", {})
        tidx = tcat.get("index", {})  # {'2024-Q1': 0, ...}
        tlab = tcat.get("label", {})
        if not tidx:
            return {}
        maxpos = max(int(v) for v in tidx.values())
        time_labels = [None] * (maxpos + 1)
        for code, pos in tidx.items():
            time_labels[int(pos)] = tlab.get(code, code)

        # Compute strides for flat array
        strides = [1] * len(ids)
        running = 1
        for i in range(len(ids)-1, -1, -1):
            strides[i] = running
            running *= int(sizes[i] or 1)

        values = js.get("value")
        if isinstance(values, dict):
            dense = [None] * running
            for k, v in values.items():
                dense[int(k)] = v
            values = dense
        if not isinstance(values, list):
            return {}

        series: Dict[str, float] = {}
        tstride = strides[tpos]
        for t, label in enumerate(time_labels):
            if label is None:
                continue
            idx = t * tstride  # other dims at index 0
            if idx >= len(values):
                continue
            v = values[idx]
            if v is None:
                continue
            if _is_num(v):
                series[str(label)] = float(v)
        return series
    except Exception as e:
        print(f"[Eurostat] parse failed: {e}")
        return {}


EU_ISO3 = {
    "AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC","HUN","IRL","ITA","LVA","LTU","LUX","MLT",
    "NLD","POL","PRT","ROU","SVK","SVN","ESP","SWE","ISL","NOR","LIE","CHE","GBR"
}

@lru_cache(maxsize=256)
def fetch_eurostat_jsonstat(dataset: str, **filters) -> Optional[dict]:
    try:
        qs = "&".join(f"{k}={v}" for k, v in filters.items() if v is not None)
        url = f"{EUROSTAT_BASE}/{dataset}?{qs}" if qs else f"{EUROSTAT_BASE}/{dataset}"
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[Eurostat] fetch failed {dataset} {filters}: {e}")
        return None

def _es_pick_first(dim: dict, preferred: list[str]) -> Optional[str]:
    cat = (dim or {}).get("category", {})
    idx = cat.get("index", {})
    if not idx:
        return None
    codes = set(idx.keys())
    for cand in preferred:
        if cand in codes:
            return cand
    try:
        rev = {v: k for k, v in idx.items()}
        return rev[min(rev)]
    except Exception:
        return next(iter(codes))

def _es_filter_params(js: dict, wants: dict[str, list[str]]) -> dict[str, str]:
    dims = js.get("dimension", {})
    time_dim = None
    for k, v in dims.items():
        if isinstance(v, dict) and (v.get("role") == "time" or k.lower() == "time"):
            time_dim = k
            break
    out = {}
    for dname, dim in dims.items():
        if dname in ("id", "size") or dname == time_dim:
            continue
        prefs = wants.get(dname, [])
        choice = _es_pick_first(dim, prefs)
        if choice:
            out[dname] = choice
    return out

def parse_jsonstat_to_series(js: dict) -> Dict[str, float]:
    try:
        dims = js.get("dimension", {})
        time_key = None
        for k, v in dims.items():
            if isinstance(v, dict) and (v.get("role") == "time" or k.lower() == "time"):
                time_key = k
                break
        if not time_key:
            keys = [k for k in dims.keys() if k not in ("id", "size")]
            if keys:
                time_key = keys[-1]

        tcat = dims.get(time_key, {}).get("category", {})
        tidx = tcat.get("index", {})
        tlab = tcat.get("label", {})
        idx_to_period = {int(i): tlab.get(code, code) for code, i in tidx.items()}

        values = js.get("value")
        if not isinstance(values, list):
            if isinstance(values, dict):
                dense = []
                for i in range(len(idx_to_period)):
                    dense.append(values.get(str(i)))
                values = dense
            else:
                return {}

        ids = js.get("id") or [k for k in dims.keys() if k not in ("id", "size")]
        if time_key in ids:
            step = 1
            for d in ids:
                if d == time_key:
                    continue
                sz = dims.get(d, {}).get("size") if isinstance(dims.get(d), dict) else None
                step *= int(sz or 1)
        else:
            step = 1

        series: Dict[str, float] = {}
        for i, v in enumerate(values[0::step]):
            if v is None:
                continue
            if i in idx_to_period and _is_num(v):
                series[str(idx_to_period[i])] = float(v)
        return series
    except Exception as e:
        print(f"[Eurostat] parse failed: {e}")
        return {}

def _is_num(x):
    try:
        float(x)
        return True
    except Exception:
        return False


@lru_cache(maxsize=256)
def eurostat_debt_to_gdp_annual(iso2: str) -> Dict[str, float]:
    """Annual General Government Debt-to-GDP (%) from Eurostat (gov_10dd_edpt1).
    Filters: unit=PC_GDP, sector=S13. Returns {YYYY: pct}.
    """
    try:
        js = fetch_eurostat_jsonstat("gov_10dd_edpt1", geo=iso2, unit="PC_GDP", sector="S13")
        if not js:
            return {}
        return parse_jsonstat_to_series(js)
    except Exception as e:
        print(f"[Eurostat] ratio annual failed {iso2}: {e}")
        return {}

def eurostat_debt_gdp_quarterly(geo_code: str) -> Optional[dict]:
    def normalize_period(p):
        return p.replace(" ", "").replace("_", "").replace("-", "").upper()
    try:
        geo = geo_code

        # GDP: current prices, national currency, million (quarterly)
        gdp_js = fetch_eurostat_jsonstat("namq_10_gdp", geo=geo, na_item="B1GQ", unit="CP_MNAC")
        if not gdp_js:
            print(f"[Eurostat][{geo}] No GDP JSONStat returned")
            return None
        gdp_series_raw = parse_jsonstat_to_series(gdp_js)
        gdp_series = {normalize_period(k): v for k, v in gdp_series_raw.items()}

        # ---- 1. Try quarterly government debt ----
        debt_series_raw = None
        debt_source = None
        debt_path = None
        queries = [
            {"unit": "MIO_NAC", "sector": "S13", "consol": "CONS"},
            {"unit": "MIO_EUR", "sector": "S13", "consol": "CONS"},
            {"sector": "S13", "consol": "CONS"},
            {"unit": "MIO_NAC", "sector": "S13"},
            {"unit": "MIO_EUR", "sector": "S13"},
            {"sector": "S13"},
            {},
        ]
        for q in queries:
            try:
                print(f"[Eurostat][{geo}] Trying gov_10q_ggdebt with: {', '.join([f'{k}={v}' for k, v in q.items()]) or 'no filters'}")
                debt_js = fetch_eurostat_jsonstat("gov_10q_ggdebt", geo=geo, **q)
                if debt_js:
                    debt_series_raw = parse_jsonstat_to_series(debt_js)
                    if debt_series_raw:
                        debt_source = "Eurostat"
                        debt_path = "EUROSTAT_Q"
                        break
            except Exception as e:
                print(f"[Eurostat][{geo}] Debt fetch failed for {q}: {e}")

        # ---- 2. If quarterly fails, try annual government debt ----
        if not debt_series_raw:
            print(f"[Eurostat][{geo}] Trying annual government debt dataset gov_10dd_edpt1 as fallback.")
            try:
                annual_debt_js = fetch_eurostat_jsonstat("gov_10dd_edpt1", geo=geo, unit="MIO_EUR", sector="S13")
                if annual_debt_js:
                    debt_series_raw = parse_jsonstat_to_series(annual_debt_js)
                    if debt_series_raw:
                        debt_source = "Eurostat (annual)"
                        debt_path = "EUROSTAT_ANNUAL"
            except Exception as e:
                print(f"[Eurostat][{geo}] Annual debt fetch failed: {e}")

        if not debt_series_raw:
            print(f"[Eurostat][{geo}] No Debt series found after all attempts.")
            return None

        debt_series = {normalize_period(k): v for k, v in debt_series_raw.items()}
        commons = sorted(set(gdp_series) & set(debt_series))
        print(f"[Eurostat][{geo}] GDP periods (norm): {list(gdp_series.keys())}")
        print(f"[Eurostat][{geo}] Debt periods (norm): {list(debt_series.keys())}")
        print(f"[Eurostat][{geo}] Overlap periods: {commons}")

        if not commons:
            print(f"[Eurostat][{geo}] No overlap between GDP and Debt periods after normalization.")
            return None

        period = commons[-1]
        debt_v = float(debt_series[period])
        gdp_v = float(gdp_series[period])
        if gdp_v == 0:
            print(f"[Eurostat][{geo}] GDP value for period {period} is zero.")
            return None

        output_period = next((k for k in gdp_series_raw if normalize_period(k) == period), period)

        return {
            "debt_value": debt_v,
            "gdp_value": gdp_v,
            "period": output_period,
            "debt_to_gdp": round((debt_v / gdp_v) * 100, 2),
            "source": debt_source,
            "government_type": "General Government",
            "currency": "LCU",
            "path_used": debt_path,
            "government_debt_series": dict(sorted(debt_series_raw.items(), key=lambda x: x[0], reverse=True)),
            "nominal_gdp_series": dict(sorted(gdp_series_raw.items(), key=lambda x: x[0], reverse=True)),
        }
    except Exception as e:
        print(f"[Eurostat] trio failed: {e}")
        return None



def parse_jsonstat_to_series(js: dict) -> dict:
    """
    Robust JSON-stat 2.0 flattener.
    Returns {period: value} for a single "slice" across non-time dimensions (index 0 for each).
    Time axis is taken according to the dataset's "id"/"size" order.
    """
    try:
        dims = js.get("dimension", {}) or {}
        ids = js.get("id") or [k for k in dims.keys() if k not in ("id","size")]
        sizes = js.get("size")
        if sizes is None:
            sizes = []
            for d in ids:
                sz = dims.get(d, {}).get("size") if isinstance(dims.get(d), dict) else None
                sizes.append(int(sz or 1))

        # Find the time dimension key
        time_key = None
        for k, v in dims.items():
            if isinstance(v, dict) and (v.get("role") == "time" or k.lower() == "time"):
                time_key = k
                break
        if time_key is None and ids:
            time_key = ids[-1]  # fallback: last dim

        # Build time index->label mapping
        tcat = dims.get(time_key, {}).get("category", {})
        tidx = tcat.get("index", {})  # {"2024-Q1": 0, ...}
        tlab = tcat.get("label", {})
        idx_to_period = {}
        for code, i in tidx.items():
            try:
                ii = int(i)
            except Exception:
                continue
            idx_to_period[ii] = tlab.get(code, code)

        # Normalize values to dense list
        values = js.get("value", [])
        total_size = 1
        for s in sizes:
            total_size *= int(s or 1)
        if isinstance(values, dict):
            dense = [None] * total_size
            for k, v in values.items():
                try:
                    pos = int(k)
                except Exception:
                    continue
                if 0 <= pos < total_size:
                    dense[pos] = v
            values = dense
        elif not isinstance(values, list):
            return {}

        # Compute stride for each dimension (row-major with ids order)
        # index = sum(coord[d] * stride[d])
        strides = []
        acc = 1
        for s in reversed(sizes[1:] + [1]):  # build strides from the right
            strides.insert(0, acc)
            acc *= s if isinstance(s, int) else int(s or 1)
        # Ensure strides length matches ids length
        if len(strides) != len(ids):
            strides = [0]*len(ids)
            acc = 1
            for i in range(len(ids)-1, -1, -1):
                strides[i] = acc
                acc *= int(sizes[i] or 1)

        # Coordinates: choose 0 for all non-time dims
        coords = [0]*len(ids)
        try:
            tpos = ids.index(time_key)
        except ValueError:
            tpos = len(ids)-1  # fallback
        tsize = int(sizes[tpos] or 0)

        # For dims AFTER time, coords already 0; For dims BEFORE time, coords also 0.
        # Offset for the fixed "slice":
        offset = 0  # all other coords are 0

        # Build series by walking along time axis
        series = {}
        for t in range(tsize):
            coords[tpos] = t
            # compute flat index
            flat = offset
            for i, c in enumerate(coords):
                flat += int(c) * int(strides[i])
            if flat < 0 or flat >= len(values):
                continue
            v = values[flat]
            if v is None:
                continue
            if not isinstance(v, (int, float)):
                try:
                    v = float(v)
                except Exception:
                    continue
            # map time index -> period label
            period = idx_to_period.get(t, str(t))
            series[str(period)] = float(v)
        return series
    except Exception as e:
        print(f"[Eurostat] parse failed: {e}")
        return {}
