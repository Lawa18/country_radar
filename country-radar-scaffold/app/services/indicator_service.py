# app/services/indicator_service.py
from app.providers.imf_provider import fetch_imf_sdmx_series
from app.providers.wb_provider import fetch_worldbank_data, wb_series, wb_entry
from app.providers.eurostat_provider import eurostat_debt_to_gdp_annual, eurostat_mro_annual, fetch_ecb_policy_rate_series
from app.services.debt_service import compute_debt_payload  # use service, not the route
from app.utils.country_utils import resolve_country_codes, resolve_currency_code, EURO_AREA_ISO2
from app.utils.series_utils import extract_latest_numeric_entry  # we'll tidy helpers later

def build_country_payload(country: str) -> dict:
    codes = resolve_country_codes(country)
    if not codes:
        return {"error": "Invalid country name", "country": country}

    iso2, iso3 = codes["iso_alpha_2"], codes["iso_alpha_3"]
    imf = fetch_imf_sdmx_series(iso2)
    wb  = fetch_worldbank_data(iso2, iso3)

    # ---- helper from your route (kept inline for now; we can move it later) ----
    def imf_series_block(label: str, wb_code: str):
        imf_block = None
        try:
            vals = imf.get(label, {})
            pairs = [(int(y), float(v)) for y, v in vals.items()
                     if isinstance(v, (float, int, str))
                     and str(v).replace('.', '', 1).replace('-', '', 1).isdigit()]
            if pairs:
                y, v = max(pairs, key=lambda x: x[0])
                imf_block = {
                    "latest": {"value": v, "date": str(y), "source": "IMF"},
                    "series": {str(yy): vv for yy, vv in sorted(pairs, reverse=True)}
                }
        except Exception:
            pass
        wb_block = wb_series(wb.get(wb_code))
        return imf_block or wb_block or {"latest": {"value": None, "date": None, "source": None}, "series": {}}

    # ---- IMF-first table (with WB fallback codes) ----
    imf_data = {
        "CPI": imf_series_block("CPI", "FP.CPI.TOTL.ZG"),
        "FX Rate": imf_series_block("FX Rate", "PA.NUS.FCRF"),
        # UI key is "Interest Rate (Policy)" but IMF label is "Interest Rate"
        "Interest Rate (Policy)": imf_series_block("Interest Rate", "FR.INR.RINR"),
        "Reserves (USD)": imf_series_block("Reserves (USD)", "FI.RES.TOTL.CD"),
    }

    # Normalize key if earlier code used "Interest Rate"
    if "Interest Rate (Policy)" not in imf_data and "Interest Rate" in imf_data:
        imf_data["Interest Rate (Policy)"] = imf_data.pop("Interest Rate")

    # ---- Euro area policy rate override (Eurostat/ECB) ----
    try:
        if iso2 in EURO_AREA_ISO2:
            mro = eurostat_mro_annual() or {}
            if mro:
                latest_year = max(int(y) for y in mro.keys())
                imf_data["Interest Rate (Policy)"] = {
                    "latest": {"value": mro[str(latest_year)], "date": str(latest_year), "source": "Eurostat (ECB MRO)"},
                    "series": mro
                }
    except Exception as e:
        print(f"[Eurostat ECB] override failed for {iso2}: {e}")

    try:
        ir_block = imf_data.get("Interest Rate (Policy)", {}) or imf_data.get("Interest Rate", {})
        latest = (ir_block or {}).get("latest") or {}
        if (latest.get("value") is None) and (iso2 in EURO_AREA_ISO2):
            ecb = fetch_ecb_policy_rate_series()
            if ecb:
                imf_data["Interest Rate (Policy)"] = ecb
    except Exception:
        pass

    # ---- GDP Growth (%) prefer IMF, fallback WB ----
    gdp_growth_imf = extract_latest_numeric_entry(imf.get("GDP Growth (%)", {}), "IMF")
    imf_data["GDP Growth (%)"] = gdp_growth_imf or wb_entry(wb.get("NY.GDP.MKTP.KD.ZG")) or {
        "value": None, "date": None, "source": None
    }

    # ---- Other WB indicators ----
    imf_data["Unemployment (%)"] = wb_entry(wb.get("SL.UEM.TOTL.ZS")) or {"value": None, "date": None, "source": None}
    imf_data["Current Account Balance (% of GDP)"] = wb_entry(wb.get("BN.CAB.XOKA.GD.ZS")) or {"value": None, "date": None, "source": None}
    imf_data["Government Effectiveness"] = wb_entry(wb.get("GE.EST")) or {"value": None, "date": None, "source": None}

    # ---- Debt/GDP blocks & series selection (reuse v1/debt service) ----
    wb_debt_ratio_hist = wb_series(wb.get("GC.DOD.TOTL.GD.ZS"))
    ratio_es = eurostat_debt_to_gdp_annual(iso2)

    debt_bundle = compute_debt_payload(country)  # call service, not route

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

    # Prefer series from v1/debt; fallback to Eurostat, else WB
    ratio_series_from_v1 = {}
    try:
        if isinstance(debt_bundle, dict):
            ratio_series_from_v1 = debt_bundle.get("debt_to_gdp_series") or {}
    except Exception:
        ratio_series_from_v1 = {}

    if not ratio_series_from_v1:
        try:
            es_ratio_series = ratio_es or {}
        except Exception:
            es_ratio_series = {}
        try:
            wb_ratio_series = wb_debt_ratio_hist.get("series") if wb_debt_ratio_hist else {}
            wb_ratio_series = wb_ratio_series or {}
        except Exception:
            wb_ratio_series = {}
        ratio_series_from_v1 = es_ratio_series or wb_ratio_series or {}

    # Ensure currency_code present when LCU/USD flagged
    try:
        if gov_debt_latest.get("currency") == "LCU" and not gov_debt_latest.get("currency_code"):
            gov_debt_latest["currency_code"] = resolve_currency_code(iso2)
        if nom_gdp_latest.get("currency") == "LCU" and not nom_gdp_latest.get("currency_code"):
            nom_gdp_latest["currency_code"] = resolve_currency_code(iso2)
        if gov_debt_latest.get("currency") == "USD" and not gov_debt_latest.get("currency_code"):
            gov_debt_latest["currency_code"] = "USD"
        if nom_gdp_latest.get("currency") == "USD" and not nom_gdp_latest.get("currency_code"):
            nom_gdp_latest["currency_code"] = "USD"
    except Exception:
        pass

    # --- Optional Eurostat full-history merge (if you have those series in debt_bundle later) ---
    # Skipped for now; we’ll revisit when we move helpers/series blocks.

    print("[/country-data] IR(Policy).latest:", (imf_data.get("Interest Rate (Policy)", {}) or {}).get("latest"))
    print("[/country-data] IR(Policy).years:", list((imf_data.get("Interest Rate (Policy)", {}).get("series") or {}).keys())[:8])

    return {
        "country": country,
        "iso_codes": codes,
        "imf_data": imf_data,
        "government_debt": {"latest": gov_debt_latest, "series": {}},
        "nominal_gdp":     {"latest": nom_gdp_latest, "series": {}},
        "debt_to_gdp": {
            "latest": debt_pct_latest,
            "series": ratio_series_from_v1
        },
        "additional_indicators": {}
    }

