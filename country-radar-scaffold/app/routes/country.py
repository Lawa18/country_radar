from fastapi import APIRouter
from app.services.indicator_service import build_country_payload

router = APIRouter()

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
        # IMPORTANT: publish as the UI key "Interest Rate (Policy)", but fetch IMF by its internal label "Interest Rate"
        "Interest Rate (Policy)": imf_series_block("Interest Rate", "FR.INR.RINR"),
        "Reserves (USD)": imf_series_block("Reserves (USD)", "FI.RES.TOTL.CD"),
        # (plus GDP Growth, Unemployment, CAB, Gov Effectiveness below as you already do)
    }
    
    # Normalize in case any earlier code used "Interest Rate" instead of "Interest Rate (Policy)"
    if "Interest Rate (Policy)" not in imf_data and "Interest Rate" in imf_data:
        imf_data["Interest Rate (Policy)"] = imf_data.pop("Interest Rate")
    
    # Euro area override from Eurostat MRO
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
        ir_block = imf_data.get("Interest Rate", {})
        latest = (ir_block or {}).get("latest") or {}
        if (latest.get("value") is None) and (iso2 in EURO_AREA_ISO2):
            ecb = fetch_ecb_policy_rate_series()
            if ecb:
                imf_data["Interest Rate (Policy)"] = ecb
    except Exception:
        pass

    @app.get("/debug/mro")
    def debug_mro():
        ser = eurostat_mro_annual() or {}
        latest_year = max(ser.keys()) if ser else None
        latest_val = ser.get(latest_year) if latest_year else None
        return {"ok": bool(ser), "count": len(ser), "latest": {"year": latest_year, "value": latest_val}}

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

        # Prefer the ratio series already selected by /v1/debt (Eurostat -> IMF -> WB)
    ratio_series_from_v1 = {}
    try:
        if isinstance(debt_bundle, dict):
            ratio_series_from_v1 = debt_bundle.get("debt_to_gdp_series") or {}
    except Exception:
        ratio_series_from_v1 = {}

    # Fallback series if v1/debt didn’t include one
    if not ratio_series_from_v1:
        # Use Eurostat annual % (GD) if available, else WB ratio series
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

    # Ensure currency_code is present when LCU/USD flagged
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

    # Build response (keeps your shapes the same)
    return {
        "country": country,
        "iso_codes": codes,
        "imf_data": imf_data,
        "government_debt": {"latest": gov_debt_latest, "series": {}},  # series optional/empty here
        "nominal_gdp":     {"latest": nom_gdp_latest, "series": {}},   # series optional/empty here
        "debt_to_gdp": {
            "latest": debt_pct_latest,           # whatever /v1/debt picked as latest
            "series": ratio_series_from_v1       # <- now Eurostat for DE (not WB 1990)
        },
        "additional_indicators": {}
    }


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

    print("[/country-data] IR(Policy).latest:", imf_data.get("Interest Rate (Policy)",{}).get("latest"))
    print("[/country-data] IR(Policy).years:", list((imf_data.get("Interest Rate (Policy)",{}).get("series") or {}).keys())[:8])

    return JSONResponse(content={
        "country": country,
        "iso_codes": codes,
        "imf_data": imf_data,
        "government_debt": government_debt_out,
        "nominal_gdp": nominal_gdp_out,
        "debt_to_gdp": debt_to_gdp_out,
        "additional_indicators": {}
    })

