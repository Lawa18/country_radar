# === Append-only override: modern monthly-first builder under the legacy name ===
# This block leaves all existing code intact. It just provides a modern builder
# and aliases the legacy export name to it so existing routes keep working.

from typing import Dict, Any, Optional, Literal

def _cr_latest(d: Dict[str, float]) -> Optional[tuple[str, float]]:
    if not d:
        return None
    try:
        k = max(d.keys())  # works for YYYY-MM and YYYY-Qx lexicographically
        v = d.get(k)
        return (k, v) if v is not None else None
    except Exception:
        return None

def _cr_trim_series(d: Dict[str, float], keep: int) -> Dict[str, float]:
    if not isinstance(d, dict) or keep <= 0:
        return {}
    try:
        ks = sorted(d.keys())[-keep:]
        return {k: d[k] for k in ks if d[k] is not None}
    except Exception:
        return {}

def _cr_build_block(source: Optional[str], latest: Optional[tuple[str, float]], series: Dict[str, Dict[str, float]]) -> Dict[str, Any]:
    if latest:
        lp, lv = latest
        return {
            "latest_value": lv,
            "latest_period": lp,
            "source": source or "N/A",
            "series": series,
        }
    return {"latest_value": None, "latest_period": None, "source": "N/A", "series": series}

def build_country_payload_v2(country: str, series: Literal["none","mini","full"]="mini", keep: int=60) -> Dict[str, Any]:
    """
    Country Radar monthly-first builder (override).
    - Monthly-first for CPI, Unemployment, FX, Reserves (IMF → Eurostat (EU/EEA/UK) → WB annual)
    - Policy rate: ECB for euro-area else IMF (no WB fallback)
    - GDP growth: IMF quarterly → WB annual
    - Annual: CAB %GDP, Gov Effectiveness from WB
    - 'series' = none | mini | full. 'keep' trims history length.
    """
    # Lazy imports so this block is self-contained and safe
    from app.utils.country_codes import resolve_country_codes

    # Providers (best-effort; missing providers won't crash the whole build)
    try:
        from app.providers.imf_provider import (
            imf_cpi_yoy_monthly, imf_unemployment_rate_monthly,
            imf_fx_usd_monthly, imf_reserves_usd_monthly,
            imf_policy_rate_monthly, imf_gdp_growth_quarterly,
        )
    except Exception:
        imf_cpi_yoy_monthly = imf_unemployment_rate_monthly = None
        imf_fx_usd_monthly = imf_reserves_usd_monthly = None
        imf_policy_rate_monthly = imf_gdp_growth_quarterly = None

    try:
        from app.providers.eurostat_provider import (
            eurostat_hicp_yoy_monthly, eurostat_unemployment_rate_monthly,
        )
    except Exception:
        eurostat_hicp_yoy_monthly = eurostat_unemployment_rate_monthly = None

    try:
        from app.providers.ecb_provider import ecb_policy_rate_for_country
    except Exception:
        ecb_policy_rate_for_country = None

    try:
        from app.providers.wb_provider import (
            wb_cpi_yoy_annual, wb_unemployment_rate_annual, wb_fx_rate_usd_annual,
            wb_reserves_usd_annual, wb_gdp_growth_annual_pct,
            wb_current_account_balance_pct_gdp_annual, wb_government_effectiveness_annual,
        )
    except Exception:
        wb_cpi_yoy_annual = wb_unemployment_rate_annual = None
        wb_fx_rate_usd_annual = wb_reserves_usd_annual = None
        wb_gdp_growth_annual_pct = None
        wb_current_account_balance_pct_gdp_annual = wb_government_effectiveness_annual = None

    # Euro area ISO2 list (policy via ECB)
    EURO_AREA_ISO2 = {
        "AT","BE","CY","DE","EE","ES","FI","FR","GR","IE","IT","LT","LU","LV","MT","NL","PT","SI","SK"
    }
    # EU/EEA + UK (Eurostat where sensible)
    EU_EEA_UK_ISO2 = {
        "AT","BE","BG","HR","CY","CZ","DE","DK","EE","ES","FI","FR","GR","EL","HU","IE",
        "IT","LT","LU","LV","MT","NL","PL","PT","RO","SE","SI","SK","IS","NO","LI","GB"
    }

    codes = resolve_country_codes(country) or {}
    iso2, iso3 = (codes.get("iso_alpha_2"), codes.get("iso_alpha_3"))
    payload: Dict[str, Any] = {
        "country": country,
        "iso2": iso2,
        "iso3": iso3,
        "indicators": {},
    }

    # Helper for series inclusion
    include_series = series != "none"
    keep_n = keep if series == "full" else (keep if series == "mini" else 0)

    # CPI YoY
    cpi_src = None
    cpi_series = {}
    latest_cpi = None
    if imf_cpi_yoy_monthly:
        s = imf_cpi_yoy_monthly(iso2) or {}
        if s:
            cpi_src = "IMF"
            latest_cpi = _cr_latest(s)
            if include_series:
                cpi_series["IMF"] = _cr_trim_series(s, keep_n)
    if latest_cpi is None and (iso2 in EU_EEA_UK_ISO2) and eurostat_hicp_yoy_monthly:
        s = eurostat_hicp_yoy_monthly(iso2) or {}
        if s:
            cpi_src = "Eurostat"
            latest_cpi = _cr_latest(s)
            if include_series:
                cpi_series["Eurostat"] = _cr_trim_series(s, keep_n)
    if latest_cpi is None and wb_cpi_yoy_annual:
        s = wb_cpi_yoy_annual(iso3) or {}
        if s:
            cpi_src = "WorldBank"
            latest_cpi = _cr_latest(s)
            if include_series:
                cpi_series["WorldBank"] = _cr_trim_series(s, keep_n)
    payload["indicators"]["cpi_yoy"] = _cr_build_block(cpi_src, latest_cpi, cpi_series)

    # Unemployment
    u_src = None
    u_series = {}
    latest_u = None
    if imf_unemployment_rate_monthly:
        s = imf_unemployment_rate_monthly(iso2) or {}
        if s:
            u_src = "IMF"
            latest_u = _cr_latest(s)
            if include_series:
                u_series["IMF"] = _cr_trim_series(s, keep_n)
    if latest_u is None and (iso2 in EU_EEA_UK_ISO2) and eurostat_unemployment_rate_monthly:
        s = eurostat_unemployment_rate_monthly(iso2) or {}
        if s:
            u_src = "Eurostat"
            latest_u = _cr_latest(s)
            if include_series:
                u_series["Eurostat"] = _cr_trim_series(s, keep_n)
    if latest_u is None and wb_unemployment_rate_annual:
        s = wb_unemployment_rate_annual(iso3) or {}
        if s:
            u_src = "WorldBank"
            latest_u = _cr_latest(s)
            if include_series:
                u_series["WorldBank"] = _cr_trim_series(s, keep_n)
    payload["indicators"]["unemployment_rate"] = _cr_build_block(u_src, latest_u, u_series)

    # FX (LCU/USD)
    fx_src = None
    fx_series = {}
    latest_fx = None
    if imf_fx_usd_monthly:
        s = imf_fx_usd_monthly(iso2) or {}
        if s:
            fx_src = "IMF"
            latest_fx = _cr_latest(s)
            if include_series:
                fx_series["IMF"] = _cr_trim_series(s, keep_n)
    if latest_fx is None and wb_fx_rate_usd_annual:
        s = wb_fx_rate_usd_annual(iso3) or {}
        if s:
            fx_src = "WorldBank"
            latest_fx = _cr_latest(s)
            if include_series:
                fx_series["WorldBank"] = _cr_trim_series(s, keep_n)
    payload["indicators"]["fx_rate_usd"] = _cr_build_block(fx_src, latest_fx, fx_series)

    # Reserves (USD)
    r_src = None
    r_series = {}
    latest_r = None
    if imf_reserves_usd_monthly:
        s = imf_reserves_usd_monthly(iso2) or {}
        if s:
            r_src = "IMF"
            latest_r = _cr_latest(s)
            if include_series:
                r_series["IMF"] = _cr_trim_series(s, keep_n)
    if latest_r is None and wb_reserves_usd_annual:
        s = wb_reserves_usd_annual(iso3) or {}
        if s:
            r_src = "WorldBank"
            latest_r = _cr_latest(s)
            if include_series:
                r_series["WorldBank"] = _cr_trim_series(s, keep_n)
    payload["indicators"]["reserves_usd"] = _cr_build_block(r_src, latest_r, r_series)

    # Policy rate (ECB override for euro area → else IMF)
    p_src = None
    p_series = {}
    latest_p = None
    if iso2 in EURO_AREA_ISO2 and ecb_policy_rate_for_country:
        s = ecb_policy_rate_for_country(iso2) or {}
        if s:
            p_src = "ECB"
            latest_p = _cr_latest(s)
            if include_series:
                p_series["ECB"] = _cr_trim_series(s, keep_n)
    if latest_p is None and imf_policy_rate_monthly:
        s = imf_policy_rate_monthly(iso2) or {}
        if s:
            p_src = "IMF"
            latest_p = _cr_latest(s)
            if include_series:
                p_series["IMF"] = _cr_trim_series(s, keep_n)
    payload["indicators"]["policy_rate"] = _cr_build_block(p_src, latest_p, p_series)

    # GDP growth: IMF quarterly → WB annual
    g_src = None
    g_series = {}
    latest_g = None
    if imf_gdp_growth_quarterly:
        s = imf_gdp_growth_quarterly(iso2) or {}
        if s:
            g_src = "IMF"
            latest_g = _cr_latest(s)
            if include_series:
                g_series["IMF_quarterly"] = _cr_trim_series(s, keep_n)
    if latest_g is None and wb_gdp_growth_annual_pct:
        s = wb_gdp_growth_annual_pct(iso3) or {}
        if s:
            g_src = "WorldBank"
            latest_g = _cr_latest(s)
            if include_series:
                g_series["WB_annual"] = _cr_trim_series(s, keep_n)
    payload["indicators"]["gdp_growth"] = _cr_build_block(g_src, latest_g, g_series)

    # Annual stable: CAB %GDP, Gov Effectiveness (World Bank)
    cab_latest = gov_eff_latest = None
    cab_series = gov_eff_series = {}
    cab_src = gov_eff_src = None

    if wb_current_account_balance_pct_gdp_annual:
        s = wb_current_account_balance_pct_gdp_annual(iso3) or {}
        if s:
            cab_src = "WorldBank"
            cab_latest = _cr_latest(s)
            if include_series:
                cab_series["WorldBank"] = _cr_trim_series(s, keep_n)
    payload["indicators"]["current_account_balance_pct_gdp"] = _cr_build_block(cab_src, cab_latest, cab_series)

    if wb_government_effectiveness_annual:
        s = wb_government_effectiveness_annual(iso3) or {}
        if s:
            gov_eff_src = "WorldBank"
            gov_eff_latest = _cr_latest(s)
            if include_series:
                gov_eff_series["WorldBank"] = _cr_trim_series(s, keep_n)
    payload["indicators"]["government_effectiveness"] = _cr_build_block(gov_eff_src, gov_eff_latest, gov_eff_series)

    # Debt: try tiered service if available; otherwise leave minimal
    debt_block = {"latest_value": None, "latest_period": None, "source": "N/A", "series": {}, "latest": {"period": None, "value": None, "source": "N/A"}}
    try:
        from app.services import debt_service as _debt
        for name in (
            "get_debt_ratio_for_country",
            "debt_latest_for_country",
            "get_debt_for_country",
            "get_debt",
            "build_debt_block",
        ):
            f = getattr(_debt, name, None)
            if callable(f):
                try:
                    # Try common signatures
                    try:
                        d = f(iso2=iso2, iso3=iso3, country=country)  # type: ignore
                    except TypeError:
                        try:
                            d = f(country)  # type: ignore
                        except TypeError:
                            d = f(iso3 or iso2)  # type: ignore
                    if isinstance(d, dict) and d:
                        # Normalize shape: expect latest_value/period/source if possible
                        if "latest_value" in d or "latest" in d:
                            debt_block = d
                        elif "series" in d and isinstance(d["series"], dict) and d["series"]:
                            lp = max(d["series"].keys())
                            lv = d["series"][lp]
                            debt_block = {
                                "latest_value": lv,
                                "latest_period": lp,
                                "source": d.get("source") or "DebtService",
                                "series": {},
                                "latest": {"period": lp, "value": lv, "source": d.get("source") or "DebtService"},
                            }
                        break
                except Exception:
                    continue
    except Exception:
        pass
    payload["debt"] = debt_block

    return payload

# === Country Radar: append-only modern builder + legacy name override =========
# This block does not remove or edit earlier code. It *redefines* the export
# name `build_country_payload` at the end of the module so existing routes use
# the monthly-first logic automatically.

from typing import Dict, Any, Optional, Literal

def _cr_latest_key(d: Dict[str, float]) -> Optional[str]:
    if not d: 
        return None
    try:
        return max(d.keys())
    except Exception:
        return None

def _cr_latest(d: Dict[str, float]) -> Optional[tuple[str, float]]:
    k = _cr_latest_key(d)
    if k is None:
        return None
    v = d.get(k)
    return (k, v) if v is not None else None

def _cr_trim(d: Dict[str, float], keep: int) -> Dict[str, float]:
    if not isinstance(d, dict) or keep <= 0:
        return {}
    try:
        ks = sorted(d.keys())[-keep:]
        return {k: d[k] for k in ks if d[k] is not None}
    except Exception:
        return {}

def _cr_block(source: Optional[str], latest: Optional[tuple[str, float]], series: Dict[str, Dict[str, float]]) -> Dict[str, Any]:
    if latest:
        p, v = latest
        return {"latest_value": v, "latest_period": p, "source": source or "N/A", "series": series}
    return {"latest_value": None, "latest_period": None, "source": "N/A", "series": series}

def build_country_payload_modern(country: str, series: Literal["none","mini","full"]="mini", keep: int=60) -> Dict[str, Any]:
    # Lazy imports to avoid boot failures if a provider is missing
    from app.utils.country_codes import resolve_country_codes

    # Providers
    try:
        from app.providers.imf_provider import (
            imf_cpi_yoy_monthly, imf_unemployment_rate_monthly,
            imf_fx_usd_monthly, imf_reserves_usd_monthly,
            imf_policy_rate_monthly, imf_gdp_growth_quarterly,
        )
    except Exception:
        imf_cpi_yoy_monthly = imf_unemployment_rate_monthly = None
        imf_fx_usd_monthly = imf_reserves_usd_monthly = None
        imf_policy_rate_monthly = imf_gdp_growth_quarterly = None

    try:
        from app.providers.eurostat_provider import (
            eurostat_hicp_yoy_monthly, eurostat_unemployment_rate_monthly,
        )
    except Exception:
        eurostat_hicp_yoy_monthly = eurostat_unemployment_rate_monthly = None

    try:
        from app.providers.ecb_provider import ecb_policy_rate_for_country
    except Exception:
        ecb_policy_rate_for_country = None

    try:
        from app.providers.wb_provider import (
            wb_cpi_yoy_annual, wb_unemployment_rate_annual, wb_fx_rate_usd_annual,
            wb_reserves_usd_annual, wb_gdp_growth_annual_pct,
            wb_current_account_balance_pct_gdp_annual, wb_government_effectiveness_annual,
        )
    except Exception:
        wb_cpi_yoy_annual = wb_unemployment_rate_annual = None
        wb_fx_rate_usd_annual = wb_reserves_usd_annual = None
        wb_gdp_growth_annual_pct = None
        wb_current_account_balance_pct_gdp_annual = wb_government_effectiveness_annual = None

    EURO_AREA_ISO2 = {
        "AT","BE","CY","DE","EE","ES","FI","FR","GR","IE","IT","LT","LU","LV","MT","NL","PT","SI","SK"
    }
    EU_EEA_UK_ISO2 = {
        "AT","BE","BG","HR","CY","CZ","DE","DK","EE","ES","FI","FR","GR","EL","HU","IE",
        "IT","LT","LU","LV","MT","NL","PL","PT","RO","SE","SI","SK","IS","NO","LI","GB"
    }

    codes = resolve_country_codes(country) or {}
    iso2, iso3 = codes.get("iso_alpha_2"), codes.get("iso_alpha_3")
    include_series = series != "none"
    keep_n = keep if series == "full" else (keep if series == "mini" else 0)

    out: Dict[str, Any] = {"country": country, "iso2": iso2, "iso3": iso3, "indicators": {}}

    # CPI YoY: IMF monthly → Eurostat monthly (EU/EEA/UK) → WB annual
    cpi_src, cpi_series, cpi_latest = None, {}, None
    if imf_cpi_yoy_monthly:
        s = imf_cpi_yoy_monthly(iso2) or {}
        if s: cpi_src, cpi_latest = "IMF", _cr_latest(s); 
        if include_series and s: cpi_series["IMF"] = _cr_trim(s, keep_n)
    if cpi_latest is None and eurostat_hicp_yoy_monthly and iso2 in EU_EEA_UK_ISO2:
        s = eurostat_hicp_yoy_monthly(iso2) or {}
        if s: cpi_src, cpi_latest = "Eurostat", _cr_latest(s)
        if include_series and s: cpi_series["Eurostat"] = _cr_trim(s, keep_n)
    if cpi_latest is None and wb_cpi_yoy_annual:
        s = wb_cpi_yoy_annual(iso3) or {}
        if s: cpi_src, cpi_latest = "WorldBank", _cr_latest(s)
        if include_series and s: cpi_series["WorldBank"] = _cr_trim(s, keep_n)
    out["indicators"]["cpi_yoy"] = _cr_block(cpi_src, cpi_latest, cpi_series)

    # Unemployment: IMF monthly → Eurostat monthly (EU/EEA/UK) → WB annual
    u_src, u_series, u_latest = None, {}, None
    if imf_unemployment_rate_monthly:
        s = imf_unemployment_rate_monthly(iso2) or {}
        if s: u_src, u_latest = "IMF", _cr_latest(s)
        if include_series and s: u_series["IMF"] = _cr_trim(s, keep_n)
    if u_latest is None and eurostat_unemployment_rate_monthly and iso2 in EU_EEA_UK_ISO2:
        s = eurostat_unemployment_rate_monthly(iso2) or {}
        if s: u_src, u_latest = "Eurostat", _cr_latest(s)
        if include_series and s: u_series["Eurostat"] = _cr_trim(s, keep_n)
    if u_latest is None and wb_unemployment_rate_annual:
        s = wb_unemployment_rate_annual(iso3) or {}
        if s: u_src, u_latest = "WorldBank", _cr_latest(s)
        if include_series and s: u_series["WorldBank"] = _cr_trim(s, keep_n)
    out["indicators"]["unemployment_rate"] = _cr_block(u_src, u_latest, u_series)

    # FX (LCU/USD): IMF monthly → WB annual
    fx_src, fx_series, fx_latest = None, {}, None
    if imf_fx_usd_monthly:
        s = imf_fx_usd_monthly(iso2) or {}
        if s: fx_src, fx_latest = "IMF", _cr_latest(s)
        if include_series and s: fx_series["IMF"] = _cr_trim(s, keep_n)
    if fx_latest is None and wb_fx_rate_usd_annual:
        s = wb_fx_rate_usd_annual(iso3) or {}
        if s: fx_src, fx_latest = "WorldBank", _cr_latest(s)
        if include_series and s: fx_series["WorldBank"] = _cr_trim(s, keep_n)
    out["indicators"]["fx_rate_usd"] = _cr_block(fx_src, fx_latest, fx_series)

    # Reserves (USD): IMF monthly → WB annual
    r_src, r_series, r_latest = None, {}, None
    if imf_reserves_usd_monthly:
        s = imf_reserves_usd_monthly(iso2) or {}
        if s: r_src, r_latest = "IMF", _cr_latest(s)
        if include_series and s: r_series["IMF"] = _cr_trim(s, keep_n)
    if r_latest is None and wb_reserves_usd_annual:
        s = wb_reserves_usd_annual(iso3) or {}
        if s: r_src, r_latest = "WorldBank", _cr_latest(s)
        if include_series and s: r_series["WorldBank"] = _cr_trim(s, keep_n)
    out["indicators"]["reserves_usd"] = _cr_block(r_src, r_latest, r_series)

    # Policy rate: ECB for euro-area → IMF monthly (no WB fallback)
    p_src, p_series, p_latest = None, {}, None
    if iso2 in EURO_AREA_ISO2 and ecb_policy_rate_for_country:
        s = ecb_policy_rate_for_country(iso2) or {}
        if s: p_src, p_latest = "ECB", _cr_latest(s)
        if include_series and s: p_series["ECB"] = _cr_trim(s, keep_n)
    if p_latest is None and imf_policy_rate_monthly:
        s = imf_policy_rate_monthly(iso2) or {}
        if s: p_src, p_latest = "IMF", _cr_latest(s)
        if include_series and s: p_series["IMF"] = _cr_trim(s, keep_n)
    out["indicators"]["policy_rate"] = _cr_block(p_src, p_latest, p_series)

    # GDP growth: IMF quarterly → WB annual
    g_src, g_series, g_latest = None, {}, None
    if imf_gdp_growth_quarterly:
        s = imf_gdp_growth_quarterly(iso2) or {}
        if s: g_src, g_latest = "IMF", _cr_latest(s)
        if include_series and s: g_series["IMF_quarterly"] = _cr_trim(s, keep_n)
    if g_latest is None and wb_gdp_growth_annual_pct:
        s = wb_gdp_growth_annual_pct(iso3) or {}
        if s: g_src, g_latest = "WorldBank", _cr_latest(s)
        if include_series and s: g_series["WB_annual"] = _cr_trim(s, keep_n)
    out["indicators"]["gdp_growth"] = _cr_block(g_src, g_latest, g_series)

    # Annual stable: CAB %GDP, Government Effectiveness
    cab_src = gov_src = None
    cab_series = gov_series = {}
    cab_latest = gov_latest = None
    if wb_current_account_balance_pct_gdp_annual:
        s = wb_current_account_balance_pct_gdp_annual(iso3) or {}
        if s: cab_src, cab_latest = "WorldBank", _cr_latest(s)
        if include_series and s: cab_series["WorldBank"] = _cr_trim(s, keep_n)
    out["indicators"]["current_account_balance_pct_gdp"] = _cr_block(cab_src, cab_latest, cab_series)

    if wb_government_effectiveness_annual:
        s = wb_government_effectiveness_annual(iso3) or {}
        if s: gov_src, gov_latest = "WorldBank", _cr_latest(s)
        if include_series and s: gov_series["WorldBank"] = _cr_trim(s, keep_n)
    out["indicators"]["government_effectiveness"] = _cr_block(gov_src, gov_latest, gov_series)

    # Debt block: use your existing service if available
    debt_block = {"latest_value": None, "latest_period": None, "source": "N/A", "series": {}, "latest": {"period": None, "value": None, "source": "N/A"}}
    try:
        from app.services import debt_service as _debt
        for name in ("get_debt_ratio_for_country","debt_latest_for_country","get_debt_for_country","get_debt","build_debt_block"):
            f = getattr(_debt, name, None)
            if callable(f):
                try:
                    try:
                        d = f(iso2=iso2, iso3=iso3, country=country)  # type: ignore
                    except TypeError:
                        try:
                            d = f(country)  # type: ignore
                        except TypeError:
                            d = f(iso3 or iso2)  # type: ignore
                    if isinstance(d, dict) and d:
                        debt_block = d
                        break
                except Exception:
                    continue
    except Exception:
        pass
    out["debt"] = debt_block

    return out

# Keep the old public name but point it to the modern builder (legacy signature preserved)
def build_country_payload(country: str) -> Dict[str, Any]:  # type: ignore[override]
    # Default to "mini" history and keep=60 for reliability
    return build_country_payload_modern(country, series="mini", keep=60)

# === End append-only override =================================================

# ===== Country Radar authoritative override (append-only) =====================
# Keep the public name stable for routes, but route it to the modern builder.
# This must be at the *very end* of the file so nothing else can override it.

from typing import Dict, Any

try:
    build_country_payload_modern  # type: ignore[name-defined]
except NameError:
    # Fallback in case the modern builder has a different name in your file;
    # Adjust the tuple below if you used another modern name.
    for _name in (
        "build_country_payload_modern",
        "build_country_payload_v2",
        "assemble_country_payload_modern",
        "assemble_country_payload",
    ):
        _cand = globals().get(_name)
        if callable(_cand):
            build_country_payload_modern = _cand  # type: ignore[assignment]
            break
    else:
        # As a last resort, keep the legacy implementation (already defined above)
        # but still expose it under a distinct name so we can see it in introspection.
        build_country_payload_modern = globals()["build_country_payload"]  # type: ignore[assignment]

def build_country_payload(country: str) -> Dict[str, Any]:  # legacy public API
    """
    Authoritative override: make the legacy name call the modern builder.
    We deliberately ignore 'series'/'keep' here so routes with a fixed signature work.
    """
    return build_country_payload_modern(country)  # type: ignore[misc]
# ==============================================================================
