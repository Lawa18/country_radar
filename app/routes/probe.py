# app/routes/probe.py — diagnostics & lite payload (additive, robust)
from __future__ import annotations

import os
import socket
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

router = APIRouter()

# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _parse_period_key(p: str) -> Tuple[int, int, int]:
    """Sort periods like 'YYYY', 'YYYY-MM', 'YYYY-Qn'. Returns (Y,M,Q)."""
    try:
        if "-Q" in p:
            y, q = p.split("-Q", 1)
            return (int(y), 0, int(q))
        if "-" in p:
            y, m = p.split("-", 1)
            return (int(y), int(m), 0)
        return (int(p), 0, 0)
    except Exception:
        return (0, 0, 0)


def _summary(series: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    if not isinstance(series, Mapping) or not series:
        return {"len": 0, "latest": None}
    keys = sorted(series.keys(), key=_parse_period_key)
    return {"len": len(keys), "latest": keys[-1]}


def _route_env_flag(name: str, default_true: bool = True) -> bool:
    v = os.getenv(name)
    if v is None:
        return default_true
    return str(v).lower() not in {"0", "false", "no", "off"}


def _host_resolves(host: str) -> bool:
    try:
        socket.getaddrinfo(host, 443)
        return True
    except socket.gaierror:
        return False


def _resolve_iso(country: str) -> Dict[str, Optional[str]]:
    """Best-effort ISO resolution without assuming exact util names."""
    result = {"name": country, "iso_alpha_2": None, "iso_alpha_3": None, "iso_numeric": None}
    try:
        from app.utils import country_codes as cc  # type: ignore
        for fn in ("resolve_country", "name_to_iso", "get_iso_codes", "lookup_country", "resolve"):
            f = getattr(cc, fn, None)
            if callable(f):
                try:
                    out = f(country)
                    if isinstance(out, Mapping):
                        result.update({
                            "name": out.get("name", country),
                            "iso_alpha_2": out.get("iso_alpha_2") or out.get("iso2"),
                            "iso_alpha_3": out.get("iso_alpha_3") or out.get("iso3"),
                            "iso_numeric": out.get("iso_numeric") or out.get("isonum"),
                        })
                        return result
                except Exception:
                    pass
    except Exception:
        pass
    return result


def _call_provider(module_name: str, candidates: Iterable[str], **kwargs) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """Try several function names & kwarg spellings; return (series, debug)."""
    dbg: Dict[str, Any] = {"module": module_name, "tried": []}
    try:
        mod = __import__(module_name, fromlist=["*"])  # type: ignore
    except Exception as e:
        dbg["error"] = f"import_failed: {e}"
        return None, dbg

    # Try several kw spellings commonly seen in this repo
    kw_variants = [kwargs]
    if "country" in kwargs:
        kw = dict(kwargs)
        kw["name"] = kw.pop("country")
        kw_variants.append(kw)
    if "iso2" in kwargs:
        kw = dict(kwargs)
        kw["code"] = kw.pop("iso2")
        kw_variants.append(kw)

    for fn in candidates:
        f = getattr(mod, fn, None)
        if not callable(f):
            dbg["tried"].append({fn: "missing"})
            continue
        for kv in kw_variants:
            t0 = time.time()
            try:
                series = f(**kv)
                dbg["tried"].append({fn: {"ok": True, "ms": round((time.time()-t0)*1000,1)}})
                if isinstance(series, Mapping):
                    return dict(series), dbg
                # allow providers that return full payloads
                if isinstance(series, (list, tuple)):
                    return {str(i): v for i, v in enumerate(series)}, dbg
                # as a last resort, wrap scalar
                return {"value": series}, dbg
            except Exception as e:
                dbg["tried"].append({fn: {"error": str(e)}})
    return None, dbg

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/__action_probe")
def action_probe(request: Request):
    return {
        "ok": True,
        "version": getattr(request.app, "version", None),
        "source": "app.routes.probe",
        "path": "/__action_probe",
    }


@router.get("/__probe_series", summary="Probe Series")
def probe_series(country: str = Query(..., description="Full country name, e.g., Germany")):
    iso = _resolve_iso(country)

    # IMF fetches
    imf_cpi, imf_cpi_dbg = _call_provider(
        "app.providers.imf_provider",
        ("get_cpi_yoy_monthly", "get_cpi_yoy", "cpi_yoy_monthly", "cpi_yoy"),
        country=country,
    )
    imf_ue, imf_ue_dbg = _call_provider(
        "app.providers.imf_provider",
        ("get_unemployment_rate_monthly", "get_unemployment_rate", "unemployment_rate_monthly", "unemployment_rate"),
        country=country,
    )
    imf_fx, imf_fx_dbg = _call_provider(
        "app.providers.imf_provider",
        ("get_fx_rate_usd", "fx_rate_usd"),
        country=country,
    )
    imf_res, imf_res_dbg = _call_provider(
        "app.providers.imf_provider",
        ("get_reserves_usd", "reserves_usd"),
        country=country,
    )
    imf_pol, imf_pol_dbg = _call_provider(
        "app.providers.imf_provider",
        ("get_policy_rate", "policy_rate"),
        country=country,
    )
    imf_gdpq, imf_gdpq_dbg = _call_provider(
        "app.providers.imf_provider",
        ("get_gdp_growth_quarterly", "gdp_growth_quarterly"),
        country=country,
    )

    # World Bank (annual fallbacks)
    wb_cpi, wb_cpi_dbg = _call_provider(
        "app.providers.wb_provider",
        ("get_cpi_inflation_annual", "cpi_inflation_annual"),
        country=country,
    )
    wb_ue, wb_ue_dbg = _call_provider(
        "app.providers.wb_provider",
        ("get_unemployment_annual", "unemployment_annual"),
        country=country,
    )
    wb_fx, wb_fx_dbg = _call_provider(
        "app.providers.wb_provider",
        ("get_fx_official_annual", "fx_official_annual"),
        country=country,
    )
    wb_res, wb_res_dbg = _call_provider(
        "app.providers.wb_provider",
        ("get_reserves_annual", "reserves_annual"),
        country=country,
    )
    wb_gdp, wb_gdp_dbg = _call_provider(
        "app.providers.wb_provider",
        ("get_gdp_growth_annual", "gdp_growth_annual"),
        country=country,
    )

    # Eurostat (optional + fail-fast)
    eurostat_enabled = _route_env_flag("EUROSTAT_ENABLED", True)
    eurostat_host = os.getenv("EUROSTAT_HOST", "data-api.ec.europa.eu")

    es_hicp = es_hicp_dbg = es_ue = es_ue_dbg = None, {}
    if eurostat_enabled and _host_resolves(eurostat_host):
        es_hicp, es_hicp_dbg = _call_provider(
            "app.providers.eurostat_provider",
            ("get_hicp_yoy_iso2", "hicp_yoy_iso2", "get_hicp_yoy"),
            iso2=iso.get("iso_alpha_2") or iso.get("iso2") or "",
        )
        es_ue, es_ue_dbg = _call_provider(
            "app.providers.eurostat_provider",
            ("get_unemployment_rate_iso2", "unemployment_rate_iso2", "get_unemployment_rate"),
            iso2=iso.get("iso_alpha_2") or iso.get("iso2") or "",
        )
    else:
        es_hicp_dbg = {"skipped": True, "reason": "disabled_or_dns"}
        es_ue_dbg = {"skipped": True, "reason": "disabled_or_dns"}

    out = {
        "ok": True,
        "country": country,
        "iso2": iso.get("iso_alpha_2"),
        "iso3": iso.get("iso_alpha_3"),
        "series": {
            "cpi": {
                "IMF": _summary(imf_cpi),
                "Eurostat": _summary(es_hicp),
                "WB_annual": _summary(wb_cpi),
            },
            "unemployment": {
                "IMF": _summary(imf_ue),
                "Eurostat": _summary(es_ue),
                "WB_annual": _summary(wb_ue),
            },
            "fx": {
                "IMF": _summary(imf_fx),
                "WB_annual": _summary(wb_fx),
            },
            "reserves": {
                "IMF": _summary(imf_res),
                "WB_annual": _summary(wb_res),
            },
            "policy_rate": {
                "IMF": _summary(imf_pol),
            },
            "gdp_growth": {
                "IMF_quarterly": _summary(imf_gdpq),
                "WB_annual": _summary(wb_gdp),
            },
        },
        "_debug": {
            "imf": {
                "cpi": imf_cpi_dbg,
                "unemployment": imf_ue_dbg,
                "fx": imf_fx_dbg,
                "reserves": imf_res_dbg,
                "policy_rate": imf_pol_dbg,
                "gdp_growth": imf_gdpq_dbg,
            },
            "wb": {
                "cpi": wb_cpi_dbg,
                "unemployment": wb_ue_dbg,
                "fx": wb_fx_dbg,
                "reserves": wb_res_dbg,
                "gdp_growth": wb_gdp_dbg,
            },
            "eurostat": {
                "hicp": es_hicp_dbg,
                "unemployment": es_ue_dbg,
                "enabled": eurostat_enabled,
                "host": eurostat_host,
            },
        },
    }
    return JSONResponse(out)


@router.get("/v1/country-lite")
def country_lite(country: str = Query(..., description="Full country name, e.g., Mexico")):
    iso = _resolve_iso(country)

    # Reuse the same calls as probe but only extract the latest values
    def _latest_val(series: Optional[Mapping[str, Any]]) -> Tuple[Optional[float], Optional[str]]:
        if not isinstance(series, Mapping) or not series:
            return None, None
        keys = sorted(series.keys(), key=_parse_period_key)
        k = keys[-1]
        try:
            v = float(series[k])
        except Exception:
            v = None
        return v, k

    imf_cpi, _ = _call_provider("app.providers.imf_provider", ("get_cpi_yoy_monthly", "get_cpi_yoy"), country=country)
    imf_ue, _ = _call_provider("app.providers.imf_provider", ("get_unemployment_rate_monthly", "get_unemployment_rate"), country=country)
    imf_fx, _ = _call_provider("app.providers.imf_provider", ("get_fx_rate_usd",), country=country)
    imf_res, _ = _call_provider("app.providers.imf_provider", ("get_reserves_usd",), country=country)
    imf_pol, _ = _call_provider("app.providers.imf_provider", ("get_policy_rate",), country=country)
    imf_gdpq, _ = _call_provider("app.providers.imf_provider", ("get_gdp_growth_quarterly",), country=country)

    # Governance & current account examples (WB)
    wb_cab, _ = _call_provider("app.providers.wb_provider", ("get_current_account_pct_gdp",), country=country)
    wb_ge, _ = _call_provider("app.providers.wb_provider", ("get_government_effectiveness",), country=country)

    # Assemble
    out: Dict[str, Any] = {
        "country": country,
        "iso_codes": iso,
        "imf_data": {},  # reserved for raw bundles if you add them later
        "latest": {},     # optional lite headline
        "series": {},     # keep your existing structure if you prefer
        "source": None,
        "additional_indicators": {},
    }

    # Example headline from WB debt ratio if present
    wb_debt_ratio, _ = _call_provider("app.providers.wb_provider", ("get_debt_ratio_annual", "debt_ratio_annual"), country=country)
    if isinstance(wb_debt_ratio, Mapping) and wb_debt_ratio:
        v, k = _latest_val(wb_debt_ratio)
        if k is not None:
            out["latest"] = {"year": k, "value": v, "source": "World Bank (ratio)"}
            out["series"] = {k: v} if v is not None else {}
            out["source"] = "World Bank (ratio)"

    # Additional indicators (latest only)
    for label, ser in (
        ("cpi_yoy", imf_cpi),
        ("unemployment_rate", imf_ue),
        ("fx_rate_usd", imf_fx),
        ("reserves_usd", imf_res),
        ("policy_rate", imf_pol),
        ("gdp_growth", imf_gdpq),
        ("current_account_balance_pct_gdp", wb_cab),
        ("government_effectiveness", wb_ge),
    ):
        v, k = _latest_val(ser)
        if k is not None:
            out["additional_indicators"][label] = {
                "latest_value": v,
                "latest_period": k,
                "source": "IMF" if label in {"cpi_yoy","unemployment_rate","fx_rate_usd","reserves_usd","policy_rate","gdp_growth"} else "WorldBank",
                "series": {},  # keep lite small
            }

    # Debt integration (reuse your debt helper if available)
    try:
        # Preferred: function inside routes.debt (commit mentioned compute_debt_payload)
        from app.routes.debt import compute_debt_payload  # type: ignore
        debt_payload = compute_debt_payload(country=country)
    except Exception:
        # Fallback: maybe services has it
        try:
            from app.services.indicator_service import compute_debt_payload  # type: ignore
            debt_payload = compute_debt_payload(country=country)
        except Exception as e:
            debt_payload = None
            out.setdefault("_debug", {})["debt_error"] = str(e)

    if isinstance(debt_payload, Mapping):
        for key in ("government_debt", "nominal_gdp", "debt_to_gdp", "debt_to_gdp_series"):
            if key in debt_payload:
                out[key] = debt_payload[key]

    return JSONResponse(out)
