# app/routes/country.py — robust /country-data (prefers v2; adapts lite; series/keep; _debug)
from __future__ import annotations

from typing import Any, Dict, Literal, Mapping, Optional, Tuple

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

router = APIRouter()


def _flex_call_builder(builder, country: str, series: str, keep: int):
    """
    Call builder with flexible signatures to support legacy functions.
    Tries (country=..., series=..., keep=...) then (country=...), then (country,).
    """
    try:
        return builder(country=country, series=series, keep=keep)
    except TypeError:
        try:
            return builder(country=country)
        except TypeError:
            return builder(country)


def _parse_period_key(p: str) -> Tuple[int, int, int]:
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


def _latest(series: Optional[Mapping[str, Any]]) -> Tuple[Optional[str], Optional[float]]:
    if not isinstance(series, Mapping) or not series:
        return None, None
    keys = sorted(series.keys(), key=_parse_period_key)
    k = keys[-1]
    try:
        v = float(series[k])
    except Exception:
        v = None
    return k, v


def _adapt_lite_to_indicators(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    If a legacy/lite payload is returned (with `additional_indicators`),
    synthesize a unified `indicators` block so consumers have a stable shape.
    Preserve original fields for backward compatibility.
    """
    if "indicators" in payload:
        return payload

    addl = payload.get("additional_indicators")
    if not isinstance(addl, Mapping) or not addl:
        return payload

    mapping = {
        "cpi_yoy": "cpi_yoy",
        "unemployment_rate": "unemployment_rate",
        "fx_rate_usd": "fx_rate_usd",
        "reserves_usd": "reserves_usd",
        "policy_rate": "policy_rate",
        "gdp_growth": "gdp_growth",
        "current_account_balance_pct_gdp": "current_account_balance_pct_gdp",
        "government_effectiveness": "government_effectiveness",
    }

    indicators: Dict[str, Any] = {}
    for k_lite, k_unified in mapping.items():
        entry = addl.get(k_lite)
        if not isinstance(entry, Mapping):
            continue
        latest_value = entry.get("latest_value")
        latest_period = entry.get("latest_period")
        source = entry.get("source")
        series = entry.get("series") if isinstance(entry.get("series"), Mapping) else {}

        indicators[k_unified] = {
            "series": series,
            "latest_period": latest_period,
            "latest_value": latest_value,
            "source": source,
            # try to infer freq; leave None if unknown
            "freq": "monthly" if isinstance(latest_period, str) and "-" in latest_period else ("quarterly" if isinstance(latest_period, str) and "-Q" in latest_period else None),
        }

    # If there was a WB debt ratio headline at the top, surface it as a synthetic indicator too
    if isinstance(payload.get("latest"), Mapping) and payload.get("source") == "World Bank (ratio)":
        lp = payload["latest"]
        indicators.setdefault("debt_to_gdp", {
            "series": payload.get("debt_to_gdp", {}).get("series", {}),
            "latest_period": lp.get("year"),
            "latest_value": lp.get("value"),
            "source": "WorldBank",
            "freq": "annual",
        })

    payload["indicators"] = indicators
    payload.setdefault("_debug", {}).setdefault("compat", {})["lite_adapted"] = True
    return payload


@router.get(
    "/country-data",
    tags=["country"],
    summary="Country Data",
    description="Returns the assembled country payload with indicators and debt.",
)
def country_data(
    country: str = Query(..., description="Full country name, e.g., Sweden"),
    series: Literal["none", "mini", "full"] = Query(
        "mini", description='Timeseries size (none = latest only, "mini" ~ ~5y, "full" = full history)'
    ),
    keep: int = Query(
        180, ge=1, le=3650, description="Keep N days of history (approx by freq)"
    ),
    debug: bool = Query(False, description="Include _debug info about builder + sources."),
) -> Dict[str, Any]:
    """
    Return the assembled country payload.

    Prefers modern builder `build_country_payload_v2`. Falls back to
    legacy `build_country_payload` if v2 is not available. If a legacy
    lite-shaped payload is returned, adapt it to expose `indicators{}`.
    """
    # Lazy import so import-time errors elsewhere don't break app startup
    try:
        from app.services import indicator_service as svc  # type: ignore
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"indicator_service import failed: {e}"},
        )

    builder = getattr(svc, "build_country_payload_v2", None) or getattr(
        svc, "build_country_payload", None
    )
    if not callable(builder):
        return {
            "ok": False,
            "error": "No builder found in indicator_service (expected build_country_payload_v2 or build_country_payload).",
            "_debug": {"module": getattr(svc, "__file__", None)},
        }

    try:
        payload = _flex_call_builder(builder, country=country, series=series, keep=keep)
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "_debug": {
                "builder": getattr(builder, "__name__", None),
                "module": getattr(builder, "__module__", None),
            },
        }

    # Normalize to dict
    if not isinstance(payload, dict):
        payload = {"result": payload}

    # If the builder returned the legacy lite shape, adapt it
    payload = _adapt_lite_to_indicators(payload)

    # If builder didn’t include debt, try to enrich
    if not any(k in payload for k in ("government_debt", "debt_to_gdp", "nominal_gdp")):
        debt_payload = None
        try:
            # Preferred: reuse the existing route helper if present
            from app.routes.debt import compute_debt_payload  # type: ignore
            debt_payload = compute_debt_payload(country=country)
        except Exception:
            try:
                # Fallback: service-level helper, if available
                from app.services.indicator_service import compute_debt_payload  # type: ignore
                debt_payload = compute_debt_payload(country=country)
            except Exception:
                debt_payload = None
        if isinstance(debt_payload, dict):
            for key in ("government_debt", "nominal_gdp", "debt_to_gdp", "debt_to_gdp_series"):
                if key in debt_payload:
                    payload[key] = debt_payload[key]

    if debug:
        dbg = payload.setdefault("_debug", {})
        try:
            mod = __import__(getattr(builder, "__module__", ""), fromlist=["*"])  # type: ignore
            file_path = getattr(mod, "__file__", None)
        except Exception:
            file_path = None
        dbg.setdefault(
            "builder",
            {
                "used": getattr(builder, "__name__", None),
                "module": getattr(builder, "__module__", None),
                "file": file_path,
                "series_arg": series,
                "keep_arg": keep,
            },
        )

    # Friendly top-level fields
    payload.setdefault("country", country)
    payload.setdefault("series_mode", series)
    payload.setdefault("keep_days", keep)
    return payload
