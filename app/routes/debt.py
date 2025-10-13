# app/routes/debt.py — merged: native builder (IMF→WB) + legacy service fallback
from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional, Tuple

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse

router = APIRouter()

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _parse_period_key(p: str) -> Tuple[int, int, int]:
    """Sort 'YYYY', 'YYYY-MM', 'YYYY-Qn' as (Y, M, Q)."""
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

def _coerce_numeric_dict(d: Optional[Mapping[str, Any]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not isinstance(d, Mapping):
        return out
    for k, v in d.items():
        try:
            out[str(k)] = float(v)
        except Exception:
            # skip non-numeric
            continue
    return out

def _to_annual(d: Mapping[str, float]) -> Dict[str, float]:
    """Collapse monthly/quarterly dict to annual by taking the latest period in each year."""
    if not d:
        return {}
    by_year: Dict[str, Tuple[str, float]] = {}
    for k, v in d.items():
        try:
            year = k.split("-")[0]
        except Exception:
            year = str(k)
        prev = by_year.get(year)
        if prev is None or _parse_period_key(k) > _parse_period_key(prev[0]):
            by_year[year] = (k, v)
    return {y: v for y, (_, v) in sorted(by_year.items(), key=lambda kv: int(kv[0]))}

def _latest(d: Mapping[str, float]) -> Tuple[Optional[str], Optional[float]]:
    if not d:
        return None, None
    ks = sorted(d.keys(), key=_parse_period_key)
    k = ks[-1]
    return k, d[k]

def _align_ratio(num: Mapping[str, float], den: Mapping[str, float]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for y, nv in num.items():
        dv = den.get(y)
        if dv in (None, 0):
            continue
        out[y] = (nv / dv) * 100.0
    return out

def _call_provider(module_name: str, candidates: Iterable[str], **kwargs) -> Tuple[Dict[str, float], Dict[str, Any]]:
    """
    Try multiple function names & kw variants; return (numeric_series, debug).
    Accepts dict-like returns; coerces to {period: float}.
    """
    dbg: Dict[str, Any] = {"module": module_name, "tried": []}
    try:
        mod = __import__(module_name, fromlist=["*"])  # type: ignore
    except Exception as e:
        dbg["error"] = f"import_failed: {e}"
        return {}, dbg

    kw_variants = [kwargs]
    if "country" in kwargs:  # common alias
        kv = dict(kwargs)
        kv["name"] = kv.pop("country")
        kw_variants.append(kv)

    for fn in candidates:
        f = getattr(mod, fn, None)
        if not callable(f):
            dbg["tried"].append({fn: "missing"})
            continue
        for kv in kw_variants:
            try:
                data = f(**kv)
                dbg["tried"].append({fn: {"ok": True}})
                return _coerce_numeric_dict(data), dbg
            except Exception as e:
                dbg["tried"].append({fn: {"error": str(e)}})
    return {}, dbg

def _pack(series: Mapping[str, float], source: Optional[str]) -> Dict[str, Any]:
    period, value = _latest(series)
    return {"latest": {"value": value, "date": period, "source": source}, "series": dict(series)}

# -----------------------------------------------------------------------------
# Public: native reusable builder
# -----------------------------------------------------------------------------

def compute_debt_payload(country: str) -> Dict[str, Any]:
    """
    Returns a dict with:
      - government_debt: {latest{value,date,source}, series{year:value}}
      - nominal_gdp:     {latest{value,date,source}, series{year:value}}
      - debt_to_gdp:     {latest{value,date,source}, series{year:value}}
      - debt_to_gdp_series: {year:value}
    IMF-first, WB fallback. If direct ratio is unavailable, computes from nominal debt & GDP.
    """
    debug: Dict[str, Any] = {}

    # 1) Direct debt/GDP ratio (prefer IMF)
    imf_ratio, dbg_imf_ratio = _call_provider(
        "app.providers.imf_provider",
        (
            "get_debt_to_gdp_annual", "debt_to_gdp_annual",
            "get_general_gov_debt_pct_gdp", "general_gov_debt_pct_gdp",
            "get_ggxwdg_ngdp_annual", "ggxwdg_ngdp_annual",
        ),
        country=country,
    )
    wb_ratio, dbg_wb_ratio = {}, {}
    if not imf_ratio:
        wb_ratio, dbg_wb_ratio = _call_provider(
            "app.providers.wb_provider",
            (
                "get_central_gov_debt_pct_gdp", "central_gov_debt_pct_gdp",
                "get_debt_to_gdp_annual", "debt_to_gdp_annual",
            ),
            country=country,
        )
    direct_ratio = _to_annual(imf_ratio or wb_ratio)
    direct_ratio_src = "IMF" if imf_ratio else ("WorldBank" if wb_ratio else None)

    # 2) Nominal debt & GDP (for computed ratio and display)
    imf_debt_nom, dbg_imf_debt = _call_provider(
        "app.providers.imf_provider",
        ("get_general_gov_debt_nominal", "general_gov_debt_nominal",
         "get_gov_debt_nominal", "gov_debt_nominal"),
        country=country,
    )
    wb_debt_nom, dbg_wb_debt = _call_provider(
        "app.providers.wb_provider",
        ("get_central_gov_debt_local", "central_gov_debt_local",
         "get_central_gov_debt_nominal", "central_gov_debt_nominal",
         "get_wb_gc_dod_totl_cn", "wb_gc_dod_totl_cn"),
        country=country,
    )
    imf_gdp_nom, dbg_imf_gdp = _call_provider(
        "app.providers.imf_provider",
        ("get_nominal_gdp", "nominal_gdp", "get_ngdp_annual", "ngdp_annual"),
        country=country,
    )
    wb_gdp_nom, dbg_wb_gdp = _call_provider(
        "app.providers.wb_provider",
        ("get_nominal_gdp", "nominal_gdp",
         "get_wb_nominal_gdp_cn", "wb_nominal_gdp_cn",
         "get_gdp_nominal_cn", "gdp_nominal_cn"),
        country=country,
    )

    debt_nominal = _to_annual(imf_debt_nom or wb_debt_nom)
    debt_nominal_src = "IMF" if imf_debt_nom else ("WorldBank" if wb_debt_nom else None)

    gdp_nominal = _to_annual(imf_gdp_nom or wb_gdp_nom)
    gdp_nominal_src = "IMF" if imf_gdp_nom else ("WorldBank" if wb_gdp_nom else None)

    # 3) Compute ratio if direct missing
    ratio_series = direct_ratio or _align_ratio(debt_nominal, gdp_nominal)
    ratio_src = direct_ratio_src or (f"computed:{debt_nominal_src or 'NA'}/{gdp_nominal_src or 'NA'}")

    out: Dict[str, Any] = {
        "government_debt": _pack(debt_nominal, debt_nominal_src),
        "nominal_gdp": _pack(gdp_nominal, gdp_nominal_src),
        "debt_to_gdp": _pack(ratio_series, ratio_src),
        "debt_to_gdp_series": dict(ratio_series),
        "_debug": {
            "ratio": {"imf": dbg_imf_ratio, "wb": dbg_wb_ratio},
            "debt_nominal": {"imf": dbg_imf_debt, "wb": dbg_wb_debt},
            "gdp_nominal": {"imf": dbg_imf_gdp, "wb": dbg_wb_gdp},
        },
    }
    return out

# -----------------------------------------------------------------------------
# Endpoint — native builder first; legacy service fallback
# -----------------------------------------------------------------------------

@router.get("/v1/debt", summary="Debt bundle (IMF→WB)", tags=["debt"])
def debt_bundle(
    country: str = Query(..., description="Full country name, e.g., Mexico"),
    debug: bool = Query(False, description="Include provider traces under _debug"),
) -> Dict[str, Any]:
    # 1) Try native builder
    try:
        result = compute_debt_payload(country=country)
        if not debug:
            result.pop("_debug", None)
        result.setdefault("ok", True)
        result.setdefault("country", country)
        return result
    except Exception as native_err:
        native_error = str(native_err)

    # 2) Fallback to legacy service behaviour (your current file’s behavior)
    try:
        from app.services import debt_service as ds  # lazy import
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to import app.services.debt_service: {e}")

    # Try a few likely function names for compatibility (preserve your old code intent)
    for name in (
        "compute_debt_payload",
        "build_debt_payload",
        "get_debt_payload",
        "debt_payload_for_country",
    ):
        fn = getattr(ds, name, None)
        if callable(fn):
            try:
                payload = fn(country)  # legacy may return ratio-only; that's OK
                return payload
            except Exception as e:
                raise HTTPException(status_code=502, detail=f"debt_service.{name} error: {e}")

    # If all else fails, surface the native error to help debugging
    raise HTTPException(
        status_code=500,
        detail=f"No supported debt payload function found in app.services.debt_service; and native builder failed: {native_error}",
    )
