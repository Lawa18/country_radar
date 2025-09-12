# app/routes/country.py
from __future__ import annotations

from typing import Any, Dict, Tuple, List
from enum import Enum
from fastapi import APIRouter, Query, Response, HTTPException

from app.services.indicator_service import build_country_payload

router = APIRouter(tags=["country"])

class SeriesMode(str, Enum):
    none = "none"
    mini = "mini"
    full = "full"

def _parse_period_key(k: str) -> Tuple[int, int, int]:
    """
    Normalize period keys so we can sort generically:
      - 'YYYY-MM' -> (YYYY, MM, 0)
      - 'YYYYMmm' -> (YYYY, MM, 0)
      - 'YYYY-Qn' or 'YYYYQn' -> (YYYY, 10+n, 0)  (quarters sort after months)
      - 'YYYY' -> (YYYY, 0, 0)
      - fallback: place at beginning
    """
    try:
        s = (k or "").strip().upper()
        # YYYY-MM
        if len(s) == 7 and s[4] == "-":
            y = int(s[:4]); m = int(s[5:7]); return (y, m, 0)
        # YYYYMmm
        if len(s) == 6 and s[4] == "M":
            y = int(s[:4]); m = int(s[5:6] + s[6:]); return (y, m, 0)
        # YYYY-Qn / YYYYQn
        if "Q" in s:
            if "-" in s:
                y, q = s.split("-Q")
            else:
                y, q = s.split("Q")
            return (int(y), 10 + int(q), 0)
        # YYYY
        if len(s) == 4 and s.isdigit():
            return (int(s), 0, 0)
        # best-effort: try to pull year and month numbers
        digits = [int(x) for x in "".join(ch if ch.isdigit() else " " for ch in s).split()]
        if len(digits) >= 2:
            y, m = digits[0], digits[1]
            return (y, m, 0)
        if len(digits) == 1:
            return (digits[0], 0, 0)
    except Exception:
        pass
    return (0, 0, 0)

def _shrink_series_map(series_by_source: Dict[str, Dict[str, Any]], keep: int) -> Dict[str, Dict[str, Any]]:
    """
    Given {"IMF": {period->value}, "Eurostat": {...}}, keep the last N points per source.
    Works for monthly, quarterly, or annual period keys.
    """
    out: Dict[str, Dict[str, Any]] = {}
    for src, ser in (series_by_source or {}).items():
        if not isinstance(ser, dict) or not ser:
            continue
        keys = sorted(ser.keys(), key=_parse_period_key)
        recent = keys[-keep:]
        out[src] = {k: ser[k] for k in recent if k in ser}
    return out

@router.get(
    "/country-data",
    summary="Country macro indicators bundle",
    response_description="Compact bundle of macro indicators for a given country.",
)
def country_data(
    country: str = Query(
        ...,
        min_length=2,
        description=(
            "Full country name (e.g., 'Germany', 'United States', 'Sweden'). "
            "Common aliases are accepted (e.g., 'USA')."
        ),
        examples={
            "germany": {"summary": "Germany", "value": "Germany"},
            "usa": {"summary": "United States (alias accepted)", "value": "USA"},
            "sweden": {"summary": "Sweden", "value": "Sweden"},
        },
    ),
    series: SeriesMode = Query(
        default=SeriesMode.mini,
        description="How much time series to include: 'none' (only latest), 'mini' (last N points), or 'full' (all).",
    ),
    keep: int = Query(
        default=36, ge=1, le=240,
        description="If series='mini', keep this many most-recent observations per series (default 36).",
    ),
) -> Dict[str, Any]:
    """
    Returns CPI (YoY), unemployment, FX vs USD, reserves (USD), policy rate, GDP growth,
    CAB %GDP, government effectiveness, and the debt block. IMF/Eurostat monthly win
    over WB; ECB overrides policy for euro area. Use 'series' and 'keep' to control payload size.
    """
    name = country.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Country must be provided.")

    payload = build_country_payload(name)
    if isinstance(payload, dict) and payload.get("error"):
        raise HTTPException(status_code=400, detail=str(payload["error"]))

    # Optionally shrink series to make responses connector-friendly
    if series != SeriesMode.full and isinstance(payload, dict):
        inds = payload.get("indicators") or {}
        for key, block in list(inds.items()):
            if not isinstance(block, dict):
                continue
            if series == SeriesMode.none:
                block["series"] = {}
            else:
                # mini: keep last N per source
                ser_map = block.get("series") or {}
                block["series"] = _shrink_series_map(ser_map, keep)

        # Debt series as well (if present)
        debt = payload.get("debt")
        if isinstance(debt, dict):
            ser_map = debt.get("series")
            if isinstance(ser_map, dict):
                if series == SeriesMode.none:
                    debt["series"] = {}
                else:
                    debt["series"] = _shrink_series_map({"debt": ser_map}, keep).get("debt", {})

    return payload

# Keep HEAD for warmups, but hide from OpenAPI to avoid connector confusion
@router.head(
    "/country-data",
    include_in_schema=False,
    summary="(hidden) HEAD for /country-data",
)
def country_data_head(
    country: str = Query(
        ...,
        min_length=2,
        description="Same as GET /country-data; used to pre-warm cache without a response body.",
    )
) -> Response:
    _ = build_country_payload(country.strip())
    return Response(status_code=200)
