# app/routes/country.py
from __future__ import annotations

from typing import Any, Dict, Tuple
from enum import Enum
from copy import deepcopy

from fastapi import APIRouter, Query, Response, HTTPException
from app.services.indicator_service import build_country_payload

router = APIRouter(tags=["country"])

class SeriesMode(str, Enum):
    none = "none"   # only latest fields, no series (default — connector friendly)
    mini = "mini"   # keep last N per source
    full = "full"   # keep everything from the service

def _parse_period_key(k: str) -> Tuple[int, int, int]:
    """
    Normalize period keys so we can sort generically:
      - 'YYYY-MM'         -> (YYYY, MM, 0)
      - 'YYYYMmm'         -> (YYYY, MM, 0)
      - 'YYYY-Qn'/YYYYQn  -> (YYYY, 10+n, 0)  (quarters after months)
      - 'YYYY'            -> (YYYY, 0, 0)
    Falls back to extracting digits best-effort.
    """
    try:
        s = (k or "").strip().upper()
        # YYYY-MM
        if len(s) == 7 and s[4] == "-":
            return (int(s[:4]), int(s[5:7]), 0)
        # YYYYMmm
        if len(s) == 6 and s[4] == "M":
            return (int(s[:4]), int(s[5:7]), 0)
        # YYYY-Qn / YYYYQn
        if "Q" in s:
            if "-Q" in s:
                y, q = s.split("-Q")
            else:
                y, q = s.split("Q")
            return (int(y), 10 + int(q), 0)
        # YYYY
        if len(s) == 4 and s.isdigit():
            return (int(s), 0, 0)
        # best-effort: pull digits
        digits = [int(x) for x in "".join(ch if ch.isdigit() else " " for ch in s).split()]
        if len(digits) >= 2:
            return (digits[0], digits[1], 0)
        if len(digits) == 1:
            return (digits[0], 0, 0)
    except Exception:
        pass
    return (0, 0, 0)

def _shrink_series_map(series_by_source: Dict[str, Dict[str, Any]], keep: int) -> Dict[str, Dict[str, Any]]:
    """
    Given {"IMF": {period->value}, "Eurostat": {...}}, keep the last N points per source.
    Works for monthly, quarterly, or annual keys.
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
        description="Full country name (e.g., 'Germany', 'United States', 'Sweden'). Aliases like 'USA' are accepted.",
    ),
    series: SeriesMode = Query(
        default=SeriesMode.none,   # default tiny for Actions
        description="How much time series to include: 'none' (only latest), 'mini' (last N), or 'full' (all).",
    ),
    keep: int = Query(
        default=36, ge=1, le=240,
        description="If series='mini', keep this many most-recent observations per series.",
    ),
) -> Dict[str, Any]:
    """
    Returns CPI (YoY), unemployment, FX vs USD, reserves (USD), policy rate,
    GDP growth, CAB %GDP, gov effectiveness, and the debt block.
    Monthly IMF/Eurostat win over WB; ECB overrides policy for euro area.

    IMPORTANT: We deep-copy before trimming so we don't mutate the service cache.
    """
    name = country.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Country must be provided.")

    payload = build_country_payload(name)
    if isinstance(payload, dict) and payload.get("error"):
        raise HTTPException(status_code=400, detail=str(payload["error"]))

    if series == SeriesMode.full:
        return payload  # return as-is (do NOT mutate cached object)

    # Work on a deep copy so we don't modify the cached object in indicator_service
    resp = deepcopy(payload)

    if isinstance(resp, dict):
        inds = resp.get("indicators") or {}
        for _, block in list(inds.items()):
            if not isinstance(block, dict):
                continue
            if series == SeriesMode.none:
                block["series"] = {}
            else:
                ser_map = block.get("series") or {}
                block["series"] = _shrink_series_map(ser_map, keep)

        # Debt series too
        debt = resp.get("debt")
        if isinstance(debt, dict):
            ser_map = debt.get("series")
            if isinstance(ser_map, dict):
                if series == SeriesMode.none:
                    debt["series"] = {}
                else:
                    debt["series"] = _shrink_series_map({"debt": ser_map}, keep).get("debt", {})

    return resp

# Keep HEAD for warmups, but hide from OpenAPI to avoid connector confusion
@router.head(
    "/country-data",
    include_in_schema=False,
    summary="(hidden) HEAD for /country-data",
)
def country_data_head(
    country: str = Query(..., min_length=2, description="Same as GET /country-data; pre-warms cache.")
) -> Response:
    _ = build_country_payload(country.strip())
    return Response(status_code=200)
