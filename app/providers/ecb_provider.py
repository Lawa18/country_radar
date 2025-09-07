# app/providers/ecb_provider.py
from __future__ import annotations
from typing import Dict, Any, List, Tuple, Optional
import time
import httpx

# --- Euro area membership (ISO2) ---
EURO_AREA_ISO2 = {
    "AT", "BE", "HR", "CY", "EE", "FI", "FR", "DE", "IE", "IT",
    "LV", "LT", "LU", "MT", "NL", "PT", "SK", "SI", "ES", "GR",
}

ECB_TIMEOUT = 6.0
ECB_TTL_SECONDS = 1800  # 30 minutes cache TTL

# SDMX key: FM.M.U2.EUR.4F.KR.MRR_FR.LEV (Main Refinancing Operations - level, monthly)
# New host:
ECB_MRO_URL_NEW = (
    "https://data-api.ecb.europa.eu/service/data/FM/"
    "M.U2.EUR.4F.KR.MRR_FR.LEV?lastNObservations=120&format=sdmx-json"
)
# Old host (kept as a fallback — we enable redirects below, so either works):
ECB_MRO_URL_OLD = (
    "https://sdw-wsrest.ecb.europa.eu/service/data/FM/"
    "M.U2.EUR.4F.KR.MRR_FR.LEV?lastNObservations=120&format=sdmx-json"
)

_HEADERS = {
    "Accept": "application/vnd.sdmx.data+json;version=1.0",
    "User-Agent": "country-radar/1.0",
}

# --- small manual cache so we don't hammer ECB ---
_cache_series: Optional[Dict[str, float]] = None
_cache_at: float = 0.0


def _fetch_json(url: str) -> Dict[str, Any]:
    """
    Try the new host first, then the old one; follow redirects (the old host
    302s to the new host now). We keep both to be resilient.
    """
    last_exc: Optional[Exception] = None
    for attempt, u in enumerate((ECB_MRO_URL_NEW, ECB_MRO_URL_OLD), start=1):
        try:
            # follow_redirects=True is important because sdw-wsrest now 302s
            with httpx.Client(
                timeout=ECB_TIMEOUT, headers=_HEADERS, follow_redirects=True
            ) as client:
                r = client.get(u)
                r.raise_for_status()
                return r.json()
        except Exception as e:
            last_exc = e
            time.sleep(0.2 * attempt)  # tiny backoff and try next
    raise RuntimeError(f"ECB fetch failed: {last_exc}")


def _parse_sdmx_observations(j: Dict[str, Any]) -> List[Tuple[str, float]]:
    """
    Extract [(YYYY-MM, value), ...] from an ECB SDMX-JSON payload.
    """
    try:
        data_sets = j.get("dataSets") or []
        if not data_sets:
            return []
        series_dict = data_sets[0].get("series") or {}
        if not series_dict:
            return []
        # take first series (there is only one for this key)
        first_key = next(iter(series_dict.keys()))
        obs_map = series_dict[first_key].get("observations") or {}

        obs_dims = (j.get("structure") or {}).get("dimensions", {}).get("observation") or []
        if not obs_dims:
            return []
        time_values = obs_dims[0].get("values") or []  # [{'id': '2023-01'}, ...]

        out: List[Tuple[str, float]] = []
        for idx_str, arr in obs_map.items():
            try:
                idx = int(idx_str)
                date = (time_values[idx].get("id") or time_values[idx].get("name"))
                if not date:
                    continue
                val = arr[0] if isinstance(arr, list) and arr else None
                if val is None:
                    continue
                out.append((str(date), float(val)))
            except Exception:
                # skip any malformed point quietly
                continue

        out.sort(key=lambda x: x[0])
        return out
    except Exception:
        return []


def ecb_mro_series_monthly() -> Dict[str, float]:
    """
    Return {'YYYY-MM': value, ...} for the ECB main refinancing rate (MRO).
    Cached for 30 minutes to keep calls snappy on Render.
    """
    global _cache_series, _cache_at
    now = time.time()
    if _cache_series is not None and (now - _cache_at) < ECB_TTL_SECONDS:
        return dict(_cache_series)

    # Fetch (new host first; fallback is inside _fetch_json)
    j = _fetch_json(ECB_MRO_URL_NEW)
    series_list = _parse_sdmx_observations(j)
    series = {d: v for d, v in series_list}

    _cache_series = series
    _cache_at = now
    return dict(series)


def ecb_mro_latest_block() -> Dict[str, Any]:
    """
    Produce a block that matches your API shape for "Interest Rate (Policy)":
    {
      "latest": {"value": <float>|None, "date": "YYYY-MM"|None, "source": "ECB SDW (MRO)"|None},
      "series": {"YYYY-MM": value, ...}
    }
    """
    try:
        series = ecb_mro_series_monthly()
    except Exception:
        return {"latest": {"value": None, "date": None, "source": None}, "series": {}}

    if not series:
        return {"latest": {"value": None, "date": None, "source": None}, "series": {}}

    latest_month = sorted(series.keys())[-1]
    return {
        "latest": {"value": series[latest_month], "date": latest_month, "source": "ECB SDW (MRO)"},
        "series": series,
    }


def ecb_policy_rate_for_country(iso2: str) -> Dict[str, Any]:
    """
    For euro-area countries, return the ECB MRO block; otherwise return an empty block,
    so your indicator_service can just drop it in without special casing.
    """
    if iso2 and iso2.upper() in EURO_AREA_ISO2:
        return ecb_mro_latest_block()
    return {"latest": {"value": None, "date": None, "source": None}, "series": {}}


__all__ = [
    "EURO_AREA_ISO2",
    "ecb_mro_series_monthly",
    "ecb_mro_latest_block",
    "ecb_policy_rate_for_country",
]
