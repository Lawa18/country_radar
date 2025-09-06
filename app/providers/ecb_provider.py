from __future__ import annotations
from typing import Dict, Tuple, Any, List
import time
import httpx

# Slightly higher timeout; ECB can be slow behind Cloudflare
ECB_TIMEOUT = 8.0
ECB_RETRIES = 2
ECB_BACKOFF = 1.0  # seconds between attempts

# MRO (Main Refinancing Operations) – euro area aggregate (U2, EUR)
# SDMX key: FM.M.U2.EUR.4F.KR.MRR_FR.LEV
ECB_MRO_URL = (
    "https://sdw-wsrest.ecb.europa.eu/service/data/FM/"
    "M.U2.EUR.4F.KR.MRR_FR.LEV?lastNObservations=120&format=sdmx-json"
)

def _parse_sdmx_observations(j: Dict[str, Any]) -> List[Tuple[str, float]]:
    """
    Extract [(YYYY-MM, value), ...] from SDMX-JSON response.
    """
    try:
        dataset = j["dataSets"][0]
        # first and only series in this query
        series_key = next(iter(dataset["series"].keys()))
        obs_map = dataset["series"][series_key]["observations"]  # {"0":[3.5], "1":[3.75], ...}

        times = j["structure"]["dimensions"]["observation"][0]["values"]  # [{"id":"2023-01"}, ...]
        out: List[Tuple[str, float]] = []
        for idx_str, arr in obs_map.items():
            idx = int(idx_str)
            meta = times[idx]
            # prefer "id" like "2024-07"; fallback to "name"
            date = meta.get("id") or meta.get("name")
            val = arr[0] if arr else None
            if val is not None and date:
                out.append((date, float(val)))
        out.sort(key=lambda x: x[0])
        return out
    except Exception:
        return []

def ecb_mro_series_monthly() -> Dict[str, float]:
    headers = {"Accept": "application/vnd.sdmx.data+json;version=1.0"}
    last_err: Exception | None = None
    for attempt in range(ECB_RETRIES + 1):
        try:
            with httpx.Client(timeout=ECB_TIMEOUT, headers=headers, http2=False) as client:
                r = client.get(ECB_MRO_URL)
                r.raise_for_status()
                series = _parse_sdmx_observations(r.json())
                if series:
                    return {d: v for d, v in series}
        except Exception as e:
            last_err = e
            if attempt < ECB_RETRIES:
                time.sleep(ECB_BACKOFF)
    # If all attempts failed, return empty (the caller will gracefully fallback)
    return {}

def ecb_mro_latest_block() -> Dict[str, Any]:
    series = ecb_mro_series_monthly()
    if not series:
        return {"latest": {"value": None, "date": None, "source": None}, "series": {}}
    latest_month = sorted(series.keys())[-1]
    return {
        "latest": {"value": series[latest_month], "date": latest_month, "source": "ECB SDW (MRO)"},
        "series": series,
    }
