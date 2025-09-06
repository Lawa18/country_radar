from __future__ import annotations
from typing import Dict, Tuple, Any, List
import httpx

ECB_TIMEOUT = 4.0

# MRO (Main Refinancing Operations) – euro area aggregate (U2, EUR)
# SDMX key: FM.M.U2.EUR.4F.KR.MRR_FR.LEV
ECB_MRO_URL = (
    "https://sdw-wsrest.ecb.europa.eu/service/data/FM/"
    "M.U2.EUR.4F.KR.MRR_FR.LEV?lastNObservations=60&format=sdmx-json"
)

def _parse_sdmx_observations(j: Dict[str, Any]) -> List[Tuple[str, float]]:
    """
    Extract [(YYYY-MM, value), ...] from SDMX-JSON response.
    """
    try:
        dataset = j["dataSets"][0]
        series_key = next(iter(dataset["series"].keys()))  # e.g., "0:0:0:0:0"
        obs_map = dataset["series"][series_key]["observations"]  # {"0":[3.5], "1":[3.75], ...}

        times = j["structure"]["dimensions"]["observation"][0]["values"]  # [{"id":"2023-01"}, ...]
        out: List[Tuple[str, float]] = []
        for idx_str, arr in obs_map.items():
            idx = int(idx_str)
            date = times[idx]["id"] if "id" in times[idx] else times[idx]["name"]
            val = float(arr[0]) if arr and arr[0] is not None else None
            if val is not None:
                out.append((date, val))
        out.sort(key=lambda x: x[0])
        return out
    except Exception:
        return []

def ecb_mro_series_monthly() -> Dict[str, float]:
    headers = {"Accept": "application/vnd.sdmx.data+json;version=1.0"}
    with httpx.Client(timeout=ECB_TIMEOUT, headers=headers) as client:
        r = client.get(ECB_MRO_URL)
        r.raise_for_status()
        series = _parse_sdmx_observations(r.json())
    return {d: v for d, v in series}

def ecb_mro_latest_block() -> Dict[str, Any]:
    series = ecb_mro_series_monthly()
    if not series:
        return {"latest": {"value": None, "date": None, "source": None}, "series": {}}
    latest_month = sorted(series.keys())[-1]
    return {
        "latest": {"value": series[latest_month], "date": latest_month, "source": "ECB SDW (MRO)"},
        "series": series,
    }
