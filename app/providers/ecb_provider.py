# app/providers/ecb_provider.py
from __future__ import annotations
from typing import Dict, Tuple, Any, List, Optional
import time
import httpx

ECB_TIMEOUT = 8.0
ECB_RETRIES = 3
ECB_BACKOFF = 0.8

# Prefer new host; keep old host which 302-redirects
ECB_MRO_URL_PRIMARY = (
    "https://data-api.ecb.europa.eu/service/data/FM/M.U2.EUR.4F.KR.MRR_FR.LEV"
    "?lastNObservations=120&format=sdmx-json"
)
ECB_MRO_URL_FALLBACK = (
    "https://sdw-wsrest.ecb.europa.eu/service/data/FM/M.U2.EUR.4F.KR.MRR_FR.LEV"
    "?lastNObservations=120&format=sdmx-json"
)

def _get_json(url: str) -> Optional[Dict[str, Any]]:
    for attempt in range(1, ECB_RETRIES + 1):
        try:
            with httpx.Client(timeout=ECB_TIMEOUT, headers={"Accept": "application/json"}, follow_redirects=True) as client:
                r = client.get(url)
                r.raise_for_status()
                return r.json()
        except Exception as e:
            print(f"[ECB] attempt {attempt} failed {url}: {e}")
            if attempt < ECB_RETRIES:
                time.sleep(ECB_BACKOFF * attempt)
    return None

def _parse_sdmx_observations(j: Dict[str, Any]) -> List[Tuple[str, float]]:
    try:
        dataset = j["dataSets"][0]
        series_key = next(iter(dataset["series"].keys()))
        obs_map = dataset["series"][series_key]["observations"]
        times = j["structure"]["dimensions"]["observation"][0]["values"]
        out: List[Tuple[str, float]] = []
        for idx_str, arr in obs_map.items():
            idx = int(idx_str)
            date = times[idx].get("id") or times[idx].get("name")
            val = float(arr[0]) if arr and arr[0] is not None else None
            if date and val is not None:
                out.append((date, val))
        out.sort(key=lambda x: x[0])
        return out
    except Exception:
        return []

def ecb_mro_series_monthly() -> Dict[str, float]:
    for url in (ECB_MRO_URL_PRIMARY, ECB_MRO_URL_FALLBACK):
        data = _get_json(url)
        if not data:
            continue
        series = _parse_sdmx_observations(data)
        if series:
            return {d: v for d, v in series}
    return {}

def ecb_mro_latest_block() -> Dict[str, Any]:
    series = ecb_mro_series_monthly()
    if not series:
        return {"latest": {"value": None, "date": None, "source": None}, "series": {}}
    latest = sorted(series.keys())[-1]
    return {
        "latest": {"value": series[latest], "date": latest, "source": "ECB SDW (MRO)"},
        "series": series,
    }
