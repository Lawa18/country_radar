# app/providers/ecb_provider.py
from __future__ import annotations
from typing import Dict, Any, List, Tuple
import httpx

# 20 euro-area ISO2 codes (2025)
EURO_AREA_ISO2 = {
    "AT","BE","HR","CY","EE","FI","FR","DE","IE","IT",
    "LV","LT","LU","MT","NL","PT","SK","SI","ES","GR",
}

ECB_TIMEOUT = 6.0

# MRO (Main Refinancing Operations) – euro area aggregate (U2, EUR)
# SDMX key: FM.M.U2.EUR.4F.KR.MRR_FR.LEV
# Use the *new* ECB API host directly to avoid redirects.
ECB_MRO_URL = (
    "https://data-api.ecb.europa.eu/service/data/FM/"
    "M.U2.EUR.4F.KR.MRR_FR.LEV?lastNObservations=180&format=sdmx-json"
)

def _parse_sdmx_observations(j: Dict[str, Any]) -> List[Tuple[str, float]]:
    """
    Extract [(YYYY-MM, value), ...] from ECB SDMX-JSON.
    """
    try:
        datasets = j.get("dataSets") or []
        if not datasets:
            return []
        series_map = datasets[0].get("series") or {}
        if not series_map:
            return []

        # There should be exactly one series for our filtered query; pick first
        first_series = next(iter(series_map.values()))
        obs_map = first_series.get("observations") or {}  # {"0":[3.5], "1":[3.75], ...}

        # Observation time labels come from structure.dimensions.observation[0].values
        dims = (j.get("structure") or {}).get("dimensions") or {}
        obs_dims = dims.get("observation") or []
        if not obs_dims:
            return []
        time_values = (obs_dims[0].get("values")) or []  # [{"id":"2023-01"}, ...]

        out: List[Tuple[str, float]] = []
        for idx_str, arr in obs_map.items():
            try:
                idx = int(idx_str)
                date = time_values[idx].get("id") or time_values[idx].get("name")
                if not date:
                    continue
                val = float(arr[0]) if (arr and arr[0] is not None) else None
                if val is not None:
                    out.append((date, val))
            except Exception:
                continue

        out.sort(key=lambda x: x[0])
        return out
    except Exception:
        return []

def ecb_mro_series_monthly() -> Dict[str, float]:
    """
    Returns monthly MRO policy rate for euro area: {"YYYY-MM": value, ...}
    """
    headers = {
        "Accept": "application/json",  # ECB honors ?format=sdmx-json
        "User-Agent": "country-radar/1.0",
    }
    with httpx.Client(timeout=ECB_TIMEOUT, headers=headers, follow_redirects=True) as client:
        r = client.get(ECB_MRO_URL)
        r.raise_for_status()
        series = _parse_sdmx_observations(r.json())
    return {d: v for d, v in series}

def ecb_mro_latest_block() -> Dict[str, Any]:
    """
    Block shaped like your other indicators:
    {
      "latest": {"value": <float>, "date": "YYYY-MM", "source": "ECB SDW (MRO)"},
      "series": {"YYYY-MM": value, ...}
    }
    """
    series = ecb_mro_series_monthly()
    if not series:
        return {"latest": {"value": None, "date": None, "source": None}, "series": {}}
    latest_month = sorted(series.keys())[-1]
    return {
        "latest": {"value": series[latest_month], "date": latest_month, "source": "ECB SDW (MRO)"},
        "series": series,
    }
