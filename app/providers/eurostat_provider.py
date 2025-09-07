# app/providers/eurostat_provider.py
from __future__ import annotations
from typing import Dict, Any, Optional, List
import time
import httpx

EUROSTAT_TIMEOUT = 8.0
EUROSTAT_RETRIES = 3
EUROSTAT_BACKOFF = 0.8

# Try both—DNS on either host sometimes flakes
EUROSTAT_BASES: List[str] = [
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data",
    "https://data-api.ec.europa.eu/api/dissemination/statistics/1.0/data",
]

def _get_json(url: str) -> Optional[Dict[str, Any]]:
    last_err: Optional[Exception] = None
    for attempt in range(1, EUROSTAT_RETRIES + 1):
        try:
            with httpx.Client(timeout=EUROSTAT_TIMEOUT, headers={"Accept": "application/json"}, follow_redirects=True) as client:
                r = client.get(url)
                r.raise_for_status()
                return r.json()
        except Exception as e:
            last_err = e
            print(f"[Eurostat] attempt {attempt} failed {url}: {e}")
            if attempt < EUROSTAT_RETRIES:
                time.sleep(EUROSTAT_BACKOFF * attempt)
    return None

def _parse_dataset(obj: Dict[str, Any]) -> Dict[str, float]:
    """
    Eurostat 1.0 API dataset -> {'time': value} numeric map (chronological).
    """
    try:
        if obj.get("class") != "dataset":
            return {}
        dim = obj.get("dimension", {})
        time_cat = dim.get("time", {}).get("category", {})
        tindex = time_cat.get("index", {})  # { "2023-01": 0, ... }
        values = obj.get("value", {})
        out: Dict[str, float] = {}
        for t_label, idx in tindex.items():
            key = str(idx)
            if key in values and values[key] is not None:
                try:
                    out[str(t_label)] = float(values[key])
                except Exception:
                    pass
        return dict(sorted(out.items()))
    except Exception:
        return {}

def eurostat_hicp_yoy_monthly(iso2: str, start: str = "2019-01") -> Dict[str, float]:
    """
    HICP monthly YoY % — whole basket (CP00).
    Dataset: prc_hicp_manr
    """
    ds = "prc_hicp_manr"
    # 'unit=RCH_A' = annual rate of change (%). Some mirrors accept 'RTE', we prefer RCH_A.
    query = f"coicop=CP00&unit=RCH_A&geo={iso2}&time={start}/2035-12&format=json"
    for base in EUROSTAT_BASES:
        url = f"{base}/{ds}?{query}"
        data = _get_json(url)
        if not data:
            continue
        parsed = _parse_dataset(data)
        if parsed:
            return parsed
    return {}

def eurostat_unemployment_rate_monthly(iso2: str, start: str = "2019-01") -> Dict[str, float]:
    """
    Unemployment rate monthly (%) — seasonally adjusted, total, age 15-74.
    Dataset: une_rt_m
    """
    ds = "une_rt_m"
    query = f"s_adj=SA&sex=T&age=Y15-74&geo={iso2}&time={start}/2035-12&format=json"
    for base in EUROSTAT_BASES:
        url = f"{base}/{ds}?{query}"
        data = _get_json(url)
        if not data:
            continue
        parsed = _parse_dataset(data)
        if parsed:
            return parsed
    return {}
