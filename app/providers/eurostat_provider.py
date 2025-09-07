from __future__ import annotations
from typing import Dict, Any, List, Tuple, Optional
import httpx

# Eurostat dissemination API (JSON-Stat 2.0)
EUROSTAT_BASE = "https://data-api.ec.europa.eu/api/dissemination/statistics/1.0/data"
ES_TIMEOUT = 8.0

# ---------- Generic JSON-Stat 2.0 parser ----------

def _jsonstat_series_from_dataset(j: Dict[str, Any]) -> Dict[str, float]:
    """
    Given a JSON-Stat 2.0 dataset (already filtered to ONE series except time),
    return a dict { "YYYY" or "YYYY-MM": float } in chronological order.
    Handles both list and dict 'value' forms.
    """
    # Some responses wrap in {"version":"2.0", "class":"dataset", ...}
    # Some may wrap in {"datasets":[{...}]} in other contexts; Eurostat uses the first.
    if "value" not in j or "dimension" not in j:
        # Try to unwrap if a "dataset" key exists (rare)
        ds = j.get("dataset") or j.get("datasets") or None
        if isinstance(ds, list) and ds:
            j = ds[0]
        elif isinstance(ds, dict):
            j = ds
        else:
            return {}

    dims = j.get("dimension") or {}
    dim_ids: List[str] = dims.get("id") or []

    # Find the time dimension name
    time_dim = None
    if "time" in dim_ids:
        time_dim = "time"
    elif "TIME_PERIOD" in dim_ids:
        time_dim = "TIME_PERIOD"
    else:
        # Fallback: look for dim whose name includes "time"
        for d in dim_ids:
            if "time" in d.lower():
                time_dim = d
                break
    if not time_dim:
        return {}

    time_dim_obj = dims.get(time_dim) or {}
    cat = (time_dim_obj.get("category") or {})
    # cat["index"] is a mapping label -> integer position
    index_map: Dict[str, int] = cat.get("index") or {}

    # Build ordered list of (pos, label)
    ordered_labels: List[Tuple[int, str]] = []
    for label, pos in index_map.items():
        try:
            ordered_labels.append((int(pos), str(label)))
        except Exception:
            continue
    ordered_labels.sort(key=lambda x: x[0])
    labels_only = [lbl for _, lbl in ordered_labels]

    values = j.get("value")

    out: Dict[str, float] = {}

    if isinstance(values, list):
        # One varying dimension (time), everything else filtered -> values align to time positions.
        for i, lbl in enumerate(labels_only):
            try:
                v = values[i]
                if v is None:
                    continue
                out[lbl] = float(v)
            except Exception:
                continue
        return out

    if isinstance(values, dict):
        # Dict with flattened indices -> value. With only time varying, keys will be "0","1",...
        for lbl, pos in index_map.items():
            key = str(pos)
            if key in values and values[key] is not None:
                try:
                    out[str(lbl)] = float(values[key])
                except Exception:
                    continue
        # Ensure chronological by label order we computed
        return {lbl: out[lbl] for lbl in labels_only if lbl in out}

    return {}

def _get_jsonstat_series(dataset: str, params: Dict[str, str]) -> Dict[str, float]:
    """
    Call Eurostat dissemination API for a dataset with given params,
    parse JSON-Stat and return {period: float}.
    """
    url = f"{EUROSTAT_BASE}/{dataset}"
    headers = {
        # JSON-Stat 2.0 is returned by default when hitting this endpoint
        "Accept": "application/json",
        "User-Agent": "country-radar/1.0",
    }
    # Eurostat allows "time=YYYY-01/YYYY-12" or "time=YYYY-01/next"
    # We'll request a reasonably long range; the server will clip to available data.
    q = dict(params)
    if "time" not in q:
        q["time"] = "1990-01/2035-12"

    with httpx.Client(timeout=ES_TIMEOUT, headers=headers, follow_redirects=True) as client:
        r = client.get(url, params=q)
        r.raise_for_status()
        j = r.json()
    return _jsonstat_series_from_dataset(j)

# ---------- Specific series ----------

def eurostat_hicp_yoy_monthly(iso2: str) -> Dict[str, float]:
    """
    Harmonised Index of Consumer Prices (HICP), annual rate of change (y/y), monthly.
    Dataset: prc_hicp_manr
    Filters:
      - coicop=CP00 (All-items)
      - unit=RTE   (Annual rate of change in %). If empty, we fallback to RCH_A.
      - geo=ISO2
    Returns: {"YYYY-MM": pct, ...}
    """
    # Try unit=RTE first (common), then fallback to RCH_A if dataset uses that code.
    base_params = {"coicop": "CP00", "geo": iso2.upper()}
    for unit in ("RTE", "RCH_A"):
        params = dict(base_params)
        params["unit"] = unit
        try:
            series = _get_jsonstat_series("prc_hicp_manr", params)
            if series:
                return series
        except Exception:
            continue
    return {}

def eurostat_unemployment_rate_monthly(iso2: str) -> Dict[str, float]:
    """
    Unemployment rate, monthly, seasonally adjusted, total 15–74.
    Dataset: une_rt_m
    Filters:
      - s_adj=SA
      - sex=T
      - age=Y15-74
      - geo=ISO2
    Returns: {"YYYY-MM": pct, ...}
    """
    params = {
        "s_adj": "SA",
        "sex": "T",
        "age": "Y15-74",
        "geo": iso2.upper(),
    }
    try:
        series = _get_jsonstat_series("une_rt_m", params)
        return series
    except Exception:
        return {}

def eurostat_debt_to_gdp_annual(iso2: str) -> Dict[str, float]:
    """
    General government gross debt, % of GDP, annual (EDP concept).
    Primary dataset: gov_10dd_edpt1 (unit=PC_GDP, sector=S13, na_item=DD)
    Fallback: gov_10dd_edpt (older naming in some mirrors)
    Returns: {"YYYY": pct, ...}
    """
    common = {"unit": "PC_GDP", "sector": "S13", "na_item": "DD", "geo": iso2.upper()}
    # Annual dataset uses year labels like "1995", "1996", ...
    for ds in ("gov_10dd_edpt1", "gov_10dd_edpt"):
        try:
            series = _get_jsonstat_series(ds, common)
            # Filter to year-only labels if any accidental monthly labels appear
            series = {k: v for k, v in series.items() if len(k) == 4 and k.isdigit()}
            if series:
                return series
        except Exception:
            continue
    return {}
