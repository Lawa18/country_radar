from typing import Dict, Any
import httpx

def _get(d: Any, *path):
    cur = d
    for k in path:
        if isinstance(cur, dict):
            cur = cur.get(k) or cur.get(f"@{k}")
        else:
            return None
    return cur

def _parse_compact_series(data: Dict[str, Any]) -> Dict[str, float]:
    ds = _get(data, "CompactData", "DataSet")
    if ds is None:
        return {}
    series = ds.get("Series") if isinstance(ds, dict) else None
    if series is None:
        return {}
    obs = series.get("Obs") if isinstance(series, dict) else None
    if obs is None:
        return {}
    out: Dict[str, float] = {}
    if isinstance(obs, dict):
        tp = _get(obs, "TIME_PERIOD"); val = _get(obs, "OBS_VALUE")
        if tp and val not in (None, ""):
            out[str(tp)] = float(val)
    elif isinstance(obs, list):
        for row in obs:
            tp = _get(row, "TIME_PERIOD"); val = _get(row, "OBS_VALUE")
            if tp and val not in (None, ""):
                try:
                    out[str(tp)] = float(val)
                except Exception:
                    continue
    return out

def _weo_series(var: str, iso3: str) -> Dict[str, float]:
    url = f"https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/WEO/{var}.A.{iso3.upper()}?dimensionAtObservation=TimeDimension"
    r = httpx.get(url, timeout=25)
    r.raise_for_status()
    return _parse_compact_series(r.json())

def imf_debt_to_gdp_annual(iso3: str) -> Dict[str, float]:
    # General government gross debt (% of GDP)
    try:
        return _weo_series("GGXWDG_NGDP", iso3)
    except Exception:
        return {}

def imf_weo_block(iso3: str) -> Dict[str, Dict[str, float]]:
    """
    Basket of annual WEO series we surface in indicators.
    Returns { label: { 'YYYY': value } }
    """
    mapping = {
        "GDP Growth (%)": "NGDP_RPCH",
        "CPI": "PCPIPCH",  # avg CPI % change
        "Unemployment (%)": "LUR",
        "Current Account Balance (% of GDP)": "BCA_NGDPD",
    }
    out: Dict[str, Dict[str, float]] = {}
    for label, var in mapping.items():
        try:
            out[label] = _weo_series(var, iso3)
        except Exception:
            out[label] = {}
    return out
