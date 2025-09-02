from typing import Dict, Any
import httpx

def _get(obj: Any, *keys):
    cur = obj
    for k in keys:
        if cur is None:
            return None
        if isinstance(cur, dict):
            # accept both bare and '@'-prefixed SDMX keys
            if k in cur:
                cur = cur[k]
            elif f"@{k}" in cur:
                cur = cur[f"@{k}"]
            else:
                return None
        else:
            return None
    return cur

def imf_debt_to_gdp_annual(iso3: str) -> Dict[str, float]:
    """
    IMF WEO: General government gross debt (% of GDP), code GGXWDG_NGDP, annual.
    Returns {'YYYY': value} or {} if unavailable.
    """
    base = "https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/WEO"
    url = f"{base}/GGXWDG_NGDP.A.{iso3.upper()}?dimensionAtObservation=TimeDimension"
    try:
        r = httpx.get(url, timeout=25)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return {}

    # Parse SDMX CompactData structure
    try:
        ds = _get(data, "CompactData", "DataSet")
        if ds is None:
            return {}

        series = _get(ds, "Series")
        if series is None:
            # sometimes Series is a list; handle both
            s_list = ds.get("Series") if isinstance(ds, dict) else None
            series = s_list[0] if isinstance(s_list, list) and s_list else None
            if series is None:
                return {}

        # Obs may be list of {'@TIME_PERIOD': 'YYYY', '@OBS_VALUE': 'nn.nn'}
        obs = series.get("Obs") if isinstance(series, dict) else None
        if obs is None:
            return {}

        out: Dict[str, float] = {}
        if isinstance(obs, dict):
            # single observation case
            tp = _get(obs, "TIME_PERIOD")
            val = _get(obs, "OBS_VALUE")
            if tp and val is not None:
                out[str(tp)] = float(val)
        elif isinstance(obs, list):
            for row in obs:
                tp = _get(row, "TIME_PERIOD")
                val = _get(row, "OBS_VALUE")
                if tp and val not in (None, ""):
                    try:
                        out[str(tp)] = float(val)
                    except Exception:
                        continue
        return out
    except Exception:
        return {}
