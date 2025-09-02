from typing import Dict
import httpx

# Eurostat uses 'UK' (not 'GB'); most others match ISO2 (DE, SE, FR, NO, CH, IS, etc.)
def _eurostat_geo(iso2: str) -> str:
    if iso2.upper() == "GB":
        return "UK"
    return iso2.upper()

def eurostat_debt_to_gdp_annual(iso2: str) -> Dict[str, float]:
    """
    Returns { 'YYYY': value } for General Government debt-to-GDP (%) from Eurostat.
    Only available for EU/EEA/UK; for others returns {}.
    Dataset: gov_10dd_edpt1, filters: na_item=GD, sector=S13, unit=PC_GDP, geo=<ISO2/UK>
    """
    geo = _eurostat_geo(iso2)
    url = "https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/gov_10dd_edpt1"
    params = {
        "na_item": "GD",     # gross debt
        "sector": "S13",     # general government
        "unit": "PC_GDP",    # % of GDP
        "geo": geo,
    }
    try:
        r = httpx.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return {}

    # Expect single series across time
    try:
        time_index = data["dimension"]["time"]["category"]["index"]  # {'2024': 0, '2023': 1, ...}
        inv_time = {int(pos): str(year) for year, pos in time_index.items()}
        values = data.get("value", {})  # {'0': 63.4, '1': 64.2, ...}
        out: Dict[str, float] = {}
        for k, v in values.items():
            try:
                idx = int(k)
                year = inv_time.get(idx)
                if year is not None and v is not None:
                    out[year] = float(v)
            except Exception:
                continue
        # eurostat returns newest first; order not important since we return dict
        return out
    except Exception:
        return {}
