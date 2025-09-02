from typing import Dict
import httpx
from statistics import mean

def _eurostat_geo(iso2: str) -> str:
    return "UK" if iso2.upper() == "GB" else iso2.upper()

# --- Debt-to-GDP (annual, unchanged) ---
def eurostat_debt_to_gdp_annual(iso2: str) -> Dict[str, float]:
    url = "https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/gov_10dd_edpt1"
    params = {
        "na_item": "GD",  # gross debt
        "sector": "S13",  # general government
        "unit": "PC_GDP", # % of GDP
        "geo": _eurostat_geo(iso2),
    }
    try:
        r = httpx.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        time_index = data["dimension"]["time"]["category"]["index"]  # {'2024':0,'2023':1,...}
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
        return out
    except Exception:
        return {}

# --- ECB MRO (Main Refinancing Rate), monthly series for Euro Area aggregate ---
# Dataset: ei_mfir_m, int_rt=MRR_FR (main refi rate), freq=M, geo=EA
def eurostat_mro_monthly() -> Dict[str, float]:
    url = "https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/ei_mfir_m"
    params = {"int_rt": "MRR_FR", "freq": "M", "geo": "EA"}
    try:
        r = httpx.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return {}

    try:
        time_idx = data["dimension"]["time"]["category"]["index"]  # {'2025-07':0, '2025-06':1, ...}
        inv_time = {int(pos): str(tp) for tp, pos in time_idx.items()}
        values = data.get("value", {})  # {'0': 2.75, '1': 2.75, ...}
        out: Dict[str, float] = {}
        for k, v in values.items():
            try:
                idx = int(k)
                tp = inv_time.get(idx)  # 'YYYY-MM'
                if tp and v is not None:
                    out[tp] = float(v)
            except Exception:
                continue
        return out
    except Exception:
        return {}
