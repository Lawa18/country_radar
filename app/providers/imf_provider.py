# app/providers/imf_provider.py
from __future__ import annotations
from typing import Dict, Any, Optional
import time
import httpx

IMF_TIMEOUT = 10.0
IMF_RETRIES = 3
IMF_BACKOFF = 1.2

# IFS variable codes (monthly where available)
IFS_CODE = {
    # Inflation YoY (%), monthly
    "CPI_YOY": "PCPI_YY",
    # Policy rate (% p.a.), monthly (many countries have this; euro area handled by ECB)
    "POLICY_RATE": "FPOLM_PA",
    # FX: local currency per USD, monthly
    "FX_USD": "ENDA_XDC_USD_RATE",
    # Official reserves, USD (end of period), monthly
    "RESERVES_USD": "RAXGS_USD",
    # Unemployment rate (%), monthly (not all countries available)
    "UNEMP_RATE": "LUR_PT",
}

def _parse_ifs_compact(obj: Dict[str, Any]) -> Dict[str, float]:
    """
    Parse IMF SDMX 'CompactData' JSON to {period -> value} (period: YYYY-MM or YYYY-Qx).
    """
    try:
        root = obj.get("CompactData", {})
        # 'CompactData' -> e.g. {'IFS': {'Series': {...}}}
        dataset = next((v for v in root.values() if isinstance(v, dict)), None)
        if not dataset:
            return {}
        series_list = dataset.get("Series") or []
        if isinstance(series_list, dict):
            series_list = [series_list]

        out: Dict[str, float] = {}
        for s in series_list:
            obs = s.get("Obs") or []
            if isinstance(obs, dict):  # sometimes Obs can be a dict
                obs = [obs]
            for o in obs:
                t = o.get("@TIME_PERIOD")
                v = o.get("@OBS_VALUE")
                if t and v is not None:
                    try:
                        out[str(t)] = float(v)
                    except Exception:
                        pass
        return dict(sorted(out.items()))  # chronological by string period
    except Exception:
        return {}

def _http_get_json(url: str, headers: Optional[Dict[str, str]] = None) -> Optional[Dict[str, Any]]:
    last_err: Optional[Exception] = None
    for attempt in range(1, IMF_RETRIES + 1):
        try:
            with httpx.Client(timeout=IMF_TIMEOUT, headers=headers or {"Accept": "application/json"}, follow_redirects=True) as client:
                r = client.get(url)
                r.raise_for_status()
                return r.json()
        except Exception as e:
            last_err = e
            print(f"[IMF] attempt {attempt} failed {url}: {e}")
            if attempt < IMF_RETRIES:
                time.sleep(IMF_BACKOFF * attempt)
    return None

def ifs_monthly_series(iso2: str, indicator_key: str, start: str = "2019-01", end: Optional[str] = None) -> Dict[str, float]:
    """
    Generic IFS monthly fetch: returns { 'YYYY-MM': value, ... } (or quarterly period strings).
    iso2: ISO-2 country (DE, US, etc)
    indicator_key: key in IFS_CODE above
    """
    code = IFS_CODE.get(indicator_key)
    if not code:
        return {}

    base = "https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData"
    freq = "M"  # monthly; IMF will return quarterly/annual for some codes if monthly doesn't exist
    qs = []
    if start:
        qs.append(f"startPeriod={start}")
    if end:
        qs.append(f"endPeriod={end}")
    query = ("?" + "&".join(qs)) if qs else ""

    url = f"{base}/IFS/{freq}.{iso2}.{code}{query}"
    data = _http_get_json(url)
    if not data:
        return {}
    return _parse_ifs_compact(data)

# Convenience wrappers
def imf_cpi_yoy_monthly(iso2: str, start: str = "2019-01") -> Dict[str, float]:
    return ifs_monthly_series(iso2, "CPI_YOY", start)

def imf_policy_rate_monthly(iso2: str, start: str = "2019-01") -> Dict[str, float]:
    return ifs_monthly_series(iso2, "POLICY_RATE", start)

def imf_fx_to_usd_monthly(iso2: str, start: str = "2019-01") -> Dict[str, float]:
    return ifs_monthly_series(iso2, "FX_USD", start)

def imf_reserves_usd_monthly(iso2: str, start: str = "2019-01") -> Dict[str, float]:
    return ifs_monthly_series(iso2, "RESERVES_USD", start)

def imf_unemployment_rate_monthly(iso2: str, start: str = "2019-01") -> Dict[str, float]:
    return ifs_monthly_series(iso2, "UNEMP_RATE", start)

# --- Legacy helper names (kept to avoid import errors in older code) ---

def fetch_imf_sdmx_series(iso2: str) -> Dict[str, Dict[str, float]]:
    """
    Returns a dict of several standard monthly series; caller picks what it needs.
    {
      "CPI": {...}, "FX Rate": {...}, "Interest Rate (Policy)": {...},
      "Reserves (USD)": {...}, "Unemployment (%)": {...}
    }
    """
    return {
        "CPI": imf_cpi_yoy_monthly(iso2),
        "FX Rate": imf_fx_to_usd_monthly(iso2),
        "Interest Rate (Policy)": imf_policy_rate_monthly(iso2),
        "Reserves (USD)": imf_reserves_usd_monthly(iso2),
        "Unemployment (%)": imf_unemployment_rate_monthly(iso2),
    }

def imf_debt_to_gdp_annual(_iso3: str) -> Dict[str, float]:
    """
    Stub (optional). If you later add WEO-based debt/GDP here, return { 'YYYY': value }.
    For now we leave debt/GDP to Eurostat (EU) or WB fallback.
    """
    return {}
