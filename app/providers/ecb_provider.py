from __future__ import annotations
from typing import Dict
import httpx, csv, io

def ecb_mro_monthly() -> Dict[str, float]:
    """
    ECB Main Refinancing Operations rate (monthly), {YYYY-MM: rate}.
    Uses SDW REST with CSV for easy parsing.
    Series key: FM.M.U2.EUR.4F.KR.MRR_FR.LEV  (frequency=Monthly, area=U2 (euro area))
    """
    url = "https://sdw.ecb.europa.eu/service/data/FM/M.U2.EUR.4F.KR.MRR_FR.LEV?format=csvdata"
    try:
        with httpx.Client(timeout=httpx.Timeout(5.0, read=10.0)) as client:
            r = client.get(url)
            r.raise_for_status()
            text = r.text
        out: Dict[str, float] = {}
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            t = (row.get("TIME_PERIOD") or "").strip()
            v = (row.get("OBS_VALUE") or "").strip()
            if len(t) == 7 and t[4] == "-":  # YYYY-MM
                try:
                    out[t] = float(v)
                except Exception:
                    continue
        return out
    except Exception:
        return {}
