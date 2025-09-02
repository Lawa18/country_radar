from typing import Dict, Any, Optional

def fetch_worldbank_data(iso2: str, iso3: str) -> Dict[str, Any]:
    """
    TODO: Implement real HTTP fetch to World Bank. For now, return empty dict to keep service bootable.
    Expected keys (examples):
      - "GC.DOD.TOTL.GD.ZS"  # debt/GDP ratio (%)
      - "GC.DOD.TOTL.CN"     # debt level (LCU)
      - "NY.GDP.MKTP.CN"     # GDP (LCU)
      - "GC.DOD.TOTL.CD"     # debt (USD)
      - "NY.GDP.MKTP.CD"     # GDP (USD)
    """
    return {}

def wb_year_dict_from_raw(raw: Optional[Any]) -> Dict[int, float]:
    """
    Transform raw WB payload into {YYYY: value}. Stub returns {} if raw not in expected shape.
    """
    if not raw:
        return {}
    # If you already have a format (e.g., list of {date: 'YYYY', value: ...}), adapt here.
    try:
        out: Dict[int, float] = {}
        if isinstance(raw, dict):
            for y, v in raw.items():
                try:
                    yy = int(str(y))
                    if v is not None:
                        out[yy] = float(v)
                except Exception:
                    continue
        elif isinstance(raw, list):
            for item in raw:
                y = item.get("date") or item.get("year")
                v = item.get("value")
                if y is None:
                    continue
                try:
                    yy = int(str(y))
                    if v is not None:
                        out[yy] = float(v)
                except Exception:
                    continue
        return out
    except Exception:
        return {}
