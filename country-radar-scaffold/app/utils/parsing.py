# app/utils/parsing.py
from __future__ import annotations
from typing import Dict, Any, Optional

def extract_latest_numeric_entry(entry_dict: dict, source_label: str = "IMF") -> Optional[Dict[str, Any]]:
    """{'YYYY': value, ...} → latest {'value','date','source'} or None."""
    try:
        pairs = [(int(y), float(v)) for y, v in entry_dict.items()
                 if isinstance(v, (float, int, str)) and str(v).replace('.', '', 1).replace('-', '', 1).isdigit()]
        if not pairs:
            return None
        y, v = max(pairs, key=lambda x: x[0])
        return {"value": v, "date": str(y), "source": source_label}
    except Exception:
        return None

def latest_common_year_pair(a: dict, b: dict) -> Optional[tuple[int, float, float]]:
    """Latest year present in both dicts → (year, a_val, b_val) or None."""
    try:
        ya = {int(y): float(v) for y, v in a.items()
              if isinstance(v, (float, int, str)) and str(v).replace('.', '', 1).replace('-', '', 1).isdigit()}
        yb = {int(y): float(v) for y, v in b.items()
              if isinstance(v, (float, int, str)) and str(v).replace('.', '', 1).replace('-', '', 1).isdigit()}
        common = sorted(set(ya) & set(yb))
        if not common:
            return None
        y = common[-1]
        return (y, ya[y], yb[y])
    except Exception:
        return None

def normalize_period(p: str) -> str:
    """Normalize 'YYYY', 'YYYY-Qx', 'YYYY_MM' → uppercase, no spaces/underscores."""
    if not isinstance(p, str):
        return ""
    return p.replace(" ", "").replace("_", "").replace("-", "").upper()
