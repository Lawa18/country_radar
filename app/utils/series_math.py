from __future__ import annotations
from typing import Dict, Tuple, Optional, Iterable
from math import isfinite

def latest(series: Dict[str, float]) -> Optional[Tuple[str, float]]:
    # assumes keys are "YYYY" or "YYYY-MM"
    if not series:
        return None
    k = max(series.keys())
    v = series.get(k)
    if v is None:
        return None
    try:
        v = float(v)
    except Exception:
        return None
    return (k, v)

def yoy_from_index(monthly_index: Dict[str, float]) -> Dict[str, float]:
    """Given a monthly index {YYYY-MM: level}, return {YYYY-MM: pct_yoy}."""
    out: Dict[str, float] = {}
    for ym, v in monthly_index.items():
        try:
            y = str(ym)[:4]
            m = str(ym)[5:7]
            prev = f"{int(y)-1}-{m}"
            if prev in monthly_index and monthly_index[prev] not in (None, 0):
                out[str(ym)] = round((float(v)/float(monthly_index[prev]) - 1.0)*100.0, 2)
        except Exception:
            continue
    return out

def mom_from_index(monthly_index: Dict[str, float]) -> Dict[str, float]:
    """Given a monthly index {YYYY-MM: level}, return {YYYY-MM: pct_mom}."""
    out: Dict[str, float] = {}
    # naive previous-month string math; OK because keys are YYYY-MM contiguous from sources
    months = sorted(monthly_index.keys())
    for i in range(1, len(months)):
        cur, prev = months[i], months[i-1]
        try:
            c, p = float(monthly_index[cur]), float(monthly_index[prev])
            if p != 0 and isfinite(c) and isfinite(p):
                out[cur] = round((c/p - 1.0)*100.0, 2)
        except Exception:
            continue
    return out

def first_non_empty(*series_dicts: Iterable[Dict[str, float]]) -> Dict[str, float]:
    for d in series_dicts:
        if isinstance(d, dict) and d:
            return d
    return {}
