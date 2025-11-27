# app/providers/oecd_provider.py
from __future__ import annotations

from typing import Dict, Any, Optional


"""
Minimal OECD provider stub.

Later, you can add real SDMX/JSON fetch logic for:
  - unemployment_rate
  - CPI index
  - other MEI indicators

For now, it returns {} so the indicator matrix will fall back
to IMF / Eurostat / World Bank.
"""


def oecd_series(iso3: str, indicator: str) -> Dict[str, float]:
    """
    Generic OECD series fetcher; returns {period: value}.
    'indicator' should align with indicator_matrix keys like:
      - "UNEMP_RATE"
      - "CPIIDX"
    """
    # TODO: implement real OECD API calls
    return {}
