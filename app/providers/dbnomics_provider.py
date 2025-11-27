# app/providers/dbnomics_provider.py
from __future__ import annotations

from typing import Dict, Any, Optional


"""
DBnomics provider stub.

In a later phase, you can use the official dbnomics Python client
or HTTP to call:
  https://api.db.nomics.world/v22/

For now, this returns {} so callers fall back to other providers.
"""


def dbnomics_series(provider_code: str, dataset: str, indicator: str, iso3: str) -> Dict[str, float]:
    """
    Fetch a series from DBnomics.

    - provider_code: e.g. "ECB", "OECD", "EUROSTAT", or national providers
    - dataset: DBnomics dataset code
    - indicator: series code within the dataset
    - iso3: ISO3 country code to help build the series name

    Returns {period: value} where period is 'YYYY', 'YYYY-MM', etc.
    """
    # TODO: implement real DBnomics call
    return {}
