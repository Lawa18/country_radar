from typing import Optional, Dict, Any
from pydantic import BaseModel

class RatioBlock(BaseModel):
    value: Optional[float]
    date: Optional[str]
    source: Optional[str]
    government_type: Optional[str] = None

class DebtResponse(BaseModel):
    country: str
    iso_codes: Dict[str, str] | None = None
    debt_to_gdp: RatioBlock
    debt_to_gdp_series: Dict[str, float] = {}
    path_used: Optional[str] = None
    government_debt: Optional[Dict[str, Any]] = None
    nominal_gdp: Optional[Dict[str, Any]] = None

class SeriesLatest(BaseModel):
    value: Optional[float]
    date: Optional[str]
    source: Optional[str]

class LabeledSeries(BaseModel):
    latest: SeriesLatest
    series: Dict[str, float] = {}

class CountryDataResponse(BaseModel):
    country: str
    iso_codes: Dict[str, str]
    imf_data: Dict[str, LabeledSeries | SeriesLatest | Dict[str, Any]]
    government_debt: LabeledSeries
    nominal_gdp: LabeledSeries
    debt_to_gdp: Dict[str, Any]
    additional_indicators: Dict[str, Any] = {}

