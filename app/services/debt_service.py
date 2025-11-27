# app/services/debt_service.py
from __future__ import annotations

from typing import Any, Dict, Optional
from datetime import date as _date


def _safe_import(path: str):
    try:
        return __import__(path, fromlist=["*"])
    except Exception:
        return None


def _get_iso3(country: str) -> Optional[str]:
    """Backward-compatible helper retained for any legacy callers.

    Newer code should prefer _get_iso_codes, but this keeps the original
    behaviour of returning a single ISO3 code based on app.utils.country_codes.
    """
    cc_mod = _safe_import("app.utils.country_codes")
    if not cc_mod or not hasattr(cc_mod, "get_country_codes"):
        return None
    try:
        codes = cc_mod.get_country_codes(country) or {}
    except Exception:
        return None
    return (
        codes.get("iso_alpha_3")
        or codes.get("alpha3")
        or codes.get("iso3")
    )


def _get_iso_codes(country: str) -> Dict[str, Optional[str]]:
    """Return a small iso code bundle for the given country name.

    This is the single source of truth for resolving iso2/iso3 inside this
    module. It uses the same app.utils.country_codes.get_country_codes helper
    as _get_iso3, but exposes both ISO2 and ISO3 codes for downstream use.
    """
    cc_mod = _safe_import("app.utils.country_codes")
    if not cc_mod or not hasattr(cc_mod, "get_country_codes"):
        return {"name": country, "iso_alpha_2": None, "iso_alpha_3": None}
    try:
        codes = cc_mod.get_country_codes(country) or {}
    except Exception:
        codes = {}
    return {
        "name": codes.get("name") or country,
        "iso_alpha_2": codes.get("iso_alpha_2") or codes.get("alpha2") or codes.get("iso2"),
        "iso_alpha_3": codes.get("iso_alpha_3") or codes.get("alpha3") or codes.get("iso3"),
    }


def _wb_years(iso3: str, code: str) -> Dict[str, float]:
    """Thin wrapper over wb_provider: fetch a WB indicator and return {year: float}."""
    wb_mod = _safe_import("app.providers.wb_provider")
    if not wb_mod:
        return {}
    try:
        raw = wb_mod.fetch_wb_indicator_raw(iso3, code)
        return wb_mod.wb_year_dict_from_raw(raw)
    except Exception:
        return {}


def _is_recent_year(year: Any, *, max_age_years: int = 5, today: Optional[_date] = None) -> bool:
    """Return True if a given year is within max_age_years of today.

    This is a light guardrail so that extremely old ratios (e.g. a single
    observation in 1990) are not surfaced as current Debt-to-GDP values.
    """
    try:
        y = int(str(year))
    except Exception:
        return False
    today = today or _date.today()
    return (today.year - y) <= max_age_years


def _to_float_year_dict(d: Any) -> Dict[str, float]:
    """Best-effort conversion of various provider outputs into {year: float}."""
    out: Dict[str, float] = {}
    if not d:
        return out
    if isinstance(d, dict):
        for k, v in d.items():
            try:
                out[str(k)] = float(v)
            except Exception:
                continue
        return out
    # handle list-of-(year, value) just in case
    if isinstance(d, (list, tuple)):
        for row in d:
            try:
                if not isinstance(row, (list, tuple)) or len(row) != 2:
                    continue
                k, v = row
                out[str(k)] = float(v)
            except Exception:
                continue
    return out


def _imf_debt_to_gdp_annual(iso2: str) -> Dict[str, float]:
    """Fetch general government debt-to-GDP from IMF, if available.

    We deliberately treat this as the *primary* source for Debt-to-GDP,
    falling back to Eurostat (for EU) and then to the World Bank ratio
    series when IMF coverage is missing.
    """
    mod = _safe_import("app.providers.imf_provider")
    if not mod:
        return {}
    fn = getattr(mod, "imf_debt_to_gdp_annual", None)
    if not callable(fn):
        return {}
    try:
        # Prefer keyword argument if the provider uses iso2=...
        raw = fn(iso2=iso2)
    except TypeError:
        # Fallback to positional call if the signature is older
        try:
            raw = fn(iso2)
        except Exception:
            return {}
    except Exception:
        return {}
    return _to_float_year_dict(raw)


def _eurostat_debt_to_gdp_annual(iso2: str) -> Dict[str, float]:
    """Best-effort Eurostat general government debt-to-GDP, if implemented.

    This is optional and only activates if app.providers.eurostat_provider
    exposes a suitable function. If nothing is available we simply return {}.
    """
    mod = _safe_import("app.providers.eurostat_provider")
    if not mod:
        return {}
    # try a couple of reasonable function names
    for name in (
        "eurostat_debt_to_gdp_annual",
        "get_debt_to_gdp_annual",
        "get_general_government_debt_to_gdp_annual",
    ):
        fn = getattr(mod, name, None)
        if not callable(fn):
            continue
        try:
            raw = fn(iso2=iso2)
        except TypeError:
            try:
                raw = fn(iso2)
            except Exception:
                continue
        except Exception:
            continue
        series = _to_float_year_dict(raw)
        if series:
            return series
    return {}


def compute_debt_payload(country: str) -> Dict[str, Any]:
    """Compute a normalized debt bundle for a country.

    Source hierarchy for Debt-to-GDP (% of GDP):

      1. IMF general government debt (% of GDP), via app.providers.imf_provider
         → primary, global coverage.
      2. Eurostat general government gross debt (% of GDP), via
         app.providers.eurostat_provider (if implemented) – mainly for EU.
      3. World Bank GC.DOD.TOTL.GD.ZS (central gov debt % of GDP) as a
         last-resort fallback when IMF/Eurostat are not available.

    We also enforce a simple recency rule: if the latest observation for the
    chosen series is older than max_age_years (default 5), we treat the series
    as unavailable instead of surfacing a very old value.

    The returned dict matches what indicator_service.py expects:
      {
        "government_debt": { "latest": {...}, "series": {...} },
        "nominal_gdp":     { "latest": {...}, "series": {...} },
        "debt_to_gdp":     { "latest": {...}, "series": {...} },
        "debt_to_gdp_series": {...}
      }
    """
    codes = _get_iso_codes(country)
    iso2 = codes.get("iso_alpha_2")
    iso3 = codes.get("iso_alpha_3")

    ratio_series: Dict[str, float] = {}
    source: Optional[str] = None

    # 1) IMF – primary global source for general government debt % of GDP
    if iso2:
        imf_series = _imf_debt_to_gdp_annual(iso2)
        if imf_series:
            ratio_series = imf_series
            source = "IMF (general government debt % of GDP)"

    # 2) Eurostat – optional override for EU countries if implemented
    #    We do not attempt to detect EU membership here; we simply ask
    #    eurostat_provider, and if it returns a non-empty series we treat
    #    that as authoritative for the given iso2.
    if iso2:
        eurostat_series = _eurostat_debt_to_gdp_annual(iso2)
        if eurostat_series:
            ratio_series = eurostat_series
            source = "Eurostat (general government gross debt % of GDP)"

    # 3) World Bank – last fallback, central gov debt % of GDP
    if not ratio_series and iso3:
        wb_series = _wb_years(iso3, "GC.DOD.TOTL.GD.ZS")
        if wb_series:
            ratio_series = _to_float_year_dict(wb_series)
            source = "World Bank (GC.DOD.TOTL.GD.ZS)"

    # Recency guardrail – avoid surfacing very old single observations
    latest_year: Optional[str] = None
    if ratio_series:
        try:
            years_sorted = sorted(ratio_series.keys(), key=lambda y: int(str(y)))
            latest_year = years_sorted[-1]
        except Exception:
            latest_year = None

        if latest_year is not None and not _is_recent_year(latest_year, max_age_years=5):
            # too old → treat as unavailable
            ratio_series = {}
            latest_year = None

    if ratio_series and latest_year is not None:
        latest_val = ratio_series[latest_year]
        debt_to_gdp_block = {
            "latest": {
                "value": latest_val,
                "date": latest_year,
                "source": source,
            },
            "series": ratio_series,
        }
        debt_to_gdp_series = ratio_series
    else:
        debt_to_gdp_block = {
            "latest": {
                "value": None,
                "date": None,
                "source": source or "unavailable",
            },
            "series": {},
        }
        debt_to_gdp_series = {}

    # For now we leave government_debt and nominal_gdp empty –
    # you can later fill them from:
    #  - GC.DOD.TOTL.CN (central gov debt, LCU)
    #  - GC.DOD.TOTL.CD (central gov debt, USD)
    #  - NY.GDP.MKTP.CN / NY.GDP.MKTP.CD (GDP, LCU/USD)
    gov_debt_block = {
        "latest": {"value": None, "date": None, "source": None},
        "series": {},
    }
    nominal_gdp_block = {
        "latest": {"value": None, "date": None, "source": None},
        "series": {},
    }

    return {
        "government_debt": gov_debt_block,
        "nominal_gdp": nominal_gdp_block,
        "debt_to_gdp": debt_to_gdp_block,
        "debt_to_gdp_series": debt_to_gdp_series,
    }


__all__ = ["compute_debt_payload"]
