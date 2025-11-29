"""
app/services/indicator_matrix.py

Declarative source matrix for core Country Radar indicators.

This module is intentionally *dumb*: it does not call any APIs.
It just describes, for each KPI, which providers to try in what order,
what dataset / indicator code to use, and what transformation (if any)
is expected.

indicator_service.py uses INDICATOR_MATRIX to decide which provider
to call and how to post-process the result.
"""

from __future__ import annotations

from typing import Dict, List, Literal, Optional, TypedDict


Frequency = Literal["A", "Q", "M", "D"]  # annual, quarterly, monthly, daily


class SourceSpec(TypedDict, total=False):
    """A single provider candidate for an indicator."""

    provider: str           # "gmd", "imf", "world_bank", "eurostat", "oecd", "ecb", "dbnomics"
    dataset: Optional[str]  # e.g. "WEO", "IFS", "WDI", "gov_10a_ggdebt"
    indicator: Optional[str]  # provider-specific indicator code or name
    func: Optional[str]     # optional helper function name in app.providers.*
    freq: Frequency         # native frequency of the series
    transform: Optional[str]  # "none", "yoy", "qoq", "mom", "ratio", etc.
    notes: Optional[str]


class IndicatorSpec(TypedDict, total=False):
    """Configuration for a single logical KPI."""

    key: str                  # internal key, e.g. "inflation_yoy"
    label: str                # human-friendly label
    unit: str                 # display unit, e.g. "percent", "USD", "EUR bn"
    preferred_freq: Frequency # frequency we try to present in the radar
    sources: List[SourceSpec] # ordered list of provider candidates
    max_age_years: Optional[int]


# -----------------------------------------------------------------------------
# Indicator Matrix
# -----------------------------------------------------------------------------

INDICATOR_MATRIX: Dict[str, IndicatorSpec] = {

    # -------------------------------------------------------------------------
    # 1) Currency – FX vs USD
    # -------------------------------------------------------------------------
    "currency": {
        "key": "currency",
        "label": "Currency vs USD",
        "unit": "per USD",          # e.g. 18.76 MXN per USD
        "preferred_freq": "M",
        "max_age_years": 1,
        "sources": [
            # Primary: IMF monthly FX vs USD via your existing helper
            {
                "provider": "imf",
                "dataset": "IFS",
                "indicator": "FX_USD",
                "func": "imf_fx_to_usd_monthly",  # re-use legacy IMF helper
                "freq": "M",
                "transform": "none",
                "notes": "Monthly FX vs USD from IMF IFS via imf_fx_to_usd_monthly(iso2).",
            },
            # Euro area / ECB: EUR/USD from ECB (for eurozone countries)
            {
                "provider": "ecb",
                "dataset": "ECB_FX",
                "indicator": "EURUSD",
                "func": None,           # e.g. ecb_fx_rate('EUR', 'USD') later
                "freq": "D",
                "transform": "none",
                "notes": "ECB FX reference rate (EUR/USD) for euro area.",
            },
            # Fallback: DBnomics FX series (optional)
            {
                "provider": "dbnomics",
                "dataset": "FX",
                "indicator": None,
                "func": None,
                "freq": "M",
                "transform": "none",
                "notes": "Optional FX fallback via DBnomics; requires provider wiring.",
            },
        ],
    },

    # -------------------------------------------------------------------------
    # 2) GDP Growth Rate – Quarterly
    # -------------------------------------------------------------------------
    "gdp_growth_quarterly": {
        "key": "gdp_growth_quarterly",
        "label": "GDP Growth Rate (Quarterly)",
        "unit": "percent",
        "preferred_freq": "Q",
        "max_age_years": 2,
        "sources": [
            # Preferred: use your existing IMF helper that already returns quarterly GDP growth
            {
                "provider": "imf",
                "dataset": "IFS",
                "indicator": "GDP_GROWTH_Q",
                "func": "imf_gdp_growth_quarterly",  # reuse legacy helper
                "freq": "Q",
                "transform": "none",                  # function already returns growth
                "notes": "Quarterly GDP growth from IMF via imf_gdp_growth_quarterly(iso2).",
            },
            # EU: Eurostat quarterly chain-linked real GDP (if you later want to compute from levels)
            {
                "provider": "eurostat",
                "dataset": "namq_10_gdp",
                "indicator": "B1GQ",
                "func": None,
                "freq": "Q",
                "transform": "qoq",
                "notes": "Quarterly GDP (B1GQ) via Eurostat; compute QoQ growth.",
            },
            # Fallback: annual growth from GMD if quarterly is not available
            {
                "provider": "gmd",
                "dataset": "GMD",
                "indicator": "gdp_real_growth",
                "func": None,
                "freq": "A",
                "transform": "none",
                "notes": "Fallback to annual real GDP growth from Global Macro Database.",
            },
        ],
    },

    # -------------------------------------------------------------------------
    # 3) GDP Annual Growth Rate – YoY
    # -------------------------------------------------------------------------
    "gdp_growth_annual": {
        "key": "gdp_growth_annual",
        "label": "GDP Annual Growth Rate",
        "unit": "percent",
        "preferred_freq": "A",
        "max_age_years": 3,
        "sources": [
            # Preferred: GMD harmonised real GDP growth
            {
                "provider": "gmd",
                "dataset": "GMD",
                "indicator": "gdp_real_growth",
                "func": None,
                "freq": "A",
                "transform": "none",
                "notes": "Harmonised annual real GDP growth from GMD.",
            },
            # Fallback: IMF WEO real GDP level -> compute YoY growth
            {
                "provider": "imf",
                "dataset": "WEO",
                "indicator": "NGDP_R",
                "func": None,
                "freq": "A",
                "transform": "yoy",
                "notes": "IMF WEO real GDP; compute YoY growth.",
            },
            # Fallback: World Bank WDI real GDP
            {
                "provider": "world_bank",
                "dataset": "WDI",
                "indicator": "NY.GDP.MKTP.KD",
                "func": None,
                "freq": "A",
                "transform": "yoy",
                "notes": "World Bank real GDP (constant US$); compute YoY.",
            },
        ],
    },

    # -------------------------------------------------------------------------
    # 4) Unemployment Rate
    # -------------------------------------------------------------------------
    "unemployment_rate": {
        "key": "unemployment_rate",
        "label": "Unemployment Rate",
        "unit": "percent",
        "preferred_freq": "M",  # prefer monthly if available, else A
        "max_age_years": 2,
        "sources": [
            # EU: Eurostat monthly unemployment
            {
                "provider": "eurostat",
                "dataset": "une_rt_m",
                "indicator": "UNEMP_RATE",
                "func": None,
                "freq": "M",
                "transform": "none",
                "notes": "Monthly unemployment rate via Eurostat for EU countries.",
            },
            # OECD: labour market stats (unemployment rate)
            {
                "provider": "oecd",
                "dataset": "MEI",
                "indicator": "UNEMP_RATE",
                "func": None,
                "freq": "M",
                "transform": "none",
                "notes": "OECD unemployment rate series for OECD members.",
            },
            # Fallback: annual unemployment rate from GMD
            {
                "provider": "gmd",
                "dataset": "GMD",
                "indicator": "unemployment_rate",
                "func": None,
                "freq": "A",
                "transform": "none",
                "notes": "Annual unemployment rate from Global Macro Database.",
            },
        ],
    },

    # -------------------------------------------------------------------------
    # 5) Inflation Rate – CPI YoY
    # -------------------------------------------------------------------------
    "inflation_yoy": {
        "key": "inflation_yoy",
        "label": "Inflation Rate (CPI YoY)",
        "unit": "percent",
        "preferred_freq": "M",
        "max_age_years": 1,
        "sources": [
            # Preferred: IMF CPI YoY via existing helper
            {
                "provider": "imf",
                "dataset": "IFS",
                "indicator": "CPI_YOY",
                "func": "imf_cpi_yoy_monthly",
                "freq": "M",
                "transform": "none",
                "notes": "CPI YoY from IMF IFS via imf_cpi_yoy_monthly(iso2).",
            },
            # EU: Eurostat HICP index -> compute YoY if needed
            {
                "provider": "eurostat",
                "dataset": "prc_hicp_midx",
                "indicator": "CP00",
                "func": None,
                "freq": "M",
                "transform": "yoy",
                "notes": "Eurostat HICP index (CP00); compute YoY inflation.",
            },
            # OECD: CPI index -> compute YoY
            {
                "provider": "oecd",
                "dataset": "MEI",
                "indicator": "CPIIDX",
                "func": None,
                "freq": "M",
                "transform": "yoy",
                "notes": "OECD CPI index; compute YoY inflation.",
            },
            # Fallback: annual CPI inflation from WB
            {
                "provider": "world_bank",
                "dataset": "WDI",
                "indicator": "FP.CPI.TOTL.ZG",
                "func": None,
                "freq": "A",
                "transform": "none",
                "notes": "World Bank annual CPI inflation (%).",
            },
        ],
    },

    # -------------------------------------------------------------------------
    # 6) Inflation Rate – MoM
    # -------------------------------------------------------------------------
    "inflation_mom": {
        "key": "inflation_mom",
        "label": "Inflation Rate (CPI MoM)",
        "unit": "percent",
        "preferred_freq": "M",
        "max_age_years": 1,
        "sources": [
            # Preferred: IMF CPI index -> compute MoM
            {
                "provider": "imf",
                "dataset": "IFS",
                "indicator": "CPI_INDEX",
                "func": None,
                "freq": "M",
                "transform": "mom",
                "notes": "Monthly CPI index from IMF; compute MoM change.",
            },
            # EU: Eurostat HICP index -> compute MoM
            {
                "provider": "eurostat",
                "dataset": "prc_hicp_midx",
                "indicator": "CP00",
                "func": None,
                "freq": "M",
                "transform": "mom",
                "notes": "Eurostat HICP index; compute MoM inflation.",
            },
            # OECD: CPI index -> compute MoM
            {
                "provider": "oecd",
                "dataset": "MEI",
                "indicator": "CPIIDX",
                "func": None,
                "freq": "M",
                "transform": "mom",
                "notes": "OECD CPI index; compute MoM inflation.",
            },
        ],
    },

    # -------------------------------------------------------------------------
    # 7) Interest Rate – Policy Rate
    # -------------------------------------------------------------------------
    "policy_rate": {
        "key": "policy_rate",
        "label": "Policy Interest Rate",
        "unit": "percent",
        "preferred_freq": "M",
        "max_age_years": 1,
        "sources": [
            # IMF: policy rate series in IFS (implementation later)
            {
                "provider": "imf",
                "dataset": "IFS",
                "indicator": "POLICY_RATE",
                "func": "imf_policy_rate_monthly",  # if you have this helper
                "freq": "M",
                "transform": "none",
                "notes": "Central bank policy rate from IMF IFS.",
            },
            # Euro area: ECB main refinancing / deposit facility rate
            {
                "provider": "ecb",
                "dataset": "ECB_MRO",
                "indicator": "MAIN_REFI",
                "func": None,
                "freq": "M",
                "transform": "none",
                "notes": "ECB main policy rate; used for euro area.",
            },
            # DBnomics / local central bank: optional
            {
                "provider": "dbnomics",
                "dataset": "CB_POLICY",
                "indicator": None,
                "func": None,
                "freq": "M",
                "transform": "none",
                "notes": "Optional policy rate fallback via DBnomics or local provider.",
            },
        ],
    },

    # -------------------------------------------------------------------------
    # 8) Balance of Trade
    # -------------------------------------------------------------------------
    "trade_balance": {
        "key": "trade_balance",
        "label": "Balance of Trade",
        "unit": "local_currency",  # or "USD", "EUR bn" depending on provider
        "preferred_freq": "M",
        "max_age_years": 3,
        "sources": [
            # IMF: trade balance (goods & services) series
            {
                "provider": "imf",
                "dataset": "IFS",
                "indicator": "TRADE_BALANCE",
                "func": None,
                "freq": "Q",   # often Q or M
                "transform": "none",
                "notes": "IMF IFS trade balance in local currency or USD.",
            },
            # Eurostat: detailed external trade
            {
                "provider": "eurostat",
                "dataset": "ext_lt_intertrd",
                "indicator": "TRADE_BALANCE",
                "func": None,
                "freq": "M",
                "transform": "none",
                "notes": "Monthly trade balance for EU countries via Eurostat.",
            },
            # Fallback: WB annual exports/imports -> derive annual trade balance
            {
                "provider": "world_bank",
                "dataset": "WDI",
                "indicator": "TRADE_BALANCE_DERIVED",
                "func": None,
                "freq": "A",
                "transform": "none",
                "notes": "Use exports/imports from WDI to derive annual trade balance.",
            },
        ],
    },

    # -------------------------------------------------------------------------
    # 9) Current Account – Level
    # -------------------------------------------------------------------------
    "current_account": {
        "key": "current_account",
        "label": "Current Account Balance",
        "unit": "USD",    # or local currency depending on provider
        "preferred_freq": "Q",
        "max_age_years": 3,
        "sources": [
            # IMF: BoP current account balance
            {
                "provider": "imf",
                "dataset": "IFS",
                "indicator": "CA_BALANCE",
                "func": None,
                "freq": "Q",
                "transform": "none",
                "notes": "IMF current account balance (often in USD).",
            },
            # World Bank: annual current account balance (US$)
            {
                "provider": "world_bank",
                "dataset": "WDI",
                "indicator": "BN.CAB.XOKA.CD",
                "func": None,
                "freq": "A",
                "transform": "none",
                "notes": "World Bank current account balance (BoP, current US$).",
            },
            # GMD: annual current account if provided
            {
                "provider": "gmd",
                "dataset": "GMD",
                "indicator": "current_account",
                "func": None,
                "freq": "A",
                "transform": "none",
                "notes": "Annual current account from GMD.",
            },
        ],
    },

    # -------------------------------------------------------------------------
    # 10) Current Account to GDP – %
    # -------------------------------------------------------------------------
    "current_account_pct_gdp": {
        "key": "current_account_pct_gdp",
        "label": "Current Account to GDP",
        "unit": "percent_of_gdp",
        "preferred_freq": "A",
        "max_age_years": 5,
        "sources": [
            # Preferred: World Bank direct CA % GDP
            {
                "provider": "world_bank",
                "dataset": "WDI",
                "indicator": "BN.CAB.XOKA.GD.ZS",
                "func": None,
                "freq": "A",
                "transform": "none",
                "notes": "World Bank current account balance (% of GDP).",
            },
            # GMD: harmonised CA % GDP
            {
                "provider": "gmd",
                "dataset": "GMD",
                "indicator": "current_account_pct_gdp",
                "func": None,
                "freq": "A",
                "transform": "none",
                "notes": "Current account % of GDP from GMD where available.",
            },
            # Derived: CA level / nominal GDP from IMF/WB
            {
                "provider": "imf",
                "dataset": "IFS+WEO",
                "indicator": "CA/GDP",
                "func": None,
                "freq": "A",
                "transform": "ratio",
                "notes": "Compute (CA / nominal GDP) * 100 when direct ratio missing.",
            },
        ],
    },

    # -------------------------------------------------------------------------
    # 11) Government Debt to GDP – %
    # -------------------------------------------------------------------------
    "gov_debt_pct_gdp": {
        "key": "gov_debt_pct_gdp",
        "label": "Government Debt to GDP",
        "unit": "percent_of_gdp",
        "preferred_freq": "A",
        "max_age_years": 5,
        "sources": [
            # Preferred: GMD general government gross debt % GDP
            {
                "provider": "gmd",
                "dataset": "GMD",
                "indicator": "gov_debt_pct_gdp",
                "func": None,
                "freq": "A",
                "transform": "none",
                "notes": "General government gross debt % of GDP (harmonised).",
            },
            # IMF WEO: general government gross debt (% of GDP)
            {
                "provider": "imf",
                "dataset": "WEO",
                "indicator": "GGXWDG_NGDP",
                "func": None,
                "freq": "A",
                "transform": "none",
                "notes": "IMF WEO general government gross debt (% of GDP).",
            },
            # Eurostat: EU general government gross debt % GDP
            {
                "provider": "eurostat",
                "dataset": "gov_10a_ggdebt",
                "indicator": "GG_DEBT_PCT_GDP",
                "func": None,
                "freq": "A",
                "transform": "none",
                "notes": "Eurostat general government gross debt % GDP (Maastricht).",
            },
            # Fallback: World Bank central gov debt % GDP
            {
                "provider": "world_bank",
                "dataset": "WDI",
                "indicator": "GC.DOD.TOTL.GD.ZS",
                "func": None,
                "freq": "A",
                "transform": "none",
                "notes": "World Bank central government debt % GDP as last resort.",
            },
        ],
    },

    # -------------------------------------------------------------------------
    # 12) Government Budget – % of GDP
    # -------------------------------------------------------------------------
    "gov_budget_pct_gdp": {
        "key": "gov_budget_pct_gdp",
        "label": "Government Budget Balance",
        "unit": "percent_of_gdp",
        "preferred_freq": "A",
        "max_age_years": 5,
        "sources": [
            # Preferred: GMD government balance / net lending/borrowing % GDP
            {
                "provider": "gmd",
                "dataset": "GMD",
                "indicator": "gov_balance_pct_gdp",
                "func": None,
                "freq": "A",
                "transform": "none",
                "notes": "General government budget balance (% of GDP) from GMD.",
            },
            # IMF WEO: general government net lending/borrowing (% of GDP)
            {
                "provider": "imf",
                "dataset": "WEO",
                "indicator": "GGXWDN_NGDP",
                "func": None,
                "freq": "A",
                "transform": "none",
                "notes": "IMF WEO general government net lending/borrowing % GDP.",
            },
            # Eurostat: EU government deficit/surplus % of GDP
            {
                "provider": "eurostat",
                "dataset": "gov_10a_main",
                "indicator": "NET_LEND_BORR_PCT_GDP",
                "func": None,
                "freq": "A",
                "transform": "none",
                "notes": "Eurostat general gov net lending/borrowing % of GDP.",
            },
        ],
    },
}
