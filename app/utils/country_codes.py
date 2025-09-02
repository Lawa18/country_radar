from typing import Optional, Dict
import pycountry

# Common aliases that pycountry.search_fuzzy sometimes misses or returns odd matches
_ALIAS_TO_ALPHA2 = {
    "uk": "GB",
    "united kingdom": "GB",
    "great britain": "GB",
    "britain": "GB",
    "england": "GB",
    "united states": "US",
    "u.s.": "US",
    "usa": "US",
    "south korea": "KR",
    "north korea": "KP",
    "czech republic": "CZ",
    "czechia": "CZ",
    "russia": "RU",
    "ivory coast": "CI",
    "cote d'ivoire": "CI",
    "türkiye": "TR",
    "turkey": "TR",
    "taiwan": "TW",
    "hong kong": "HK",
    "macau": "MO",
    "vietnam": "VN",
    "laos": "LA",
}

# Lightweight currency map for common countries. Fallback = "USD".
# (You can expand this over time; this is enough to boot.)
_ALPHA2_TO_CCY = {
    # EUR area + a few that use EUR
    "DE":"EUR","FR":"EUR","IT":"EUR","ES":"EUR","PT":"EUR","NL":"EUR","BE":"EUR","LU":"EUR","IE":"EUR",
    "FI":"EUR","AT":"EUR","GR":"EUR","CY":"EUR","MT":"EUR","EE":"EUR","LV":"EUR","LT":"EUR","SI":"EUR",
    "SK":"EUR","HR":"EUR",
    # Nordics / Europe
    "SE":"SEK","NO":"NOK","DK":"DKK","IS":"ISK","CH":"CHF","GB":"GBP","PL":"PLN","CZ":"CZK","HU":"HUF",
    "RO":"RON","BG":"BGN","UA":"UAH",
    # Americas
    "US":"USD","CA":"CAD","MX":"MXN","BR":"BRL","AR":"ARS","CL":"CLP","CO":"COP","PE":"PEN","UY":"UYU",
    # MENA
    "TR":"TRY","SA":"SAR","AE":"AED","QA":"QAR","OM":"OMR","KW":"KWD","BH":"BHD","EG":"EGP","MA":"MAD",
    "DZ":"DZD","TN":"TND","IL":"ILS",
    # Africa
    "ZA":"ZAR","NG":"NGN","KE":"KES","GH":"GHS","ET":"ETB",
    # Asia
    "JP":"JPY","CN":"CNY","KR":"KRW","IN":"INR","ID":"IDR","MY":"MYR","SG":"SGD","TH":"THB","PH":"PHP",
    "VN":"VND","TW":"TWD","HK":"HKD"
}

def _from_alias(name: str) -> Optional[str]:
    key = name.strip().lower()
    return _ALIAS_TO_ALPHA2.get(key)

def resolve_country_codes(country: str) -> Optional[Dict[str, str]]:
    """
    Accepts country name or ISO codes; returns {'name','iso_alpha_2','iso_alpha_3','iso_numeric'}.
    """
    if not country or not country.strip():
        return None

    s = country.strip()
    # Direct ISO2/ISO3 match
    if len(s) == 2:
        try:
            c = pycountry.countries.get(alpha_2=s.upper())
            if c:
                return {
                    "name": getattr(c, "name", s),
                    "iso_alpha_2": c.alpha_2,
                    "iso_alpha_3": c.alpha_3,
                    "iso_numeric": getattr(c, "numeric", ""),
                }
        except Exception:
            pass
    if len(s) == 3:
        try:
            c = pycountry.countries.get(alpha_3=s.upper())
            if c:
                return {
                    "name": getattr(c, "name", s),
                    "iso_alpha_2": c.alpha_2,
                    "iso_alpha_3": c.alpha_3,
                    "iso_numeric": getattr(c, "numeric", ""),
                }
        except Exception:
            pass

    # Alias → ISO2
    a2 = _from_alias(s)
    if a2:
        c = pycountry.countries.get(alpha_2=a2)
        if c:
            return {
                "name": getattr(c, "name", a2),
                "iso_alpha_2": c.alpha_2,
                "iso_alpha_3": c.alpha_3,
                "iso_numeric": getattr(c, "numeric", ""),
            }

    # Fuzzy match by name
    try:
        results = pycountry.countries.search_fuzzy(s)
        if results:
            c = results[0]
            return {
                "name": getattr(c, "name", s),
                "iso_alpha_2": c.alpha_2,
                "iso_alpha_3": c.alpha_3,
                "iso_numeric": getattr(c, "numeric", ""),
            }
    except Exception:
        pass

    return None

def resolve_currency_code(iso_alpha_2: str) -> str:
    """
    Map ISO2 country -> currency code for LCU display. Fallback 'USD'.
    """
    if not iso_alpha_2:
        return "USD"
    return _ALPHA2_TO_CCY.get(iso_alpha_2.upper(), "USD")
