import pytest
from app.services.indicator_service import build_country_payload, _merge_indicator_data
from app.providers.eurostat_provider import EURO_AREA_ISO2

def test_country_payload_structure():
    """Test that the country payload has the expected structure."""
    result = build_country_payload("Germany")
    
    # Should not have error
    assert "error" not in result
    
    # Should have all expected top-level keys
    expected_keys = {"country", "iso_codes", "imf_data", "government_debt", "nominal_gdp", "debt_to_gdp", "additional_indicators"}
    assert set(result.keys()) == expected_keys
    
    # Check country identification
    assert result["country"] == "Germany"
    assert result["iso_codes"]["iso_alpha_2"] == "DE"
    assert result["iso_codes"]["iso_alpha_3"] == "DEU"
    
    # Check that imf_data has all expected indicators
    imf_data = result["imf_data"]
    expected_indicators = {
        "CPI", "FX Rate", "Interest Rate (Policy)", "Reserves (USD)",
        "GDP Growth (%)", "Unemployment (%)", "Current Account Balance (% of GDP)", 
        "Government Effectiveness"
    }
    assert set(imf_data.keys()) == expected_indicators
    
    # Check that series indicators have the right structure
    for indicator in ["CPI", "FX Rate", "Interest Rate (Policy)", "Reserves (USD)"]:
        assert "latest" in imf_data[indicator]
        assert "series" in imf_data[indicator]
        assert "value" in imf_data[indicator]["latest"]
        assert "date" in imf_data[indicator]["latest"]
        assert "source" in imf_data[indicator]["latest"]
    
    # Check that latest-only indicators have the right structure
    for indicator in ["GDP Growth (%)", "Unemployment (%)", "Current Account Balance (% of GDP)", "Government Effectiveness"]:
        assert "value" in imf_data[indicator]
        assert "date" in imf_data[indicator]
        assert "source" in imf_data[indicator]

def test_merge_indicator_data_priority():
    """Test that the merge function prioritizes IMF > Eurostat > World Bank correctly."""
    
    # Mock data structures
    imf_data = {
        "CPI_YoY": {"2024-01": 2.5, "2024-02": 2.6}
    }
    
    eurostat_data = {
        "CPI_YoY": {"2024-01": 2.4, "2024-02": 2.5},
        "Unemployment_Rate": {"2024-01": 7.1, "2024-02": 7.0}
    }
    
    wb_data = {
        "FP.CPI.TOTL.ZG": [{"date": "2023", "value": 6.0}],
        "SL.UEM.TOTL.ZS": [{"date": "2023", "value": 7.5}]
    }
    
    # Test for EU country (should prefer IMF > Eurostat > World Bank)
    result_eu = _merge_indicator_data(imf_data, eurostat_data, wb_data, "DE")
    
    # CPI should come from IMF (highest priority)
    assert result_eu["CPI"]["latest"]["source"] == "IMF IFS"
    assert result_eu["CPI"]["latest"]["value"] == 2.6  # Latest from IMF
    
    # Unemployment should come from Eurostat (IMF doesn't have it, EU country)
    assert result_eu["Unemployment (%)"]["source"] == "Eurostat"
    assert result_eu["Unemployment (%)"]["value"] == 7.0  # Latest from Eurostat
    
    # Test for non-EU country (should prefer IMF > World Bank, skip Eurostat)
    result_non_eu = _merge_indicator_data(imf_data, {}, wb_data, "US")
    
    # CPI should come from IMF
    assert result_non_eu["CPI"]["latest"]["source"] == "IMF IFS"
    
    # Unemployment should come from World Bank (no Eurostat for non-EU)
    assert result_non_eu["Unemployment (%)"]["source"] == "World Bank WDI"

def test_eu_country_detection():
    """Test that EU countries are correctly identified."""
    
    # Test some EU countries
    assert "DE" in EURO_AREA_ISO2  # Germany
    assert "FR" in EURO_AREA_ISO2  # France  
    assert "ES" in EURO_AREA_ISO2  # Spain
    assert "GB" in EURO_AREA_ISO2  # United Kingdom
    
    # Test some non-EU countries
    assert "US" not in EURO_AREA_ISO2  # United States
    assert "JP" not in EURO_AREA_ISO2  # Japan
    assert "CN" not in EURO_AREA_ISO2  # China

def test_invalid_country():
    """Test handling of invalid country names."""
    result = build_country_payload("NonexistentCountry")
    assert "error" in result
    assert result["error"] == "Invalid country name"

if __name__ == "__main__":
    test_country_payload_structure()
    test_merge_indicator_data_priority()
    test_eu_country_detection()
    test_invalid_country()
    print("All tests passed!")