# Country Radar API Scaffold

A modular FastAPI scaffold for country macroeconomic indicators with enhanced multi-provider data sourcing.

## Enhanced Data Providers

The API now intelligently sources data from multiple providers with automatic fallback:

### Data Source Priority

1. **IMF International Financial Statistics (IFS)** - Monthly/quarterly data when available
   - CPI inflation (monthly)
   - Exchange rates (monthly) 
   - International reserves (monthly)
   - Unemployment rates (monthly)
   - GDP growth (quarterly)
   - Policy rates (monthly)

2. **Eurostat** - Monthly data for EU/EEA countries
   - HICP inflation (monthly)
   - Unemployment rates (monthly) 
   - Government debt ratios (annual)

3. **European Central Bank (ECB)** - Policy rates for Euro area
   - Main Refinancing Operations rate (monthly)

4. **World Bank WDI** - Annual data as reliable fallback
   - All indicators available as backup

### Monthly Data Advantage

When available, the API now provides monthly indicators instead of just annual data, enabling:
- More timely economic analysis
- Better trend identification
- Higher frequency monitoring for AI agents

### API Endpoints

- `GET /country-data?country={name}` - Enhanced country indicators with multi-provider sourcing
- `GET /v1/debt?country={name}` - Government debt analysis with Eurostat/IMF priority

The system gracefully handles API failures and automatically falls back to ensure reliability while maximizing data timeliness and accuracy.
