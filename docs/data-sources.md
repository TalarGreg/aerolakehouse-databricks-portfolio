# Data Source Contracts

| Source | Landing data | Bronze Delta output | Current scope |
|---|---|---|---|
| OpenSky Network | JSON state-vector snapshots | `raw_opensky_states` | Aircraft position and movement observations |
| OpenWeatherMap | Current and forecast JSON | `raw_owm_current`, `raw_owm_forecast` | WAW and GDN weather context |
| Aviationstack | Flight JSON over HTTPS | `raw_aviationstack_flights` | Optional schedule context with a 48-hour refresh gate |
| Aviation Weather Center | METAR, TAF, PIREP, and SIGMET JSON | `raw_awc_metar` | METAR is promoted; other products remain in landing storage |
| OurAirports | Versioned CSV extracts | Four reference tables | Airports, runways, countries, and regions |

Raw provider responses are not committed to this repository. Users are
responsible for reviewing each provider's current terms, attribution
requirements, rate limits, and redistribution rules before running the project.

## Known Contract Boundary

TAF, PIREP, and SIGMET/AIRMET have distinct response schemas. They are retained
under product-specific Volume paths but intentionally excluded from the METAR
Bronze contract. Adding them requires separate schemas, checkpoints, tables,
quality rules, and tests.
