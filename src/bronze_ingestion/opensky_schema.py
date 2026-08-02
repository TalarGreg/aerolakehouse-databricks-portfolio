"""OpenSky Network API — single source of truth for state vector schema.

All components that need OpenSky field definitions import from here:
  - bronze_opensky_ingest.py  → OPENSKY_INGEST_FIELDS (names only, sensors excluded)
  - flatten.py                → OPENSKY_STATE_FIELDS  (names + Spark types)
  - bronze_loader.py          → OPENSKY_STATE_FIELDS  (for output column list)

API reference: https://openskynetwork.github.io/opensky-api/rest.html#response
"""

from __future__ import annotations

# Number of fields returned by OpenSky REST API with extended=1.
# Used for schema drift detection in the ingest notebook.
OPENSKY_API_FIELD_COUNT = 18

# Index of the `sensors` field in the raw API response array.
# sensors is always null for non-feeder accounts and is dropped by map_states()
# in the ingest notebook. It remains in the canonical output definition so the
# Bronze schema stays stable and older compatible structs can expose it as null.
OPENSKY_SENSORS_INDEX = 12

# Complete state vector field definitions — name + Spark cast type.
# Order matches the OpenSky REST API field order exactly (extended=1).
#
# The ingest notebook maps source positions to named dictionaries. The list
# order below remains aligned with the upstream API for one-source-of-truth
# maintenance and schema-drift checks.
OPENSKY_STATE_FIELDS: list[tuple[str, str]] = [
    ("icao24",          "string"),   # 0  — ICAO 24-bit address (hex)
    ("callsign",        "string"),   # 1  — callsign (8 chars, space-padded)
    ("origin_country",  "string"),   # 2  — country of origin
    ("time_position",   "long"),     # 3  — Unix timestamp of last position update
    ("last_contact",    "long"),     # 4  — Unix timestamp of last message
    ("longitude",       "double"),   # 5  — WGS-84 longitude (deg), null if unknown
    ("latitude",        "double"),   # 6  — WGS-84 latitude (deg), null if unknown
    ("baro_altitude",   "double"),   # 7  — barometric altitude (m), null if on ground
    ("on_ground",       "boolean"),  # 8  — true if aircraft is on ground
    ("velocity",        "double"),   # 9  — ground speed (m/s), null if unknown
    ("true_track",      "double"),   # 10 — true track (deg CW from north), null if unknown
    ("vertical_rate",   "double"),   # 11 — vertical rate (m/s), positive = climbing
    ("sensors",         "string"),   # 12 — receiver IDs (null for non-feeder accounts)
    ("geo_altitude",    "double"),   # 13 — geometric altitude (m), null if unknown
    ("squawk",          "string"),   # 14 — transponder code (Mode A), null if unknown
    ("spi",             "boolean"),  # 15 — Special Purpose Indicator
    ("position_source", "integer"),  # 16 — 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM
    ("category",        "integer"),  # 17 — aircraft category (extended=1 only)
                                     #       2=Light, 4=Large, 6=Heavy, 8=Rotorcraft, 14=UAV
]

# Field names only, with sensors excluded.
# Used by map_states() in bronze_opensky_ingest.py to zip API response arrays
# into named dicts. sensors is excluded because map_states() drops it at index 12
# via: trimmed = state[:12] + state[13:]
OPENSKY_INGEST_FIELDS: list[str] = [
    name for name, _ in OPENSKY_STATE_FIELDS
    if name != "sensors"
]
