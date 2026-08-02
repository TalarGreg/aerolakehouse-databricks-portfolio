-- OurAirports Bronze tables — full schema with partitioning
-- Partitioned by _bronze_ingestion_date for efficient Silver reads:
--   SELECT * WHERE _bronze_ingestion_date = (SELECT MAX(...) FROM ...)
--
-- Run once per catalog environment BEFORE first pipeline execution.
-- Override target_catalog for another environment before running the DDL.

DECLARE OR REPLACE VARIABLE target_catalog STRING DEFAULT 'dev_alh';

CREATE TABLE IF NOT EXISTS IDENTIFIER(target_catalog || '.bronze.raw_our_airports_airports') (
    -- source columns (all STRING — Bronze = raw, no type casting)
    id                  STRING  COMMENT 'Unique OurAirports ID',
    ident               STRING  COMMENT 'ICAO-style airport identifier',
    type                STRING  COMMENT 'Airport type (small_airport, large_airport, heliport, etc.)',
    name                STRING  COMMENT 'Official airport name',
    latitude_deg        STRING,
    longitude_deg       STRING,
    elevation_ft        STRING,
    continent           STRING,
    iso_country         STRING  COMMENT 'ISO 3166-1 alpha-2 country code',
    iso_region          STRING  COMMENT 'ISO 3166-2 region code',
    municipality        STRING,
    scheduled_service   STRING,
    icao_code           STRING,
    iata_code           STRING,
    gps_code            STRING,
    local_code          STRING,
    home_link           STRING,
    wikipedia_link      STRING,
    keywords            STRING,
    -- technical columns
    _bronze_ingestion_date  DATE        COMMENT 'Partition key — date when file was loaded',
    _bronze_ingest_ts       TIMESTAMP   COMMENT 'Exact ingestion timestamp (UTC)',
    _bronze_source          STRING      COMMENT 'Source name from bronze_sources.yml',
    _source_file_path       STRING      COMMENT 'Full path of source file in Volume'
)
USING DELTA
PARTITIONED BY (_bronze_ingestion_date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'OurAirports airports reference data — appended each time source file changes';

-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS IDENTIFIER(target_catalog || '.bronze.raw_our_airports_runways') (
    id                          STRING  COMMENT 'Unique OurAirports runway ID',
    airport_ref                 STRING  COMMENT 'Foreign key to airports.id',
    airport_ident               STRING  COMMENT 'ICAO-style airport identifier',
    length_ft                   STRING,
    width_ft                    STRING,
    surface                     STRING  COMMENT 'Surface material (ASPH, CONC, TURF, etc.)',
    lighted                     STRING,
    closed                      STRING,
    le_ident                    STRING  COMMENT 'Low-end runway identifier',
    le_latitude_deg             STRING,
    le_longitude_deg            STRING,
    le_elevation_ft             STRING,
    le_heading_degT             STRING,
    le_displaced_threshold_ft   STRING,
    he_ident                    STRING  COMMENT 'High-end runway identifier',
    he_latitude_deg             STRING,
    he_longitude_deg            STRING,
    he_elevation_ft             STRING,
    he_heading_degT             STRING,
    he_displaced_threshold_ft   STRING,
    -- technical columns
    _bronze_ingestion_date  DATE        COMMENT 'Partition key — date when file was loaded',
    _bronze_ingest_ts       TIMESTAMP   COMMENT 'Exact ingestion timestamp (UTC)',
    _bronze_source          STRING      COMMENT 'Source name from bronze_sources.yml',
    _source_file_path       STRING      COMMENT 'Full path of source file in Volume'
)
USING DELTA
PARTITIONED BY (_bronze_ingestion_date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'OurAirports runways reference data — appended each time source file changes';

-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS IDENTIFIER(target_catalog || '.bronze.raw_our_airports_countries') (
    id              STRING  COMMENT 'Unique OurAirports country ID',
    code            STRING  COMMENT 'ISO 3166-1 alpha-2 country code',
    name            STRING,
    continent       STRING,
    wikipedia_link  STRING,
    keywords        STRING,
    -- technical columns
    _bronze_ingestion_date  DATE        COMMENT 'Partition key — date when file was loaded',
    _bronze_ingest_ts       TIMESTAMP   COMMENT 'Exact ingestion timestamp (UTC)',
    _bronze_source          STRING      COMMENT 'Source name from bronze_sources.yml',
    _source_file_path       STRING      COMMENT 'Full path of source file in Volume'
)
USING DELTA
PARTITIONED BY (_bronze_ingestion_date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'OurAirports countries reference data — appended each time source file changes';

-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS IDENTIFIER(target_catalog || '.bronze.raw_our_airports_regions') (
    id              STRING  COMMENT 'Unique OurAirports region ID',
    code            STRING  COMMENT 'ISO 3166-2 region code',
    local_code      STRING,
    name            STRING,
    continent       STRING,
    iso_country     STRING  COMMENT 'ISO 3166-1 alpha-2 country code',
    wikipedia_link  STRING,
    keywords        STRING,
    -- technical columns
    _bronze_ingestion_date  DATE        COMMENT 'Partition key — date when file was loaded',
    _bronze_ingest_ts       TIMESTAMP   COMMENT 'Exact ingestion timestamp (UTC)',
    _bronze_source          STRING      COMMENT 'Source name from bronze_sources.yml',
    _source_file_path       STRING      COMMENT 'Full path of source file in Volume'
)
USING DELTA
PARTITIONED BY (_bronze_ingestion_date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'OurAirports regions reference data — appended each time source file changes';
