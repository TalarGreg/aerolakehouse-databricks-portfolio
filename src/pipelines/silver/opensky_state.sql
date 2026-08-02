-- Normalize append-only OpenSky state vectors and route critical failures to quarantine.
CREATE TEMPORARY VIEW opensky_state_prepared AS
WITH normalized AS (
    SELECT
        icao24 AS raw_icao24,
        callsign AS raw_callsign,
        origin_country AS raw_origin_country,
        ingest_ts AS raw_ingest_ts,
        api_time AS raw_api_time,
        time_position AS raw_time_position,
        last_contact AS raw_last_contact,
        longitude AS raw_longitude,
        latitude AS raw_latitude,
        baro_altitude AS raw_baro_altitude,
        geo_altitude AS raw_geo_altitude,
        velocity AS raw_velocity,
        true_track AS raw_true_track,
        vertical_rate AS raw_vertical_rate,
        on_ground AS raw_on_ground,
        squawk AS raw_squawk,
        spi AS raw_spi,
        position_source AS raw_position_source,
        category AS raw_category,
        states_count AS raw_states_count,

        NULLIF(UPPER(TRIM(CAST(icao24 AS STRING))), '') AS icao24,
        NULLIF(UPPER(TRIM(CAST(callsign AS STRING))), '') AS callsign,
        NULLIF(UPPER(TRIM(CAST(origin_country AS STRING))), '') AS origin_country,
        TRY_CAST(ingest_ts AS TIMESTAMP) AS source_ingest_ts,
        TIMESTAMP_SECONDS(TRY_CAST(api_time AS BIGINT)) AS snapshot_ts,
        TIMESTAMP_SECONDS(TRY_CAST(time_position AS BIGINT)) AS position_ts,
        TIMESTAMP_SECONDS(TRY_CAST(last_contact AS BIGINT)) AS last_contact_ts,
        CAST(longitude AS DOUBLE) AS longitude,
        CAST(latitude AS DOUBLE) AS latitude,
        CAST(baro_altitude AS DOUBLE) AS baro_altitude_m,
        CAST(geo_altitude AS DOUBLE) AS geo_altitude_m,
        CAST(velocity AS DOUBLE) AS velocity_mps,
        CAST(true_track AS DOUBLE) AS true_track_deg,
        CAST(vertical_rate AS DOUBLE) AS vertical_rate_mps,
        CAST(
            latitude IS NOT NULL AND longitude IS NOT NULL
            AS BOOLEAN
        ) AS position_available,
        CAST(on_ground AS BOOLEAN) AS on_ground,
        CAST(squawk AS STRING) AS squawk,
        CAST(spi AS BOOLEAN) AS spi,
        CAST(position_source AS INT) AS position_source,
        CAST(category AS INT) AS category,
        CAST(_bronze_ingest_ts AS TIMESTAMP) AS _bronze_ingest_ts,
        CAST(_bronze_record_hash AS STRING) AS _bronze_record_hash,
        CAST(_source_file_path AS STRING) AS _source_file_path,
        CURRENT_TIMESTAMP() AS silver_processed_ts
    FROM STREAM(bronze.raw_opensky_states)
),
validated AS (
    SELECT
        *,
        TIMESTAMPDIFF(SECOND, position_ts, snapshot_ts) AS position_age_seconds,
        TIMESTAMPDIFF(SECOND, last_contact_ts, snapshot_ts) AS last_contact_age_seconds,

        icao24 IS NOT NULL
            AND icao24 RLIKE '^[0-9A-F]{6}$' AS valid_icao24,
        snapshot_ts IS NOT NULL AS valid_snapshot_ts,
        source_ingest_ts IS NOT NULL AS valid_source_ingest_ts,
        callsign IS NULL OR LENGTH(callsign) <= 8 AS valid_callsign,
        (latitude IS NULL AND longitude IS NULL)
            OR (latitude IS NOT NULL AND longitude IS NOT NULL) AS valid_position_pair,
        latitude IS NULL OR latitude BETWEEN -90.0 AND 90.0 AS valid_latitude,
        longitude IS NULL OR longitude BETWEEN -180.0 AND 180.0 AS valid_longitude,
        velocity_mps IS NULL OR velocity_mps >= 0.0 AS valid_velocity,
        true_track_deg IS NULL
            OR (true_track_deg >= 0.0 AND true_track_deg < 360.0) AS valid_true_track,
        squawk IS NULL OR squawk RLIKE '^[0-7]{4}$' AS valid_squawk,
        position_source IS NULL OR position_source BETWEEN 0 AND 3 AS valid_position_source,
        category IS NULL OR category BETWEEN 0 AND 20 AS valid_category,
        baro_altitude_m IS NULL OR baro_altitude_m >= -1000.0 AS valid_baro_altitude,
        geo_altitude_m IS NULL OR geo_altitude_m >= -1000.0 AS valid_geo_altitude,
        position_ts IS NULL
            OR snapshot_ts IS NULL
            OR position_ts <= snapshot_ts + INTERVAL 5 SECONDS AS valid_position_time,
        last_contact_ts IS NULL
            OR snapshot_ts IS NULL
            OR last_contact_ts <= snapshot_ts + INTERVAL 5 SECONDS AS valid_last_contact_time,
        _bronze_ingest_ts IS NOT NULL
            AND _bronze_record_hash IS NOT NULL
            AND _source_file_path IS NOT NULL AS valid_bronze_lineage
    FROM normalized
),
classified AS (
    SELECT
        *,
        FILTER(
            ARRAY(
                CASE WHEN NOT valid_icao24 THEN 'INVALID_ICAO24' END,
                CASE WHEN NOT valid_snapshot_ts THEN 'MISSING_SNAPSHOT_TS' END
            ),
            reason -> reason IS NOT NULL
        ) AS quarantine_reasons,
        FILTER(
            ARRAY(
                CASE WHEN NOT valid_source_ingest_ts THEN 'INVALID_SOURCE_INGEST_TS' END,
                CASE WHEN NOT valid_callsign THEN 'INVALID_CALLSIGN' END,
                CASE WHEN NOT valid_position_pair THEN 'INCOMPLETE_POSITION' END,
                CASE WHEN NOT valid_latitude THEN 'INVALID_LATITUDE' END,
                CASE WHEN NOT valid_longitude THEN 'INVALID_LONGITUDE' END,
                CASE WHEN NOT valid_velocity THEN 'NEGATIVE_VELOCITY' END,
                CASE WHEN NOT valid_true_track THEN 'INVALID_TRUE_TRACK' END,
                CASE WHEN NOT valid_squawk THEN 'INVALID_SQUAWK' END,
                CASE WHEN NOT valid_position_source THEN 'INVALID_POSITION_SOURCE' END,
                CASE WHEN NOT valid_category THEN 'INVALID_CATEGORY' END,
                CASE WHEN NOT valid_baro_altitude THEN 'INVALID_BARO_ALTITUDE' END,
                CASE WHEN NOT valid_geo_altitude THEN 'INVALID_GEO_ALTITUDE' END,
                CASE WHEN NOT valid_position_time THEN 'POSITION_TIMESTAMP_FROM_FUTURE' END,
                CASE WHEN NOT valid_last_contact_time THEN 'LAST_CONTACT_TIMESTAMP_FROM_FUTURE' END,
                CASE WHEN NOT valid_bronze_lineage THEN 'MISSING_BRONZE_LINEAGE' END
            ),
            warning -> warning IS NOT NULL
        ) AS quality_warnings
    FROM validated
)
SELECT
    *,
    SIZE(quarantine_reasons) > 0 AS is_quarantined,
    SIZE(quality_warnings) AS warning_count
FROM classified;

-- Persist valid historical aircraft states and retain warning-only records.
CREATE OR REFRESH STREAMING TABLE opensky_state (
    CONSTRAINT valid_icao24 EXPECT (icao24 IS NOT NULL AND icao24 RLIKE '^[0-9A-F]{6}$'),
    CONSTRAINT required_snapshot_ts EXPECT (snapshot_ts IS NOT NULL),
    CONSTRAINT required_silver_processed_ts EXPECT (silver_processed_ts IS NOT NULL),
    CONSTRAINT valid_source_ingest_ts EXPECT (source_ingest_ts IS NOT NULL),
    CONSTRAINT valid_callsign EXPECT (callsign IS NULL OR LENGTH(callsign) <= 8),
    CONSTRAINT valid_position_pair EXPECT (
        (latitude IS NULL AND longitude IS NULL)
        OR (latitude IS NOT NULL AND longitude IS NOT NULL)
    ),
    CONSTRAINT valid_latitude EXPECT (latitude IS NULL OR latitude BETWEEN -90.0 AND 90.0),
    CONSTRAINT valid_longitude EXPECT (longitude IS NULL OR longitude BETWEEN -180.0 AND 180.0),
    CONSTRAINT valid_velocity EXPECT (velocity_mps IS NULL OR velocity_mps >= 0.0),
    CONSTRAINT valid_true_track EXPECT (
        true_track_deg IS NULL OR (true_track_deg >= 0.0 AND true_track_deg < 360.0)
    ),
    CONSTRAINT valid_squawk EXPECT (squawk IS NULL OR squawk RLIKE '^[0-7]{4}$'),
    CONSTRAINT valid_position_source EXPECT (
        position_source IS NULL OR position_source BETWEEN 0 AND 3
    ),
    CONSTRAINT valid_category EXPECT (category IS NULL OR category BETWEEN 0 AND 20),
    CONSTRAINT valid_baro_altitude EXPECT (
        baro_altitude_m IS NULL OR baro_altitude_m >= -1000.0
    ),
    CONSTRAINT valid_geo_altitude EXPECT (
        geo_altitude_m IS NULL OR geo_altitude_m >= -1000.0
    ),
    CONSTRAINT valid_position_time EXPECT (
        position_ts IS NULL OR position_ts <= snapshot_ts + INTERVAL 5 SECONDS
    ),
    CONSTRAINT valid_last_contact_time EXPECT (
        last_contact_ts IS NULL OR last_contact_ts <= snapshot_ts + INTERVAL 5 SECONDS
    ),
    CONSTRAINT valid_bronze_lineage EXPECT (
        _bronze_ingest_ts IS NOT NULL
        AND _bronze_record_hash IS NOT NULL
        AND _source_file_path IS NOT NULL
    )
)
COMMENT 'Historical, normalized OpenSky aircraft states with warning-level quality metrics.'
AS
SELECT
    icao24,
    callsign,
    origin_country,
    source_ingest_ts,
    snapshot_ts,
    position_ts,
    last_contact_ts,
    longitude,
    latitude,
    baro_altitude_m,
    geo_altitude_m,
    velocity_mps,
    true_track_deg,
    vertical_rate_mps,
    position_available,
    on_ground,
    squawk,
    spi,
    position_source,
    category,
    position_age_seconds,
    last_contact_age_seconds,
    _bronze_ingest_ts,
    _bronze_record_hash,
    _source_file_path,
    silver_processed_ts
FROM STREAM(opensky_state_prepared)
WHERE NOT is_quarantined;

-- Persist critical failures with source values and diagnostic reasons.
CREATE OR REFRESH STREAMING TABLE opensky_state_quarantine
COMMENT 'OpenSky aircraft states rejected from Silver because a critical key is invalid.'
AS
SELECT
    raw_icao24,
    raw_callsign,
    raw_origin_country,
    raw_ingest_ts,
    raw_api_time,
    raw_time_position,
    raw_last_contact,
    raw_longitude,
    raw_latitude,
    raw_baro_altitude,
    raw_geo_altitude,
    raw_velocity,
    raw_true_track,
    raw_vertical_rate,
    raw_on_ground,
    raw_squawk,
    raw_spi,
    raw_position_source,
    raw_category,
    raw_states_count,
    icao24,
    callsign,
    origin_country,
    source_ingest_ts,
    snapshot_ts,
    position_ts,
    last_contact_ts,
    longitude,
    latitude,
    baro_altitude_m,
    geo_altitude_m,
    velocity_mps,
    true_track_deg,
    vertical_rate_mps,
    position_available,
    on_ground,
    squawk,
    spi,
    position_source,
    category,
    position_age_seconds,
    last_contact_age_seconds,
    quarantine_reasons,
    quality_warnings,
    warning_count,
    _bronze_ingest_ts,
    _bronze_record_hash,
    _source_file_path,
    silver_processed_ts
FROM STREAM(opensky_state_prepared)
WHERE is_quarantined;
