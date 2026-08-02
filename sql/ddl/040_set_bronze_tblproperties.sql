-- Bronze table TBLPROPERTIES
-- Sets Delta optimization properties on all Bronze tables.
--
-- Run once after initial pipeline setup, or whenever a new Bronze table is added.
-- Safe to re-run — ALTER TABLE SET TBLPROPERTIES is idempotent.
-- Override target_catalog for another environment before running the DDL.
--
-- optimizeWrite : Databricks auto-tunes file sizes during write (avoids small files)
-- autoCompact   : After each write, Delta auto-merges small files in the background
--
-- autoCompact applies only to append tables (overwrite tables replace all files each
-- run, so compaction would be immediately discarded).

-- ---------------------------------------------------------------------------
-- Append tables — optimizeWrite + autoCompact
-- ---------------------------------------------------------------------------

DECLARE OR REPLACE VARIABLE target_catalog STRING DEFAULT 'dev_alh';

ALTER TABLE IDENTIFIER(target_catalog || '.bronze.raw_opensky_states')
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    );

ALTER TABLE IDENTIFIER(target_catalog || '.bronze.raw_aviationstack_flights')
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    );

ALTER TABLE IDENTIFIER(target_catalog || '.bronze.raw_owm_current')
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    );

ALTER TABLE IDENTIFIER(target_catalog || '.bronze.raw_owm_forecast')
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    );

ALTER TABLE IDENTIFIER(target_catalog || '.bronze.raw_awc_metar')
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    );

-- ---------------------------------------------------------------------------
-- Overwrite tables (OurAirports) — no TBLPROPERTIES needed
-- Each run fully replaces all files → small-file problem never accumulates.
-- optimizeWrite and autoCompact would be wasted on every overwrite.
-- ---------------------------------------------------------------------------

-- (no ALTER TABLE statements needed for overwrite tables)
