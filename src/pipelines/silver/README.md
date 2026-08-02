# OpenSky Silver Pipeline

## Scope

The `opensky_silver` Lakeflow Spark Declarative Pipeline reads the append-only
`bronze.raw_opensky_states` Delta table and publishes two streaming tables in
the pipeline's configured catalog and `silver` schema:

- `opensky_state` contains normalized historical aircraft states;
- `opensky_state_quarantine` contains records that fail a critical key rule.

The source uses a two-part identifier intentionally. The pipeline configuration
selects the environment-specific catalog, while `bronze` remains the explicit
source schema. Output table names are unqualified and therefore resolve to the
configured `silver` schema.

## Processing Mode Decision

The pipeline uses streaming-table semantics and a triggered execution mode.
This is a deliberate architectural decision rather than a batch-table design:

- Bronze receives an append approximately every 20 minutes;
- the existing scheduled job starts one incremental Silver update after Bronze
  freshness validation succeeds;
- the pipeline checkpoint records source progress between updates;
- `full_refresh` remains disabled for scheduled runs;
- a missed update is recovered by processing every unread Bronze commit during
  the next successful update.

The target state is continuous processing after the source ingestion cadence
and latency requirements justify an always-running pipeline. That migration
requires setting `continuous: true` and removing the pipeline from the Lakeflow
Job task graph because continuous pipelines cannot run as pipeline tasks.

## Data Quality Policy

Records are quarantined immediately when `icao24` is not a six-character
uppercase hexadecimal value or when `snapshot_ts` cannot be derived. Other
quality conditions are warning-level expectations and do not remove records
from the historical Silver table.

Stale position and contact timestamps are retained as valid source behavior.
Only timestamps more than five seconds ahead of the API snapshot generate a
warning. Velocity has no artificial upper bound but cannot be negative.
Altitude has a lower plausibility bound of -1000 metres and no upper bound.
Vertical rate is retained for monitoring without a fixed validity range.

No stateful deduplication is applied initially. The observed logical key is
`(snapshot_ts, icao24)`, and uniqueness must remain under operational
monitoring before a watermark or deduplication policy is introduced.
