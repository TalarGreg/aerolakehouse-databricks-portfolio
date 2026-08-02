# ADR 001: Triggered Streaming for Bronze-to-Silver Processing

- Status: Accepted
- Date: 2026-07-21

## Context

External APIs currently land files every 20 minutes. A continuously running
stream would spend most of its lifetime waiting for input and would provide no
meaningful freshness improvement at the current source cadence.

## Decision

Use Structured Streaming and Lakeflow Spark Declarative Pipelines in triggered
mode. Each run processes unread commits from the persisted checkpoint and then
stops. The Bronze ingestion job triggers the Silver pipeline after Bronze
freshness validation succeeds.

## Consequences

- Streaming checkpoints preserve incremental progress between scheduled runs.
- Compute is used only when new input is expected.
- End-to-end latency is bounded by the 20-minute ingestion schedule.
- The custom Bronze `foreachBatch` path has at-least-once semantics; consumers
  that require uniqueness must deduplicate with stable business keys.
- Moving to continuous streaming later requires changing the pipeline trigger
  and ensuring that upstream ingestion also provides continuous input.
