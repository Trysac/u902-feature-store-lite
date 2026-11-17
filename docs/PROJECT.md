# Project record

## Objective

Offline and online feature store on SQLite with point-in-time correct joins that never leak future values into training data.

## Scope

- In scope: SQLite-backed feature store implementation; point-in-time joins; TTL enforcement; materialized online store; Python API for registration, ingestion, and retrieval; input validation for datetimes and types.
- Out of scope: Streaming ingestion; network/service registry; feature transformations (e.g., aggregation); parquet/data warehouse sources; multi-entity keys.

## Effort metrics

| Metric | Value |
|---|---|
| Tokens (input / output) | 820791 / 18850 |
| Hours (human) | pending measurement |
| Cost (USD) | 0.5481 |

## Factory run u902 (measured from API usage)

| Date (UTC) | Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|---|
| 2026-11-10 | generation | google/gemini-4-argon | yes | 5443 | 15715 | 0 | 0 | 0.3361 |
| 2026-11-17 | fix-1 | google/gemini-3.8-flash | no | 52566 | 822 | 0 | 120621 | 0.0516 |
| 2026-11-17 | audit | google/gemini-3.8-flash | no | 126940 | 848 | 0 | 394030 | 0.1279 |
| 2026-11-17 | docs | google/gemini-3.1-flash-lite | no | 121191 | 1465 | 0 | 0 | 0.0325 |

Total: $0.5481. Prices: vertex line, as measured by its adapter. Providers serve the current version of each model id, so the date tells which version answered; versions, launch stages and price sources per line are in the factory's model register (docs/MODELS.md).
