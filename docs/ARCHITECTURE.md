# Architecture

## Overview

`feature_store_lite` implements an offline and online feature store backed by a single SQLite database (`:memory:` or file path) with zero runtime dependencies outside the Python standard library.

## Data Model

- `views`: Stores registered `FeatureView` schemas, TTL in microseconds, and the locked entity ID type (`int` or `str`).
- `offline_rows`: Append-only historical feature table indexed on `(view_name, entity_int/entity_str, event_ts_us, created_ts_us, seq)`.
- `online_rows`: Serving table keyed by `(view_name, entity_key)` storing the latest materialized feature payload alongside its `(event_ts_us, created_ts_us, seq)` version tuple.

## Point-in-Time Join Algorithm

For each requested `FeatureView`:
1. Fetch feature rows from `offline_rows` ordered by `(entity, event_ts_us, created_ts_us, seq)`.
2. Sort the input `entity_rows` by `(entity_id, event_ts_us)` while retaining each row's original index.
3. Perform a single linear merge pass per entity to locate the candidate with the largest `(event_ts_us, created_ts_us, seq)` satisfying `event_ts_us <= t` and `event_ts_us >= t - ttl`.
4. Restore the original input order.
