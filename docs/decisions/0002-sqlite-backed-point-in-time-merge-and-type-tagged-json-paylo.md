# SQLite-backed Point-in-Time Merge and Type-Tagged JSON Payloads

Status: accepted (generated, pending human review)

## Context

Building training datasets with plain equality joins leaks future feature values, while full feature stores require external clusters and services. Furthermore, SQLite and JSON can conflate booleans, integers, and floats or lose timezone precision.

## Decision

Store all timestamps as UTC integer microseconds since the epoch, preserve exact Python scalar types (`None`, `bool`, `int`, `float`, `str`) using compact type-tagged JSON arrays, and execute historical point-in-time joins via an indexed SQLite scan paired with an in-memory sorted merge pass per entity.

## Consequences

Zero runtime dependencies, exact parity with pandas.merge_asof backward joins with TTL tolerance, deterministic tie-breaking on created timestamp and ingest order, and exact round-tripping of scalar feature types.
