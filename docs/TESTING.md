# Testing

## Existing Tests

- `tests/test_feature_store_lite.py`: This file contains tests covering:
  - Acceptance criteria for point-in-time joins, including TTL boundary conditions.
  - Tie-breaking behavior for equal event timestamps (using `created_timestamp` and ingest order).
  - Oracle property testing: Compares results against `pandas.merge_asof` for 1,000 random test cases.
  - Materialization logic and non-overwrite consistency.
  - Error handling for invalid inputs (e.g., naive datetimes, missing columns, unknown views/features).
  - Capacity limits (`too_many_rows` and `store_full`).
  - Data type round-tripping for supported types (int, float, bool, str).