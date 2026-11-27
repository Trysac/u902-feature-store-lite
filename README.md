# feature-store-lite

Training sets built with a plain join on entity id use feature values recorded after the label time, so models look better offline than in production (label leakage). Full feature stores need a cluster and a platform team. This library keeps feature rows in SQLite, builds training sets with point-in-time joins and TTLs, and materialises the latest values to an online table for serving.

## Installation

Not published to PyPI yet. Install it from GitHub:

```bash
pip install "git+https://github.com/Trysac/u902-feature-store-lite"
```

## Usage

```python
from datetime import datetime, timedelta, timezone
from feature_store_lite import FeatureStore, FeatureView

store = FeatureStore()
store.register(FeatureView("driver_stats", "driver_id", ("trips",), ttl=timedelta(hours=2)))
t0 = datetime(2026, 1, 5, 10, 0, tzinfo=timezone.utc)
store.ingest("driver_stats", [{"driver_id": 1001, "event_timestamp": t0, "trips": 5}])
hist = store.get_historical_features(
    [{"driver_id": 1001, "event_timestamp": t0}], ["driver_stats:trips"]
)
store.materialize("driver_stats", t0 - timedelta(hours=1), t0 + timedelta(hours=1))
online = store.get_online_features(["driver_stats:trips"], [1001])
```

## API Summary

- `FeatureView(name, entity, features, ttl=None, timestamp_field='event_timestamp', created_field='created_timestamp')`: Defines features. Names must match `[a-z][a-z0-9_]{0,62}`.
- `FeatureStore(path=':memory:', *, max_rows=1_000_000)`: Manages storage.
- `register(view)`: Adds a schema.
- `ingest(view_name, rows)`: Adds historical feature rows. Refuses if `max_rows` capacity is exceeded (`store_full`).
- `get_historical_features(entity_rows, features)`: Joins historical data point-in-time (`f <= t` and `f >= t - ttl`).
- `materialize(view_name, start, end)`: Updates online store for features in range.
- `get_online_features(features, entity_ids)`: Retrieves latest values.

Errors (raised as `FeatureStoreError` with code `ErrorCode`): `naive_timestamp` (requires timezone-aware datetime), `unknown_view`, `unknown_feature`, `missing_column`, `too_many_rows` (limit: 100,000 entity rows), `store_full`, `invalid_value`, `invalid_view`.

## Why not Feast?

Feast requires a registry service or cloud infrastructure. `feature-store-lite` provides point-in-time semantics in a single SQLite file, ideal for small teams or local testing.

## Security

See [docs/SECURITY.md](docs/SECURITY.md).