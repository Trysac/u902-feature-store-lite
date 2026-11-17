"""Tests for feature_store_lite covering all acceptance criteria, property oracle, and edge cases."""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from feature_store_lite import FeatureStore, FeatureStoreError, FeatureView

UTC = timezone.utc


def dt(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 1, 5, hour, minute, second, tzinfo=UTC)


def test_acceptance_1_and_4_historical_and_online() -> None:
    """Acceptance 1 & 4: point-in-time historical join with TTL and online materialization."""
    store = FeatureStore()
    view = FeatureView(
        name="driver_stats",
        entity="driver_id",
        features=("trips",),
        ttl=timedelta(hours=2),
    )
    store.register(view)
    # Idempotent re-registration
    store.register(view)

    ingested = store.ingest(
        "driver_stats",
        [
            {"driver_id": 1001, "event_timestamp": dt(10, 0), "trips": 5},
            {"driver_id": 1001, "event_timestamp": dt(11, 0), "trips": 7},
            {"driver_id": 1002, "event_timestamp": dt(9, 0), "trips": 3},
        ],
    )
    assert ingested == 3

    entity_rows = [
        {"driver_id": 1001, "event_timestamp": dt(10, 30)},
        {"driver_id": 1001, "event_timestamp": dt(11, 0)},
        {"driver_id": 1001, "event_timestamp": dt(9, 59)},
        {"driver_id": 1002, "event_timestamp": dt(11, 30)},
        {"driver_id": 1002, "event_timestamp": dt(11, 0)},
    ]
    res = store.get_historical_features(entity_rows, ["driver_stats:trips"])
    assert [r["driver_stats__trips"] for r in res] == [5, 7, None, None, 3]
    assert [r["driver_id"] for r in res] == [1001, 1001, 1001, 1002, 1002]
    assert [r["event_timestamp"] for r in res] == [
        dt(10, 30),
        dt(11, 0),
        dt(9, 59),
        dt(11, 30),
        dt(11, 0),
    ]

    # Acceptance 4: materialize and get_online_features
    written = store.materialize("driver_stats", start=dt(0, 0), end=dt(12, 0))
    assert written == 2
    online = store.get_online_features(["driver_stats:trips"], [1001, 1002, 9999])
    assert online == {"driver_stats__trips": [7, 3, None]}

    # Later materialize over an older interval never replaces a newer online value
    written_older = store.materialize("driver_stats", start=dt(0, 0), end=dt(10, 30))
    assert (
        written_older == 1
    )  # 1002 at 09:00 is equal (re-written), 1001 at 10:00 is older than 11:00
    online_after = store.get_online_features(["driver_stats:trips"], [1001, 1002, 9999])
    assert online_after == {"driver_stats__trips": [7, 3, None]}
    store.close()


def test_acceptance_2_ties_created_and_ingest_order() -> None:
    """Acceptance 2: latest created timestamp wins; equal created timestamp -> last ingested wins."""
    store = FeatureStore()
    view = FeatureView(
        name="driver_stats",
        entity="driver_id",
        features=("trips",),
        ttl=timedelta(hours=2),
    )
    store.register(view)
    store.ingest(
        "driver_stats",
        [
            {
                "driver_id": 1001,
                "event_timestamp": dt(10, 0),
                "created_timestamp": dt(10, 5),
                "trips": 5,
            },
            {
                "driver_id": 1001,
                "event_timestamp": dt(10, 0),
                "created_timestamp": dt(10, 1),
                "trips": 6,
            },
        ],
    )
    res = store.get_historical_features(
        [{"driver_id": 1001, "event_timestamp": dt(10, 30)}],
        ["driver_stats:trips"],
    )
    assert res[0]["driver_stats__trips"] == 5

    # Equal created timestamps: row ingested last wins
    store2 = FeatureStore()
    store2.register(view)
    store2.ingest(
        "driver_stats",
        [
            {
                "driver_id": 1001,
                "event_timestamp": dt(10, 0),
                "created_timestamp": dt(10, 5),
                "trips": 5,
            },
            {
                "driver_id": 1001,
                "event_timestamp": dt(10, 0),
                "created_timestamp": dt(10, 5),
                "trips": 9,
            },
        ],
    )
    res2 = store2.get_historical_features(
        [{"driver_id": 1001, "event_timestamp": dt(10, 30)}],
        ["driver_stats:trips"],
    )
    assert res2[0]["driver_stats__trips"] == 9


def _run_single_oracle_case(rng: random.Random) -> None:
    store = FeatureStore()
    use_ttl = rng.choice([True, False])
    ttl = timedelta(seconds=rng.randint(1, 7200)) if use_ttl else None
    view = FeatureView(
        name="driver_stats",
        entity="driver_id",
        features=("trips",),
        ttl=ttl,
    )
    store.register(view)

    n_entity = rng.randint(1, 50)
    n_feat = rng.randint(1, 200)
    base = datetime(2026, 1, 5, 0, 0, 0, tzinfo=UTC)

    entity_Pool = [1001, 1002, 1003, 1004, 1005]
    entity_rows = [
        {
            "driver_id": rng.choice(entity_Pool),
            "event_timestamp": base + timedelta(seconds=rng.randint(0, 10000)),
        }
        for _ in range(n_entity)
    ]
    feat_rows = [
        {
            "driver_id": rng.choice(entity_Pool),
            "event_timestamp": base + timedelta(seconds=rng.randint(0, 10000)),
            "created_timestamp": base + timedelta(seconds=rng.randint(0, 10000)),
            "trips": rng.randint(0, 1000),
        }
        for _ in range(n_feat)
    ]
    store.ingest("driver_stats", feat_rows)
    actual = store.get_historical_features(entity_rows, ["driver_stats:trips"])
    actual_vals = [r["driver_stats__trips"] for r in actual]

    # Build pandas oracle with merge_asof(direction='backward', by='driver_id', tolerance=ttl, allow_exact_matches=True)
    left_df = pd.DataFrame(entity_rows)
    left_df["_orig_idx"] = range(len(entity_rows))
    left_sorted = left_df.sort_values("event_timestamp", kind="mergesort")

    right_df = pd.DataFrame(feat_rows)
    right_df["_seq"] = range(len(feat_rows))
    right_sorted = right_df.sort_values(
        ["event_timestamp", "created_timestamp", "_seq"], kind="mergesort"
    )

    merged = pd.merge_asof(
        left_sorted,
        right_sorted[["driver_id", "event_timestamp", "trips"]],
        on="event_timestamp",
        by="driver_id",
        direction="backward",
        tolerance=ttl,
        allow_exact_matches=True,
    )
    merged_restored = merged.sort_values("_orig_idx")
    expected_vals = [None if pd.isna(v) else int(v) for v in merged_restored["trips"].tolist()]
    assert actual_vals == expected_vals
    store.close()


def test_acceptance_3_oracle_1000_seeded_cases() -> None:
    """Acceptance 3: 1_000 random cases (seeded, up to 50 entity rows and 200 feature rows each) match pandas.merge_asof."""
    rng = random.Random(20260105)  # noqa: S311
    for _ in range(1_000):
        _run_single_oracle_case(rng)


@settings(
    max_examples=25,
    derandomize=True,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
@given(seed=st.integers(min_value=0, max_value=10_000_000))
def test_hypothesis_oracle_property(seed: int) -> None:
    """Property test driven by Hypothesis (derandomize=True) against pandas.merge_asof."""
    rng = random.Random(seed)  # noqa: S311
    _run_single_oracle_case(rng)


def test_acceptance_5_errors() -> None:
    """Acceptance 5: naive_timestamp, unknown_view, unknown_feature, missing_column."""
    store = FeatureStore()
    view = FeatureView(
        name="driver_stats",
        entity="driver_id",
        features=("trips",),
        ttl=timedelta(hours=2),
    )
    store.register(view)

    # Naive datetime
    with pytest.raises(FeatureStoreError) as exc_info:
        store.ingest(
            "driver_stats",
            [
                {
                    "driver_id": 1001,
                    "event_timestamp": datetime(2026, 1, 5, 10, 0),
                    "trips": 5,
                }
            ],
        )
    assert exc_info.value.code == "naive_timestamp"

    with pytest.raises(FeatureStoreError) as exc_info:
        store.get_historical_features(
            [{"driver_id": 1001, "event_timestamp": datetime(2026, 1, 5, 10, 0)}],
            ["driver_stats:trips"],
        )
    assert exc_info.value.code == "naive_timestamp"

    # Unknown view
    with pytest.raises(FeatureStoreError) as exc_info:
        store.ingest("missing_view", [])
    assert exc_info.value.code == "unknown_view"

    with pytest.raises(FeatureStoreError) as exc_info:
        store.get_historical_features(
            [{"driver_id": 1001, "event_timestamp": dt(10, 0)}],
            ["missing_view:trips"],
        )
    assert exc_info.value.code == "unknown_view"

    # Unknown feature 'driver_stats:speed'
    with pytest.raises(FeatureStoreError) as exc_info:
        store.get_historical_features(
            [{"driver_id": 1001, "event_timestamp": dt(10, 0)}],
            ["driver_stats:speed"],
        )
    assert exc_info.value.code == "unknown_feature"

    # Entity row without 'driver_id'
    with pytest.raises(FeatureStoreError) as exc_info:
        store.get_historical_features(
            [{"event_timestamp": dt(10, 0)}],
            ["driver_stats:trips"],
        )
    assert exc_info.value.code == "missing_column"


def test_acceptance_6_limits_too_many_rows_and_store_full() -> None:
    """Acceptance 6: 100_001 entity rows raise 'too_many_rows', 100_000 accepted; max_rows=3 raises 'store_full' atomically."""
    store = FeatureStore(max_rows=3)
    view = FeatureView(name="driver_stats", entity="driver_id", features=("trips",))
    store.register(view)

    row_template = {"driver_id": 1001, "event_timestamp": dt(10, 0)}
    rows_100k = [row_template] * 100_000
    res = store.get_historical_features(rows_100k, ["driver_stats:trips"])
    assert len(res) == 100_000

    rows_100k_plus_1 = [row_template] * 100_001
    with pytest.raises(FeatureStoreError) as exc_info:
        store.get_historical_features(rows_100k_plus_1, ["driver_stats:trips"])
    assert exc_info.value.code == "too_many_rows"

    with pytest.raises(FeatureStoreError) as exc_info:
        store.get_online_features(["driver_stats:trips"], [1001] * 100_001)
    assert exc_info.value.code == "too_many_rows"

    # Store full atomic rollback
    assert (
        store.ingest(
            "driver_stats",
            [
                {"driver_id": 1001, "event_timestamp": dt(9, 0), "trips": 1},
                {"driver_id": 1001, "event_timestamp": dt(10, 0), "trips": 2},
            ],
        )
        == 2
    )
    with pytest.raises(FeatureStoreError) as exc_info:
        store.ingest(
            "driver_stats",
            [
                {"driver_id": 1001, "event_timestamp": dt(11, 0), "trips": 3},
                {"driver_id": 1001, "event_timestamp": dt(12, 0), "trips": 4},
            ],
        )
    assert exc_info.value.code == "store_full"

    # Nothing from the failed call was stored
    hist = store.get_historical_features(
        [{"driver_id": 1001, "event_timestamp": dt(12, 30)}],
        ["driver_stats:trips"],
    )
    assert hist[0]["driver_stats__trips"] == 2


def test_acceptance_7_value_types_and_invalid_values() -> None:
    """Acceptance 7: None, bool, int, float, str round-trip with exact type; invalid types and NaN raise 'invalid_value'."""
    store = FeatureStore()
    view = FeatureView(
        name="typed_view",
        entity="entity_id",
        features=("i_val", "f_val", "b_val", "s_val", "n_val"),
    )
    store.register(view)
    store.ingest(
        "typed_view",
        [
            {
                "entity_id": "user_1",
                "event_timestamp": dt(10, 0),
                "i_val": 7,
                "f_val": 7.0,
                "b_val": True,
                "s_val": "active",
                "n_val": None,
            }
        ],
    )
    refs = [
        "typed_view:i_val",
        "typed_view:f_val",
        "typed_view:b_val",
        "typed_view:s_val",
        "typed_view:n_val",
    ]
    hist = store.get_historical_features(
        [{"entity_id": "user_1", "event_timestamp": dt(10, 0)}], refs
    )[0]

    assert hist["typed_view__i_val"] == 7 and type(hist["typed_view__i_val"]) is int
    assert hist["typed_view__f_val"] == 7.0 and type(hist["typed_view__f_val"]) is float
    assert hist["typed_view__b_val"] is True and type(hist["typed_view__b_val"]) is bool
    assert hist["typed_view__s_val"] == "active" and type(hist["typed_view__s_val"]) is str
    assert hist["typed_view__n_val"] is None

    store.materialize("typed_view", dt(0, 0), dt(12, 0))
    online = store.get_online_features(refs, ["user_1"])
    assert online["typed_view__i_val"][0] == 7 and type(online["typed_view__i_val"][0]) is int
    assert online["typed_view__f_val"][0] == 7.0 and type(online["typed_view__f_val"][0]) is float
    assert online["typed_view__b_val"][0] is True and type(online["typed_view__b_val"][0]) is bool

    # Reject invalid feature values: NaN, Inf, list/dict/object
    for bad_val in [float("nan"), float("inf"), [1, 2], {"a": 1}, object()]:
        with pytest.raises(FeatureStoreError) as exc_info:
            store.ingest(
                "typed_view",
                [
                    {
                        "entity_id": "user_1",
                        "event_timestamp": dt(10, 0),
                        "i_val": bad_val,
                        "f_val": 7.0,
                        "b_val": True,
                        "s_val": "ok",
                        "n_val": None,
                    }
                ],
            )
        assert exc_info.value.code == "invalid_value"


def test_two_views_joined_and_mixed_entity_types_rejected(tmp_path: Path) -> None:
    """Two views joined in one request, persistence to SQLite file, and mixed int/str entity IDs rejected."""
    db_file = str(tmp_path / "features.db")
    store = FeatureStore(path=db_file)
    v1 = FeatureView(name="driver_stats", entity="driver_id", features=("trips",))
    v2 = FeatureView(
        name="vehicle_stats",
        entity="vehicle_id",
        features=("age_days",),
        created_field=None,
    )
    store.register(v1)
    store.register(v2)

    store.ingest(
        "driver_stats",
        [{"driver_id": 1001, "event_timestamp": dt(10, 0), "trips": 12}],
    )
    store.ingest(
        "vehicle_stats",
        [{"vehicle_id": "veh_a", "event_timestamp": dt(9, 30), "age_days": 400}],
    )

    # Mixing str into int view is rejected
    with pytest.raises(FeatureStoreError) as exc_info:
        store.ingest(
            "driver_stats",
            [{"driver_id": "1001", "event_timestamp": dt(10, 0), "trips": 12}],
        )
    assert exc_info.value.code == "invalid_value"

    with pytest.raises(FeatureStoreError) as exc_info:
        store.get_historical_features(
            [{"driver_id": "1001", "event_timestamp": dt(10, 0)}],
            ["driver_stats:trips"],
        )
    assert exc_info.value.code == "invalid_value"

    with pytest.raises(FeatureStoreError) as exc_info:
        store.get_online_features(["driver_stats:trips"], ["1001"])
    assert exc_info.value.code == "invalid_value"

    # Re-open from disk and join both views
    store.close()
    store2 = FeatureStore(path=db_file)
    joined = store2.get_historical_features(
        [
            {
                "driver_id": 1001,
                "vehicle_id": "veh_a",
                "event_timestamp": dt(10, 0),
            }
        ],
        ["driver_stats:trips", "vehicle_stats:age_days"],
    )
    assert joined == [
        {
            "driver_id": 1001,
            "vehicle_id": "veh_a",
            "event_timestamp": dt(10, 0),
            "driver_stats__trips": 12,
            "vehicle_stats__age_days": 400,
        }
    ]
    store2.close()


def test_validation_and_edge_cases() -> None:
    """Test view validation, empty inputs, and boundary conditions."""
    with pytest.raises(FeatureStoreError) as exc_info:
        FeatureView(name="Invalid-Name", entity="driver_id", features=("trips",))
    assert exc_info.value.code == "invalid_view"

    with pytest.raises(FeatureStoreError) as exc_info:
        FeatureView(name="valid_name", entity="BadEntity", features=("trips",))
    assert exc_info.value.code == "invalid_view"

    with pytest.raises(FeatureStoreError) as exc_info:
        FeatureView(name="valid_name", entity="driver_id", features=())
    assert exc_info.value.code == "invalid_view"

    with pytest.raises(FeatureStoreError) as exc_info:
        FeatureView(name="valid_name", entity="driver_id", features=("trips", "trips"))
    assert exc_info.value.code == "invalid_view"

    with pytest.raises(FeatureStoreError) as exc_info:
        FeatureView(name="valid_name", entity="driver_id", features=("driver_id",))
    assert exc_info.value.code == "invalid_view"

    with pytest.raises(FeatureStoreError) as exc_info:
        FeatureView(
            name="valid_name",
            entity="driver_id",
            features=("trips",),
            ttl=timedelta(seconds=-1),
        )
    assert exc_info.value.code == "invalid_view"

    with pytest.raises(FeatureStoreError) as exc_info:
        FeatureStore(max_rows=-1)
    assert exc_info.value.code == "invalid_value"

    store = FeatureStore()
    v = FeatureView(name="driver_stats", entity="driver_id", features=("trips",))
    store.register(v)

    # Conflicting registration
    v_conflict = FeatureView(name="driver_stats", entity="driver_id", features=("trips", "other"))
    with pytest.raises(FeatureStoreError) as exc_info:
        store.register(v_conflict)
    assert exc_info.value.code == "invalid_view"

    # Empty ingest / historical / materialize / online
    assert store.ingest("driver_stats", []) == 0
    assert store.get_historical_features([], ["driver_stats:trips"]) == []
    assert store.materialize("driver_stats", dt(10, 0), dt(10, 0)) == 0
    assert store.get_online_features(["driver_stats:trips"], []) == {"driver_stats__trips": []}

    # Bool entity id rejected
    with pytest.raises(FeatureStoreError) as exc_info:
        store.ingest(
            "driver_stats",
            [{"driver_id": True, "event_timestamp": dt(10, 0), "trips": 1}],
        )
    assert exc_info.value.code == "invalid_value"

    # Missing columns in ingest
    with pytest.raises(FeatureStoreError) as exc_info:
        store.ingest("driver_stats", [{"event_timestamp": dt(10, 0), "trips": 1}])
    assert exc_info.value.code == "missing_column"

    with pytest.raises(FeatureStoreError) as exc_info:
        store.ingest("driver_stats", [{"driver_id": 1, "trips": 1}])
    assert exc_info.value.code == "missing_column"

    with pytest.raises(FeatureStoreError) as exc_info:
        store.ingest("driver_stats", [{"driver_id": 1, "event_timestamp": dt(10, 0)}])
    assert exc_info.value.code == "missing_column"

    # Bad feature reference format
    with pytest.raises(FeatureStoreError) as exc_info:
        store.get_online_features(["invalid_ref_without_colon"], [1])
    assert exc_info.value.code == "unknown_feature"
