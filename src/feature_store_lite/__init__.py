"""Offline and online feature store on SQLite with point-in-time correct joins."""

from __future__ import annotations

import json
import logging
import math
import re
import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal

__all__ = [
    "ErrorCode",
    "FeatureStore",
    "FeatureStoreError",
    "FeatureView",
    "__version__",
]

__version__ = "0.1.0"

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

ErrorCode = Literal[
    "naive_timestamp",
    "unknown_view",
    "unknown_feature",
    "missing_column",
    "too_many_rows",
    "store_full",
    "invalid_value",
    "invalid_view",
]

_IDENTIFIER_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_MAX_ENTITY_ROWS = 100_000
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class FeatureStoreError(Exception):
    """Typed error raised by feature_store_lite operations."""

    def __init__(self, code: ErrorCode, message: str) -> None:
        super().__init__(message[:200])
        self.code: ErrorCode = code


@dataclass(frozen=True)
class FeatureView:
    """Definition of a group of features tied to an entity."""

    name: str
    entity: str
    features: tuple[str, ...]
    ttl: timedelta | None = None
    timestamp_field: str = "event_timestamp"
    created_field: str | None = "created_timestamp"

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not _IDENTIFIER_RE.match(self.name):
            raise FeatureStoreError("invalid_view", "Invalid view name")
        if not isinstance(self.entity, str) or not _IDENTIFIER_RE.match(self.entity):
            raise FeatureStoreError("invalid_view", "Invalid entity column name")
        if not isinstance(self.timestamp_field, str) or not _IDENTIFIER_RE.match(
            self.timestamp_field
        ):
            raise FeatureStoreError("invalid_view", "Invalid timestamp_field name")
        if self.created_field is not None and (
            not isinstance(self.created_field, str) or not _IDENTIFIER_RE.match(self.created_field)
        ):
            raise FeatureStoreError("invalid_view", "Invalid created_field name")
        if not isinstance(self.features, tuple):
            object.__setattr__(self, "features", tuple(self.features))
        if not self.features:
            raise FeatureStoreError("invalid_view", "FeatureView must define at least one feature")
        seen: set[str] = set()
        for feat in self.features:
            if not isinstance(feat, str) or not _IDENTIFIER_RE.match(feat):
                raise FeatureStoreError("invalid_view", "Invalid feature name")
            if feat in seen:
                raise FeatureStoreError("invalid_view", "Duplicate feature name in view")
            if feat in {
                self.entity,
                self.timestamp_field,
                self.created_field,
                "event_timestamp",
            }:
                raise FeatureStoreError(
                    "invalid_view", "Feature name collides with reserved/column name"
                )
            seen.add(feat)
        if self.ttl is not None and (
            not isinstance(self.ttl, timedelta) or self.ttl < timedelta(0)
        ):
            raise FeatureStoreError("invalid_view", "ttl must be a non-negative timedelta or None")


def _dt_to_us(val: object) -> int:
    if not isinstance(val, datetime):
        raise FeatureStoreError("invalid_value", "Timestamp must be a datetime instance")
    if val.tzinfo is None or val.tzinfo.utcoffset(val) is None:
        raise FeatureStoreError("naive_timestamp", "Datetime must be timezone-aware")
    delta = val.astimezone(timezone.utc) - _EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def _validate_entity_id(val: object) -> tuple[str, int | str]:
    if isinstance(val, bool) or not isinstance(val, (int, str)):
        raise FeatureStoreError("invalid_value", "Entity id must be int or str (not bool)")
    if isinstance(val, int):
        if val < -(1 << 63) or val >= (1 << 63):
            raise FeatureStoreError("invalid_value", "Integer entity id out of 64-bit range")
        return ("int", val)
    if len(val) > 1024:
        raise FeatureStoreError("invalid_value", "String entity id exceeds 1024 chars")
    return ("str", val)


def _encode_feature_value(val: object) -> list[object]:
    if val is None:
        return ["n", None]
    if isinstance(val, bool):
        return ["b", val]
    if isinstance(val, int):
        return ["i", val]
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            raise FeatureStoreError("invalid_value", "Float value cannot be NaN or Inf")
        return ["f", val]
    if isinstance(val, str):
        if len(val) > 65536:
            raise FeatureStoreError("invalid_value", "String feature value too large")
        return ["s", val]
    raise FeatureStoreError("invalid_value", "Feature value must be None, bool, int, float, or str")


def _decode_payload(payload: str) -> dict[str, object]:
    raw: dict[str, list[object]] = json.loads(payload)
    out: dict[str, object] = {}
    for k, tagged in raw.items():
        tag, v = tagged[0], tagged[1]
        if tag == "n":
            out[k] = None
        elif tag == "b":
            out[k] = bool(v)
        elif tag == "i":
            out[k] = int(v) if isinstance(v, (int, str)) else int(str(v))
        elif tag == "f":
            out[k] = float(v) if isinstance(v, (float, int, str)) else float(str(v))
        else:
            out[k] = str(v)
    return out


class FeatureStore:
    """SQLite-backed offline and online feature store."""

    def __init__(self, path: str = ":memory:", *, max_rows: int = 1_000_000) -> None:
        if isinstance(max_rows, bool) or not isinstance(max_rows, int) or max_rows < 0:
            raise FeatureStoreError("invalid_value", "max_rows must be a non-negative integer")
        self._path = path
        self._max_rows = max_rows
        self._conn = sqlite3.connect(path)
        self._init_db()

    def _init_db(self) -> None:
        with self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS views (
                    name TEXT PRIMARY KEY,
                    entity TEXT NOT NULL,
                    features_json TEXT NOT NULL,
                    ttl_us INTEGER,
                    timestamp_field TEXT NOT NULL,
                    created_field TEXT,
                    entity_type TEXT
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS offline_rows (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    view_name TEXT NOT NULL,
                    entity_int INTEGER,
                    entity_str TEXT,
                    event_ts_us INTEGER NOT NULL,
                    created_ts_us INTEGER NOT NULL,
                    payload TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_offline_int
                ON offline_rows (view_name, entity_int, event_ts_us, created_ts_us, seq)
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_offline_str
                ON offline_rows (view_name, entity_str, event_ts_us, created_ts_us, seq)
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS online_rows (
                    view_name TEXT NOT NULL,
                    entity_key TEXT NOT NULL,
                    event_ts_us INTEGER NOT NULL,
                    created_ts_us INTEGER NOT NULL,
                    seq INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    PRIMARY KEY (view_name, entity_key)
                )
                """
            )

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        self._conn.close()

    def _get_view_record(self, view_name: str) -> tuple[FeatureView, int | None, str | None]:
        if not isinstance(view_name, str):
            raise FeatureStoreError("unknown_view", "Unknown feature view")
        cur = self._conn.execute(
            "SELECT name, entity, features_json, ttl_us, timestamp_field, "
            "created_field, entity_type FROM views WHERE name = ?",
            (view_name,),
        )
        row = cur.fetchone()
        if row is None:
            raise FeatureStoreError("unknown_view", f"Unknown view: {view_name}")
        name, entity, features_json, ttl_us, ts_field, created_field, entity_type = row
        features = tuple(json.loads(features_json))
        ttl = timedelta(microseconds=ttl_us) if ttl_us is not None else None
        view = FeatureView(
            name=name,
            entity=entity,
            features=features,
            ttl=ttl,
            timestamp_field=ts_field,
            created_field=created_field,
        )
        return view, ttl_us, entity_type

    def register(self, view: FeatureView) -> None:
        """Register or idempotently re-register a FeatureView."""
        if not isinstance(view, FeatureView):
            raise FeatureStoreError("invalid_view", "Expected a FeatureView instance")
        ttl_us: int | None = None
        if view.ttl is not None:
            ttl_us = (view.ttl.days * 86400 + view.ttl.seconds) * 1_000_000 + view.ttl.microseconds
        features_json = json.dumps(list(view.features))
        cur = self._conn.execute(
            "SELECT entity, features_json, ttl_us, timestamp_field, created_field "
            "FROM views WHERE name = ?",
            (view.name,),
        )
        existing = cur.fetchone()
        if existing is not None:
            if existing != (
                view.entity,
                features_json,
                ttl_us,
                view.timestamp_field,
                view.created_field,
            ):
                raise FeatureStoreError(
                    "invalid_view",
                    f"View {view.name} already registered with different definition",
                )
            return
        with self._conn:
            self._conn.execute(
                "INSERT INTO views (name, entity, features_json, ttl_us, "
                "timestamp_field, created_field, entity_type) VALUES (?, ?, ?, ?, ?, ?, NULL)",
                (
                    view.name,
                    view.entity,
                    features_json,
                    ttl_us,
                    view.timestamp_field,
                    view.created_field,
                ),
            )

    def ingest(self, view_name: str, rows: Iterable[Mapping[str, object]]) -> int:
        """Ingest feature rows into the offline store for ``view_name``."""
        view, _ttl_us, stored_entity_type = self._get_view_record(view_name)
        current_count_row = self._conn.execute("SELECT COUNT(*) FROM offline_rows").fetchone()
        current_count = int(current_count_row[0]) if current_count_row else 0

        prepared: list[tuple[str, int | None, str | None, int, int, str]] = []
        entity_type = stored_entity_type

        for row in rows:
            if not isinstance(row, Mapping):
                raise FeatureStoreError("invalid_value", "Each row must be a Mapping")
            if view.entity not in row:
                raise FeatureStoreError("missing_column", f"Missing entity column {view.entity}")
            if view.timestamp_field not in row:
                raise FeatureStoreError(
                    "missing_column",
                    f"Missing timestamp column {view.timestamp_field}",
                )
            row_etype, eid = _validate_entity_id(row[view.entity])
            if entity_type is None:
                entity_type = row_etype
            elif entity_type != row_etype:
                raise FeatureStoreError(
                    "invalid_value",
                    "Entity IDs in a view must not mix int and str",
                )

            event_ts_us = _dt_to_us(row[view.timestamp_field])
            if (
                view.created_field is not None
                and view.created_field in row
                and row[view.created_field] is not None
            ):
                created_ts_us = _dt_to_us(row[view.created_field])
            else:
                created_ts_us = -(1 << 62)

            payload_dict: dict[str, list[object]] = {}
            for feat in view.features:
                if feat not in row:
                    raise FeatureStoreError("missing_column", f"Missing feature column {feat}")
                payload_dict[feat] = _encode_feature_value(row[feat])

            if current_count + len(prepared) + 1 > self._max_rows:
                raise FeatureStoreError(
                    "store_full",
                    f"Store capacity of {self._max_rows} rows exceeded",
                )

            e_int = eid if isinstance(eid, int) else None
            e_str = eid if isinstance(eid, str) else None
            prepared.append(
                (
                    view.name,
                    e_int,
                    e_str,
                    event_ts_us,
                    created_ts_us,
                    json.dumps(payload_dict, separators=(",", ":")),
                )
            )

        if not prepared:
            return 0

        with self._conn:
            if stored_entity_type is None and entity_type is not None:
                self._conn.execute(
                    "UPDATE views SET entity_type = ? WHERE name = ?",
                    (entity_type, view.name),
                )
            self._conn.executemany(
                "INSERT INTO offline_rows (view_name, entity_int, entity_str, "
                "event_ts_us, created_ts_us, payload) VALUES (?, ?, ?, ?, ?, ?)",
                prepared,
            )
        return len(prepared)

    def _parse_feature_refs(
        self, features: Sequence[str]
    ) -> tuple[
        list[tuple[str, str, str]],
        dict[str, list[str]],
        dict[str, tuple[FeatureView, int | None, str | None]],
    ]:
        if isinstance(features, (str, bytes)) or not isinstance(features, Sequence):
            raise FeatureStoreError(
                "unknown_feature", "features must be a sequence of 'view:feature' strings"
            )
        parsed: list[tuple[str, str, str]] = []
        by_view: dict[str, list[str]] = {}
        view_records: dict[str, tuple[FeatureView, int | None, str | None]] = {}

        for ref in features:
            if not isinstance(ref, str) or ":" not in ref:
                raise FeatureStoreError("unknown_feature", f"Invalid feature reference: {ref}")
            parts = ref.split(":")
            if len(parts) != 2 or not parts[0] or not parts[1]:
                raise FeatureStoreError("unknown_feature", f"Invalid feature reference: {ref}")
            vname, fname = parts[0], parts[1]
            if vname not in view_records:
                view_records[vname] = self._get_view_record(vname)
            view_obj = view_records[vname][0]
            if fname not in view_obj.features:
                raise FeatureStoreError(
                    "unknown_feature",
                    f"Unknown feature {fname} in view {vname}",
                )
            out_col = f"{vname}__{fname}"
            parsed.append((vname, fname, out_col))
            if vname not in by_view:
                by_view[vname] = []
            if fname not in by_view[vname]:
                by_view[vname].append(fname)

        return parsed, by_view, view_records

    def get_historical_features(
        self,
        entity_rows: Sequence[Mapping[str, object]],
        features: Sequence[str],
    ) -> list[dict[str, object]]:
        """Perform a point-in-time join for ``entity_rows`` against ``features``."""
        if isinstance(entity_rows, (str, bytes)) or not isinstance(entity_rows, Sequence):
            raise FeatureStoreError("invalid_value", "entity_rows must be a Sequence of Mappings")
        n_rows = len(entity_rows)
        if n_rows > _MAX_ENTITY_ROWS:
            raise FeatureStoreError(
                "too_many_rows",
                f"entity_rows count {n_rows} exceeds limit {_MAX_ENTITY_ROWS}",
            )

        parsed_refs, by_view, view_records = self._parse_feature_refs(features)
        if n_rows == 0:
            return []

        required_entities = {view_records[vname][0].entity for vname in by_view}

        results: list[dict[str, object]] = []
        entity_ts_us: list[int] = []
        view_entity_types: dict[str, str | None] = {
            vname: view_records[vname][2] for vname in by_view
        }

        for row in entity_rows:
            if not isinstance(row, Mapping):
                raise FeatureStoreError("invalid_value", "Each entity row must be a Mapping")
            if "event_timestamp" not in row:
                raise FeatureStoreError("missing_column", "Missing 'event_timestamp' in entity row")
            ts_us = _dt_to_us(row["event_timestamp"])
            entity_ts_us.append(ts_us)

            out_row: dict[str, object] = {"event_timestamp": row["event_timestamp"]}
            for ent_col in required_entities:
                if ent_col not in row:
                    raise FeatureStoreError("missing_column", f"Missing entity column {ent_col}")
                out_row[ent_col] = row[ent_col]

            for vname in by_view:
                ent_col = view_records[vname][0].entity
                r_type, _ = _validate_entity_id(row[ent_col])
                expected_type = view_entity_types[vname]
                if expected_type is None:
                    view_entity_types[vname] = r_type
                elif expected_type != r_type:
                    raise FeatureStoreError(
                        "invalid_value",
                        "Mixed int and str entity IDs for view",
                    )

            for _vname, _fname, out_col in parsed_refs:
                out_row[out_col] = None
            results.append(out_row)

        for vname, feat_names in by_view.items():
            view_obj, ttl_us, stored_etype = view_records[vname]
            if stored_etype is None:
                continue

            ent_col = view_obj.entity
            indexed_entities: list[tuple[int | str, int, int]] = []
            for idx in range(n_rows):
                eid = entity_rows[idx][ent_col]
                indexed_entities.append((eid, entity_ts_us[idx], idx))  # type: ignore[arg-type]

            indexed_entities.sort(key=lambda x: (x[0], x[1]))

            if stored_etype == "int":
                cur = self._conn.execute(
                    "SELECT entity_int, event_ts_us, created_ts_us, seq, payload "
                    "FROM offline_rows WHERE view_name = ? "
                    "ORDER BY entity_int ASC, event_ts_us ASC, created_ts_us ASC, seq ASC",
                    (vname,),
                )
            else:
                cur = self._conn.execute(
                    "SELECT entity_str, event_ts_us, created_ts_us, seq, payload "
                    "FROM offline_rows WHERE view_name = ? "
                    "ORDER BY entity_str ASC, event_ts_us ASC, created_ts_us ASC, seq ASC",
                    (vname,),
                )

            feat_rows = cur.fetchall()
            if not feat_rows:
                continue

            m_feat = len(feat_rows)
            f_idx = 0
            e_idx = 0

            while e_idx < n_rows:
                curr_id = indexed_entities[e_idx][0]
                e_end = e_idx + 1
                while e_end < n_rows and indexed_entities[e_end][0] == curr_id:
                    e_end += 1

                while f_idx < m_feat and feat_rows[f_idx][0] < curr_id:
                    f_idx += 1

                if f_idx < m_feat and feat_rows[f_idx][0] == curr_id:
                    f_start = f_idx
                    f_end = f_idx + 1
                    while f_end < m_feat and feat_rows[f_end][0] == curr_id:
                        f_end += 1

                    ptr = f_start
                    for k in range(e_idx, e_end):
                        _, t_us, orig_idx = indexed_entities[k]
                        while ptr < f_end and feat_rows[ptr][1] <= t_us:
                            ptr += 1
                        cand_idx = ptr - 1
                        if cand_idx >= f_start:
                            cand_ts_us = int(feat_rows[cand_idx][1])
                            if ttl_us is None or cand_ts_us >= t_us - ttl_us:
                                decoded = _decode_payload(feat_rows[cand_idx][4])
                                for fname in feat_names:
                                    results[orig_idx][f"{vname}__{fname}"] = decoded[fname]

                    f_idx = f_end

                e_idx = e_end

        return results

    def materialize(self, view_name: str, start: datetime, end: datetime) -> int:
        """Materialize latest feature values in ``[start, end)`` to the online table."""
        _view, _ttl_us, stored_etype = self._get_view_record(view_name)
        start_us = _dt_to_us(start)
        end_us = _dt_to_us(end)
        if end_us <= start_us or stored_etype is None:
            return 0

        if stored_etype == "int":
            cur = self._conn.execute(
                "SELECT entity_int, event_ts_us, created_ts_us, seq, payload "
                "FROM offline_rows "
                "WHERE view_name = ? AND event_ts_us >= ? AND event_ts_us < ? "
                "ORDER BY entity_int ASC, event_ts_us ASC, created_ts_us ASC, seq ASC",
                (view_name, start_us, end_us),
            )
        else:
            cur = self._conn.execute(
                "SELECT entity_str, event_ts_us, created_ts_us, seq, payload "
                "FROM offline_rows "
                "WHERE view_name = ? AND event_ts_us >= ? AND event_ts_us < ? "
                "ORDER BY entity_str ASC, event_ts_us ASC, created_ts_us ASC, seq ASC",
                (view_name, start_us, end_us),
            )

        best_per_entity: dict[str, tuple[int, int, int, str]] = {}
        for eid, ev_ts, cr_ts, seq, payload in cur:
            ekey = f"i:{eid}" if stored_etype == "int" else f"s:{eid}"
            best_per_entity[ekey] = (int(ev_ts), int(cr_ts), int(seq), str(payload))

        if not best_per_entity:
            return 0

        written = 0
        with self._conn:
            for ekey, (ev_ts, cr_ts, seq, payload) in best_per_entity.items():
                existing = self._conn.execute(
                    "SELECT event_ts_us, created_ts_us, seq FROM online_rows "
                    "WHERE view_name = ? AND entity_key = ?",
                    (view_name, ekey),
                ).fetchone()
                if existing is not None:
                    ex_ev, ex_cr, ex_seq = (int(existing[0]), int(existing[1]), int(existing[2]))
                    if (ev_ts, cr_ts, seq) < (ex_ev, ex_cr, ex_seq):
                        continue
                self._conn.execute(
                    "INSERT OR REPLACE INTO online_rows "
                    "(view_name, entity_key, event_ts_us, created_ts_us, seq, payload) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (view_name, ekey, ev_ts, cr_ts, seq, payload),
                )
                written += 1

        return written

    def get_online_features(
        self, features: Sequence[str], entity_ids: Sequence[object]
    ) -> dict[str, list[object]]:
        """Fetch materialized online features for ``entity_ids``."""
        if isinstance(entity_ids, (str, bytes)) or not isinstance(entity_ids, Sequence):
            raise FeatureStoreError("invalid_value", "entity_ids must be a Sequence of int or str")
        if len(entity_ids) > _MAX_ENTITY_ROWS:
            raise FeatureStoreError(
                "too_many_rows",
                f"entity_ids count {len(entity_ids)} exceeds limit {_MAX_ENTITY_ROWS}",
            )

        parsed_refs, by_view, view_records = self._parse_feature_refs(features)
        out: dict[str, list[object]] = {
            out_col: [None] * len(entity_ids) for _v, _f, out_col in parsed_refs
        }
        if not entity_ids or not parsed_refs:
            return out

        validated_ids: list[tuple[str, str]] = []
        for eid in entity_ids:
            etype, val = _validate_entity_id(eid)
            ekey = f"i:{val}" if etype == "int" else f"s:{val}"
            validated_ids.append((etype, ekey))

        for vname, feat_names in by_view.items():
            _view_obj, _ttl_us, stored_etype = view_records[vname]
            expected_type = stored_etype
            for etype, _ekey in validated_ids:
                if expected_type is None:
                    expected_type = etype
                elif expected_type != etype:
                    raise FeatureStoreError(
                        "invalid_value",
                        "Mixed int and str entity IDs for view",
                    )

            unique_keys = list({ekey for _, ekey in validated_ids})
            online_map: dict[str, dict[str, object]] = {}
            for ekey in unique_keys:
                cur = self._conn.execute(
                    "SELECT payload FROM online_rows WHERE view_name = ? AND entity_key = ?",
                    (vname, ekey),
                )
                row = cur.fetchone()
                if row is not None:
                    online_map[ekey] = _decode_payload(str(row[0]))

            for idx, (_etype, ekey) in enumerate(validated_ids):
                if ekey in online_map:
                    decoded = online_map[ekey]
                    for fname in feat_names:
                        out[f"{vname}__{fname}"][idx] = decoded[fname]

        return out
