---
title: "Index DDL"
description: "CREATE / DROP / ALTER / SHOW INDEX statements and the index type catalog"
---

# Index DDL

Indexes accelerate predicate evaluation and ANN search over lakehouse tables. All
index types share the same DDL surface; behavior differs by `USING <type>`.
A recognized index type or successful metadata operation does not prove build,
publication, freshness, background authorization, or query acceleration. Verify the
index artifact and actual query plan. See
[Current Capability Status](../getting-started/capability-status.md) and the
[index publication boundary](../architecture/distributed-writes.md).

## Index Types

| Type | Category | Best for |
|------|----------|----------|
| [`BLOOM_FILTER`](data-skipping/bloom-filter.md) | Data-skipping | Equality predicates on high-cardinality columns |
| [`NGRAM_BLOOM_FILTER`](data-skipping/ngram-bloom-filter.md) | Data-skipping | Substring (`LIKE '%...%'`) pattern matching |
| [`BITMAP`](data-skipping/bitmap.md) | Data-skipping | Low-cardinality columns, combined predicates |
| [`SPARSE`](data-skipping/sparse.md) | Data-skipping | Range predicates on sorted / near-sorted columns |
| [`BTREE`](lookup/btree.md) | Lookup | Range and equality lookups |
| [`HASH`](lookup/hash.md) | Lookup | Fast equality lookups |
| [`PREFIX`](lookup/prefix.md) | Lookup | Prefix-based string matching |
| [`INVERTED`](full-text.md) | Full-text | Token-based full-text search |
| [`GIN`](containment.md) | Containment | Composite value containment (arrays, maps) |
| [`R_TREE`](spatial.md) | Spatial | Bounding-box and geospatial predicates |
| [`VECTOR`](vector.md) | Vector | Approximate nearest neighbor (ANN) search |

## CREATE INDEX

```sql
CREATE INDEX IF NOT EXISTS idx_events_user
ON prod.analytics.events (user_id)
USING BLOOM_FILTER
WITH (
  'bloom.fpp' = '0.01',
  'bloom.expected_entries' = '1000000'
);
```

**With FORCE (override a conflicting connector-native index capability):**

```sql
CREATE INDEX idx_orders_status
ON prod.analytics.orders (status)
USING BLOOM_FILTER
FORCE;
```

## DROP INDEX

```sql
DROP INDEX IF EXISTS idx_events_user ON prod.analytics.events;
```

## ALTER INDEX

```sql
ALTER INDEX idx_orders_status ON prod.analytics.orders REBUILD;
```

## SHOW INDEXES

```sql
SHOW INDEXES ON prod.analytics.events;
```

The result is ordered by `index_name`, then `index_id`, and excludes `DROPPED`
descriptors. Every row has the same columns in the order shown below. New
descriptor fields follow the existing positional result columns:

| Column | Meaning |
|---|---|
| `index_name` | SQL index name |
| `index_type` | Canonical index type |
| `columns` | Comma-separated indexed columns in key order |
| `state` | `BUILDING`, `READY`, or `STALE` |
| `stale_reason` | Fenced stale reason, or null |
| `created_at` | Descriptor creation timestamp |
| `algorithm` | Vector algorithm or full-text analyzer, otherwise empty |
| `metric` | Vector metric, otherwise empty |
| `index_id` | Stable Pangrova descriptor identity |
| `generation` | Integer current descriptor generation |
| `build_snapshot_id` | Snapshot reserved by the current build, or empty |
| `last_indexed_snapshot_id` | Last published source snapshot, or empty |
| `updated_at` | Last descriptor transition timestamp |

## Result and error contracts

- `CREATE INDEX` and `ALTER INDEX ... REBUILD` return one row with `index_id`
  and `state`. The state describes the accepted descriptor transition, not build
  completion.
- `CREATE INDEX IF NOT EXISTS` returns the existing descriptor's real `index_id`
  and current `state`; it does not submit another build.
- `FORCE` only overrides a conflicting connector-native index capability. It
  does not rebuild or replace an existing Pangrova index.
- `DROP INDEX` returns no rows. `DROP INDEX IF EXISTS` also returns no rows when
  the index is absent.
- An existing name without `IF NOT EXISTS` uses `METADATA_INDEX_ALREADY_EXISTS`.
  A missing index uses `METADATA_INDEX_NOT_FOUND`. Invalid index definitions,
  unsupported algorithms, and incompatible rebuild schemas use
  `METADATA_INDEX_VALIDATION_ERROR`.
- JDBC `DatabaseMetaData.getIndexInfo` is explicitly unsupported and throws
  `SQLFeatureNotSupportedException`; clients must use `SHOW INDEXES` rather than
  interpreting an empty JDBC result as proof that no indexes exist.

## Notes

- The index type name after `USING` is an identifier; options in `WITH (...)` are
  type-specific — see each index type's page.
- `CREATE INDEX IF NOT EXISTS` returns the existing index ID and current state;
  it does not start a duplicate build. `FORCE` only overrides a conflicting
  connector-native capability. Use `ALTER INDEX ... REBUILD` to replace an
  existing index generation.
- Use `ALTER INDEX ... REBUILD` for an explicit rebuild. A rebuild refuses a
  `READY` descriptor whose bound source schema no longer matches the current
  table. An explicit rebuild of a `STALE` descriptor binds its current indexed-
  column schema after validating source access and compatibility; unresolved
  connector schema operations still block rebuilding the claimed table.
  Indexes created before schema V122 without a binding become `STALE` on upgrade.
  The JVM strategy contract builds a payload from the supplied file rows.
  Commit-driven maintenance and checkpointing belong to the Coordinator lifecycle.
  The Coordinator checks the active snapshot and its lineage before listing
  files for an incremental rebuild. Events for the active snapshot or a
  known ancestor complete without enumerating their file sets.
- Scalar index rebuilds write each generation under a distinct `v3/.../gN/`
  storage prefix. Publication selects the new generation only after its files
  are complete; retained older mappings continue to refer to their original
  paths until garbage collection. Previously written `v1` paths remain readable
  through their persisted mappings.
- Scalar index DROP retains the descriptor until snapshot references and index files are
  released. Cleanup uses the index's persisted exact scope even if the original
  user's source-read permission has since been revoked; it cannot build or
  publish another generation. A per-index PostgreSQL lock serializes new
  snapshot pins with final cleanup.
- Paimon file indexes use the catalog filesystem and read the requested physical
  file with its owning table schema. A missing file fails the build instead of
  reading another file or the entire table.
- Paimon scalar indexes retain each source snapshot with a per-index tag that
  has no expiry time. If Paimon later expires the numbered snapshot file,
  index maintenance reads that snapshot through its retained tag.
- `DROP TABLE`, table/column rename, column add/drop and column type changes fence
  affected indexes in PostgreSQL before connector DDL starts. The schema journal
  and descriptor fence commit together; maintenance applies the final transition
  only to the original captured descriptor fence. A connector result of unknown
  outcome leaves the table operation claimed; a new build cannot proceed until
  authoritative outcome evidence resolves the claim. Only a proved pre-call
  failure may abort and release it. External schema changes are detected by
  periodic reconciliation.
- For the design behind index build and evaluation, see
  [Indexing Architecture](../architecture/indexing.md).
