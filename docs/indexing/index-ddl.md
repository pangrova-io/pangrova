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

**With FORCE (rebuild even if exists):**

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

## Notes

- The index type name after `USING` is an identifier; options in `WITH (...)` are
  type-specific — see each index type's page.
- Use `ALTER INDEX ... REBUILD` for an explicit rebuild. The JVM strategy
  contract builds a payload from the supplied file rows. Commit-driven
  maintenance and checkpointing belong to the Coordinator lifecycle.
  The Coordinator checks the active snapshot and its lineage before listing
  files for an incremental rebuild. Events for the active snapshot or a
  known ancestor complete without enumerating their file sets.
- Scalar index rebuilds write each generation under a distinct `v2/.../gN/`
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
- For the design behind index build and evaluation, see
  [Indexing Architecture](../architecture/indexing.md).
