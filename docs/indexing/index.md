---
title: "Indexing"
description: "Index families, DDL and publication lifecycle, query use, and operational limits"
---

# Indexing

Pangrova keeps index artifacts outside the lakehouse table and binds them to table
identity, snapshot references, and generations. This page is the entry point for choosing
an index family and understanding the shared publication lifecycle.

## Index families

| Family | Types | Best for |
|---|---|---|
| Data skipping | [BLOOM_FILTER](data-skipping/bloom-filter.md), [NGRAM_BLOOM_FILTER](data-skipping/ngram-bloom-filter.md), [BITMAP](data-skipping/bitmap.md), [SPARSE](data-skipping/sparse.md) | Excluding files or row groups before reading payload data |
| Lookup | [BTREE](lookup/btree.md), [HASH](lookup/hash.md), [PREFIX](lookup/prefix.md) | Equality, range, and prefix predicates with direct row access |
| Full-text | [INVERTED](full-text.md) | Token and phrase search |
| Containment | [GIN](containment.md) | Array, map, and composite-value membership |
| Spatial | [R_TREE](spatial.md) | Bounding-box and geospatial predicates |
| Vector | [VECTOR](vector.md) | Approximate nearest-neighbor search with a bound retrieval plan |

The DDL surface is shared; options depend on `USING <type>`. See
[Index DDL](index-ddl.md) for `CREATE`, `DROP`, `ALTER`, and `SHOW INDEX` syntax.

## Publication lifecycle

1. `CREATE INDEX` registers a descriptor and creates a build attempt.
2. The build writes an immutable generation under a versioned storage prefix.
3. Publication selects the new generation only after every required file exists.
4. Older mappings keep referring to their original paths until retained snapshots and
   readers release them.
5. Garbage collection removes objects only through the persisted exact scope.

A successful metadata operation does not prove that the build is ready, that a query is
eligible to use the index, or that the index reduces I/O. Verify the index state, the
published generation, and the actual query plan.

## Query eligibility

- Split pruning requires a `READY` index bound to the scan's table identity, snapshot, and
  predicate shape.
- A plan may keep a predicate in Native execution when no eligible index exists; it does
  not silently drop the predicate.
- Vector search additionally requires an eligible distance-order query, a positive limit,
  and a matching ANN index binding. An unbound query can remain exact SQL.

## Operations and limits

- Use `ALTER INDEX ... REBUILD` for an explicit rebuild; snapshot-driven maintenance and
  checkpointing belong to the Coordinator lifecycle.
- Dropping an index retains its descriptor until snapshot references and files are
  released; cleanup can outlive the original user's source-read permission but cannot
  build or publish another generation.
- Paimon scalar indexes retain source snapshots with per-index tags that do not expire;
  a later Paimon snapshot expiry does not break retained index maintenance.

**See also:** [Indexing architecture](../architecture/indexing.md),
[Maintenance](../sql-reference/maintenance.md),
[Current capability status](../getting-started/capability-status.md)
