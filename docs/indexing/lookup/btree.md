---
title: "BTREE"
description: "Sorted B-Tree index for range and equality queries"
---

# BTREE Index

A B-tree index keeps values in sorted pages. The JVM evaluator supports equality,
`IN`, and range lookups; the Native Worker can also use a single-column B-tree
artifact to skip whole data files before scan.

## When to Use

- Range queries (`BETWEEN`, `<`, `>`), ordered scans, and equality lookups
- Columns frequently used in `ORDER BY` with filters
- General-purpose indexing when no specialized type fits

## Syntax

```sql
CREATE INDEX idx_orders_date
ON prod.analytics.orders (order_date)
USING BTREE
FORCE;
```

`FORCE` overrides a conflicting connector-native capability such as Iceberg
min/max statistics; it does not replace an existing Pangrova index.

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `btree.page_size` | `4096` | Maximum keys per B-tree page |
| `btree.store_positions` | `false` | Store row positions in leaf entries for JVM evaluation; Native split pruning does not perform row-level lookups |

## Native split pruning

Native pruning supports single-column equality, `IN`, and range predicates on
exact String, Integer, Long, Float and Double values. String ordering matches
Java UTF-16 comparison, and floating-point ordering preserves Java NaN and
signed-zero semantics. Composite keys, null predicates, empty indexes,
unsupported scalar types, and missing or malformed artifacts retain the split.
A skipped split is reported through `NativeTaskStats.index_splits_skipped`.

## Query Behavior

```sql
SELECT * FROM prod.analytics.orders
WHERE order_date BETWEEN DATE '2025-01-01' AND DATE '2025-03-31';

SELECT * FROM prod.analytics.orders WHERE order_id = 1042;
```

**See Also:** [Index DDL](../index-ddl.md), [HASH](hash.md) for equality-only workloads, [SPARSE](../data-skipping/sparse.md) for cheaper range skipping
