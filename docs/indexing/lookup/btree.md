---
title: "BTREE"
description: "Sorted B-Tree index for range and equality queries"
---

# BTREE Index

A B-Tree index keeps values in sorted order, supporting both equality and range
lookups with logarithmic descent to matching row positions.

## When to Use

- Range queries (`BETWEEN`, `<`, `>`), ordered scans, and equality lookups
- Columns frequently used in `ORDER BY` with filters
- General-purpose indexing when no specialized type fits

## Syntax

```sql
CREATE INDEX idx_orders_date
ON prod.analytics.orders (order_date)
USING BTREE;
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `btree.page_size` | `4096` | Index page size in bytes |
| `btree.store_positions` | `false` | Store row positions in leaf entries for direct row access |

## Query Behavior

```sql
SELECT * FROM prod.analytics.orders
WHERE order_date BETWEEN DATE '2025-01-01' AND DATE '2025-03-31';

SELECT * FROM prod.analytics.orders WHERE order_id = 1042;
```

**See Also:** [Index DDL](../index-ddl.md), [HASH](hash.md) for equality-only workloads, [SPARSE](../data-skipping/sparse.md) for cheaper range skipping
