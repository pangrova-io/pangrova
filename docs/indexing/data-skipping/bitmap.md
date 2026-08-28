---
title: "BITMAP"
description: "Roaring bitmap index for low-cardinality columns"
---

# BITMAP Index

A bitmap index stores a Roaring Bitmap per distinct value, marking the rows that
contain it. Bitmaps for combined predicates can be intersected cheaply during
planning to skip non-matching row groups.

## When to Use

- Low-cardinality columns (status, region, event_type, flags)
- Queries combining several low-cardinality predicates with AND/OR
- Cases where a bloom filter is a poor fit because the column has few distinct values

## Syntax

```sql
CREATE INDEX idx_orders_status
ON prod.analytics.orders (status)
USING BITMAP
WITH ('bitmap.max_cardinality' = '1000');
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `bitmap.max_cardinality` | `10000` | Upper bound of distinct values accepted at build time |

## Query Behavior

```sql
SELECT * FROM prod.analytics.orders WHERE status = 'SHIPPED';

-- Bitmaps are intersected for compound predicates
SELECT * FROM prod.analytics.orders
WHERE status = 'SHIPPED' AND order_date >= DATE '2025-01-01';
```

Columns whose cardinality exceeds `bitmap.max_cardinality` are rejected at build
time; prefer [BLOOM_FILTER](bloom-filter.md) for high-cardinality columns.

**See Also:** [Index DDL](../index-ddl.md)
