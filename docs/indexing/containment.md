---
title: "Containment"
description: "GIN index for composite value containment queries"
---

# Containment (GIN) Index

A GIN (Generalized Inverted Index) index maps each element of composite values —
arrays, maps — to the rows containing it. Containment predicates resolve element
membership through posting lists instead of scanning values.

## When to Use

- `ARRAY_CONTAINS` / `ARRAY_CONTAINS_ALL` filters on array columns
- Element membership tests on multi-value columns
- Tag-style schemas where rows carry variable-length element sets

## Syntax

```sql
CREATE INDEX idx_events_tags
ON prod.analytics.events (tags)
USING GIN
WITH ('gin.store_positions' = 'false');
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `gin.store_positions` | `false` | Store posting-list positions for direct row access |

## Query Behavior

```sql
SELECT * FROM prod.analytics.events WHERE ARRAY_CONTAINS(tags, 'promo');

SELECT * FROM prod.analytics.events
WHERE ARRAY_CONTAINS_ALL(tags, 'premium', 'featured');
```

**See Also:** [Index DDL](index-ddl.md), [ARRAY data type](../data-types/complex/array.md), [Array Functions](../sql-reference/functions/array.md)
