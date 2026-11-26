---
title: "SPARSE"
description: "Sparse sampling index for range queries"
---

# SPARSE Index

A sparse index samples every N-th row and records its value, building a lightweight
ordered summary. Range predicates can locate candidate row ranges without reading
full min/max footers for every block.

## When to Use

- Range predicates (`<`, `<=`, `>`, `>=`, `BETWEEN`) on sorted or near-sorted columns
- Very large tables where full indexes are too expensive
- Time-series style data with naturally clustered ordering

## Syntax

```sql
CREATE INDEX idx_events_time
ON prod.analytics.events (event_time)
USING SPARSE
WITH ('sparse.stride' = '1000')
FORCE;
```

`FORCE` overrides a conflicting connector-native capability such as Iceberg
min/max statistics; it does not replace an existing Pangrova index.

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `sparse.stride` | `1000` | Sample one row every `stride` rows |

## Query Behavior

```sql
SELECT * FROM prod.analytics.events
WHERE event_time >= TIMESTAMP '2025-06-01 00:00:00'
  AND event_time <  TIMESTAMP '2025-06-02 00:00:00';
```

The sparse index narrows the scan to candidate ranges; effectiveness depends on how
clustered the indexed column is within the data layout.

**See Also:** [Index DDL](../index-ddl.md), [BTREE](../lookup/btree.md) for precise range lookups
