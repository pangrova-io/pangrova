---
title: "Spatial Index (R_TREE)"
description: "R-Tree index for bounding-box and geospatial predicates"
---

# Spatial (R_TREE) Index

An R-Tree index organizes geometry bounding boxes into a balanced hierarchy of
minimum bounding rectangles. Spatial predicates prune candidate rows by
bounding-box overlap before exact geometry evaluation.

## When to Use

- `ST_INTERSECTS`, `ST_WITHIN`, `ST_CONTAINS`, and other spatial predicates over
  `GEOMETRY` / `GEOGRAPHY` columns
- Geofencing, map viewport queries, and proximity filters
- Large tables where brute-force geometry evaluation dominates query time

## Syntax

```sql
CREATE INDEX idx_stores_location
ON prod.analytics.stores (location)
USING R_TREE;
```

## Query Behavior

```sql
SELECT store_id, name
FROM prod.analytics.stores
WHERE ST_INTERSECTS(location, ST_GEOMFROMTEXT('POLYGON((-122.4 37.7, -122.3 37.7, -122.3 37.8, -122.4 37.8, -122.4 37.7))'));
```

The R-Tree answers with candidate rows whose bounding boxes intersect the query
window; the exact spatial predicate is re-evaluated on candidates.

**See Also:** [Index DDL](index-ddl.md), [Spatial Functions](../sql-reference/functions/spatial.md), [GEOMETRY](../data-types/spatial/geometry.md)
