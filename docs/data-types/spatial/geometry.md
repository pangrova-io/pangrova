---
title: "GEOMETRY"
description: "Planar geospatial type: WKB storage, CRS metadata, mappings, and limits"
---

# GEOMETRY

`GEOMETRY` stores planar (Cartesian) geospatial values in Well-Known Binary (WKB). It is
intended for projected coordinates and planar predicates such as bounding-box intersection.

| Property | Value |
|----------|-------|
| Storage | Well-Known Binary (WKB) |
| Default CRS | CRS:84 metadata (WGS 84 longitude/latitude) |
| Evaluation | Planar (Cartesian) coordinates |
| Iceberg support | `GEOMETRY` type, Iceberg format-version 3 |
| Paimon support | Stored as `VARBINARY`; spatial semantics are not restored on read |
| Lance support | Not declared by the managed connector |

Geodesic (spherical) distance and area are not implemented. Values declared as `GEOMETRY`
are evaluated in planar coordinates even when the CRS metadata is geographic.

## Declaration and insert

```sql
CREATE TABLE locations (
  id BIGINT,
  shape GEOMETRY
) TBLPROPERTIES ('format-version' = '3');

-- POINT(1.0 2.0) as WKB little-endian
INSERT INTO locations
VALUES (1, X'0101000000000000000000F03F0000000000000040');
```

## Construction and predicates

```sql
SELECT ST_GEOMFROMTEXT('POINT(1 2)') AS point_value;

SELECT id
FROM locations
WHERE ST_INTERSECTS(
  shape,
  ST_GEOMFROMTEXT('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))')
);
```

Supported constructors accept WKT `POINT` and `POLYGON` values, including multi-ring
polygons with holes. Non-POINT/POLYGON input, empty rings, and unclosed rings are rejected
explicitly rather than producing a fabricated result.

## Indexing

An `R_TREE` index accelerates bounding-box pruning for
[spatial predicates](../../indexing/spatial.md). The index narrows candidates by minimum
bounding rectangle; the exact predicate is re-evaluated on each candidate.

## Example with a spatial index

```sql
CREATE INDEX idx_locations_shape
ON locations (shape)
USING R_TREE;

SELECT id
FROM locations
WHERE ST_WITHIN(shape, ST_GEOMFROMTEXT('POLYGON((-1 -1, 3 -1, 3 3, -1 3, -1 -1))'));
```

## Related

- [GEOGRAPHY](geography.md) for the geographic-tagged variant
- [Spatial functions](../../sql-reference/functions/spatial.md)
- [Spatial index](../../indexing/spatial.md)
- [Type casting](../type-casting.md)
