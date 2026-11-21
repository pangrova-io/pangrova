---
title: "GEOGRAPHY"
description: "WGS 84-tagged geospatial type: WKB storage, planar evaluation, and limits"
---

# GEOGRAPHY

`GEOGRAPHY` stores WGS 84-tagged geospatial values in Well-Known Binary (WKB). The type
carries geographic CRS metadata, but the current execution layer evaluates spatial
predicates in planar coordinates; geodesic (spherical) math is **not implemented**.

| Property | Value |
|----------|-------|
| Storage | Well-Known Binary (WKB) |
| CRS | CRS:84 metadata (WGS 84 longitude/latitude) |
| Evaluation | Planar (Cartesian) coordinates; geodesic math not implemented |
| Iceberg support | `GEOGRAPHY` type, Iceberg format-version 3 |
| Paimon support | Stored as `VARBINARY`; the spatial type is not restored on read |
| Lance support | Not declared by the managed connector |

## Declaration and insert

```sql
CREATE TABLE cities (
  id BIGINT,
  position GEOGRAPHY
) TBLPROPERTIES ('format-version' = '3');

-- POINT(-122.3321, 47.6062) — Seattle
INSERT INTO cities
VALUES (1, X'0101000000E5D022DBF99E5EC0F6285C8FC2E44740');
```

## Predicates

```sql
SELECT *
FROM regions
WHERE ST_INTERSECTS(boundary, point_geom);

SELECT *
FROM zones
WHERE ST_CONTAINS(zone_geom, query_point);

SELECT *
FROM areas
WHERE ST_WITHIN(query_point, area_geom);
```

The same constructors and explicit rejection rules as [GEOMETRY](geometry.md) apply:
WKT `POINT` and `POLYGON` are accepted, and unsupported geometries fail rather than
degrading to an approximate result.

## Interpretation boundary

- CRS metadata is preserved through Iceberg table properties.
- Predicates and distances use planar coordinates today. Do not interpret `ST_DISTANCE`
  or containment results as geodesic measurements.
- Paimon stores the value as binary; a Paimon round trip returns `VARBINARY`, not
  `GEOGRAPHY`.
- No spatial index is required for correctness; add an `R_TREE` index when bounding-box
  pruning is appropriate for the workload.

## Related

- [GEOMETRY](geometry.md) for the planar type
- [Spatial functions](../../sql-reference/functions/spatial.md)
- [Spatial index](../../indexing/spatial.md)
