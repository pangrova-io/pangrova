---
title: "GEOGRAPHY"
description: "Geodesic geospatial type (WKB encoded)"
---

# GEOGRAPHY


| Property | Value |
|----------|-------|
| Storage | Well-Known Binary (WKB) encoded |
| CRS | CRS:84 (WGS 84, spherical coordinates) |
| Use case | Geodesic (spherical) geospatial operations |

**Declaration:**

```sql
CREATE TABLE cities (id BIGINT, position GEOGRAPHY)
    TBLPROPERTIES ('format-version' = '3');
```

**Insert (WKB hex):**

```sql
-- POINT(-122.3321, 47.6062) — Seattle
INSERT INTO cities VALUES (1, X'0101000000E5D022DBF99E5EC0F6285C8FC2E44740');
```

**Spatial Functions:**

```sql
SELECT * FROM regions
WHERE ST_INTERSECTS(boundary, point_geom);

SELECT * FROM zones
WHERE ST_CONTAINS(zone_geom, query_point);

SELECT * FROM areas
WHERE ST_WITHIN(query_point, area_geom);
```

**Notes:**
- Stored as VARBINARY internally with zero additional overhead
- Requires Iceberg format-version 3 for full support
- CRS metadata is preserved through Iceberg table properties

**See Also:** [Functions — Spatial](../../sql-reference/functions/spatial.md)
