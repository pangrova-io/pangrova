---
title: "Spatial"
description: "ST_ geospatial predicates and constructor functions"
---

# Spatial Functions

Geospatial functions for GEOMETRY and GEOGRAPHY types. Data is stored in WKB (Well-Known Binary) format.

---

## ST_GEOMFROMTEXT

```sql
ST_GEOMFROMTEXT(wkt) → VARBINARY
```

Parses a WKT (Well-Known Text) string into a GEOMETRY value (encoded as WKB).

```sql
SELECT ST_GEOMFROMTEXT('POINT (116.39 39.91)') AS point;
```

---

## ST_GEOGRAPHYFROMTEXT

```sql
ST_GEOGRAPHYFROMTEXT(wkt) → VARBINARY
```

Parses a WKT string into a GEOGRAPHY value (encoded as WKB).

```sql
SELECT ST_GEOGRAPHYFROMTEXT('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') AS region;
```

---

## ST_INTERSECTS

```sql
ST_INTERSECTS(geom1, geom2) → BOOLEAN
```

Returns TRUE if the two geometries share any spatial area (have at least one point in common).

```sql
SELECT * FROM buildings
WHERE ST_INTERSECTS(footprint, search_area);
```

---

## ST_WITHIN

```sql
ST_WITHIN(geom1, geom2) → BOOLEAN
```

Returns TRUE if `geom1` is completely inside `geom2`.

```sql
-- Find all stores within a city boundary
SELECT * FROM stores
WHERE ST_WITHIN(location, city_boundary);
```

---

## ST_CONTAINS

```sql
ST_CONTAINS(geom1, geom2) → BOOLEAN
```

Returns TRUE if `geom1` completely contains `geom2`. Inverse of ST_WITHIN.

```sql
-- Find regions that contain a specific point
SELECT region_name FROM regions
WHERE ST_CONTAINS(boundary, query_point);
```

---

## Notes

- GEOMETRY uses planar (Cartesian) coordinate math
- GEOGRAPHY uses geodesic (spherical) coordinate math on WGS 84
- Both types are stored as WKB binary (reuses VARBINARY column reader)
- Iceberg format-version 3 is required for GEOMETRY/GEOGRAPHY columns
- CRS metadata (default CRS:84) is preserved through Iceberg table properties

**See Also:** [GEOMETRY](../../data-types/spatial/geometry.md), [GEOGRAPHY](../../data-types/spatial/geography.md)
