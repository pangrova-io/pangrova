---
title: "GEOMETRY"
description: "Planar geospatial type (WKB encoded)"
---

# GEOMETRY


| Property | Value |
|----------|-------|
| Storage | Well-Known Binary (WKB) encoded |
| CRS | CRS:84 (WGS 84, lon/lat) by default |
| Use case | Planar (Cartesian) geospatial operations |

**Declaration:**

```sql
CREATE TABLE locations (
    id BIGINT,
    shape GEOMETRY
) TBLPROPERTIES ('format-version' = '3');
```

**Insert (WKB hex):**

```sql
-- POINT(1.0 2.0) as WKB little-endian
INSERT INTO locations VALUES (1, X'0101000000000000000000F03F0000000000000040');
```
