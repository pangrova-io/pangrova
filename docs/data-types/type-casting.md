---
title: "Type Casting"
description: "CAST syntax and the type compatibility matrix"
---

# Type Casting

Rules and examples for converting values between data types.


## CAST Syntax

```sql
CAST(expression AS target_type)
```

**Examples:**

```sql
-- Numeric conversions
SELECT CAST(42 AS BIGINT);
SELECT CAST(3.14 AS DECIMAL(5,2));
SELECT CAST('123' AS INT);

-- String conversions
SELECT CAST(42 AS VARCHAR);
SELECT CAST(DATE '2024-01-01' AS VARCHAR);

-- Date/time conversions
SELECT CAST('2024-01-15' AS DATE);
SELECT CAST('2024-01-15 10:30:00' AS TIMESTAMP);

-- Complex type casting
SELECT CAST(ARRAY[1, 2, 3] AS ARRAY(BIGINT)) FROM t;
```

## Type Compatibility Matrix

| From \ To | INT | BIGINT | FLOAT | DOUBLE | DECIMAL | VARCHAR | BOOLEAN |
|-----------|-----|--------|-------|--------|---------|---------|---------|
| INT | - | Yes | Yes | Yes | Yes | Yes | No |
| BIGINT | Narrow | - | Yes | Yes | Yes | Yes | No |
| FLOAT | Truncate | Truncate | - | Yes | Yes | Yes | No |
| DOUBLE | Truncate | Truncate | Narrow | - | Yes | Yes | No |
| DECIMAL | Truncate | Truncate | Yes | Yes | - | Yes | No |
| VARCHAR | Parse | Parse | Parse | Parse | Parse | - | Parse |
| BOOLEAN | No | No | No | No | No | Yes | - |

**Notes:**
- "Narrow" = possible loss of precision or overflow
- "Truncate" = decimal portion is removed
- "Parse" = string must be a valid representation of the target type
- Invalid casts produce a runtime error
- CAST targets: numeric types, BOOLEAN, DATE, TIMESTAMP, STRING/VARCHAR, DECIMAL/NUMERIC, UUID, and ARRAY/MAP/STRUCT. Vector and spatial types are not cast targets.
