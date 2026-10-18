---
title: "FLOAT"
description: "32-bit IEEE 754 floating point"
---

# FLOAT


| Property | Value |
|----------|-------|
| Storage | 32-bit IEEE 754 floating point |
| Precision | ~7 decimal digits |

An expression mixing `FLOAT` and `DOUBLE` uses `DOUBLE`, regardless of operand
order. Planner uses single-precision type metadata for `FLOAT`; it must not reuse
Calcite's default double-precision `FLOAT` metadata.

```sql
CREATE TABLE measurements (id BIGINT, temperature FLOAT);
```
