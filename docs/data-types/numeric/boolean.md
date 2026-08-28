---
title: "BOOLEAN"
description: "True/false logical type"
---

# BOOLEAN


| Property | Value |
|----------|-------|
| Storage | 1 bit |
| Values | `TRUE`, `FALSE`, `NULL` |

```sql
CREATE TABLE flags (id BIGINT, active BOOLEAN);
INSERT INTO flags VALUES (1, TRUE), (2, FALSE);
```
