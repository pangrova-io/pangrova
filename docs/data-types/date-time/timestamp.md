---
title: "TIMESTAMP"
description: "Timestamp with microsecond precision"
---

# TIMESTAMP


| Property | Value |
|----------|-------|
| Storage | Timestamp with microsecond precision (without timezone) |
| Format | `YYYY-MM-DD HH:MM:SS.ffffff` |

```sql
SELECT TIMESTAMP '2024-01-15 10:30:00' AS ts;
INSERT INTO logs (created_at) VALUES (TIMESTAMP '2024-06-01 12:00:00.123456');
```
