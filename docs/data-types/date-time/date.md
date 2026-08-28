---
title: "DATE"
description: "Calendar date type"
---

# DATE


| Property | Value |
|----------|-------|
| Storage | Calendar date (year, month, day) |
| Format | `YYYY-MM-DD` |

```sql
SELECT DATE '2024-01-15' AS d;
INSERT INTO events (event_date) VALUES (DATE '2024-06-01');
```
