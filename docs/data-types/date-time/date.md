---
title: "DATE"
description: "Calendar date type"
---

# DATE


| Property | Value |
|----------|-------|
| Storage | Calendar date (year, month, day) |
| Format | `YYYY-MM-DD` |

Java result decoding returns `java.time.LocalDate`, including DATE values nested
inside ROW, ARRAY, and MAP. Arrow day and millisecond encodings are interpreted
using their declared units; millisecond dates use UTC and do not depend on the
client's default timezone.

```sql
SELECT DATE '2024-01-15' AS d;
INSERT INTO events (event_date) VALUES (DATE '2024-06-01');
```
