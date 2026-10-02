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

Literal conversion preserves the declared fractional precision, including dates
before the Unix epoch. The planner encodes Substrait timestamp values as signed
integers in units of `10^-precision` seconds; the Native Worker converts these to
seconds and a nonnegative nanosecond remainder for both projections and `VALUES`
writes. Timestamp literals with a timezone use the same unit contract after the
planner has resolved the timezone.

Ordinary timestamp columns default to precision 6; explicit timestamp precision
may range from 0 to 9. `TIME` uses up to six fractional digits, matching the
Native Worker's microsecond time storage.
