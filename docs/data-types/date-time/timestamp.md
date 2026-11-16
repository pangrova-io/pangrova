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

Ordinary timestamp columns default to precision 6. The current DDL grammar
accepts only bare `TIMESTAMP`; `TIMESTAMP(p)` is not parsed, and `TIMESTAMP_NS`
is the precision-9 spelling. `TIME` uses up to six fractional digits, matching the
Native Worker's microsecond time storage.

## Connector mappings

| Connector | Write mapping | Read mapping |
|-----------|---------------|--------------|
| Apache Iceberg | `TimestampType.withoutZone()` | `TIMESTAMP` or `TIMESTAMP_NS` by declared precision |
| Apache Paimon | `TimestampType` (precision default 6) | `TIMESTAMP` |
| Lance | Not declared by the managed connector | Not declared |

`TIMESTAMP WITH TIME ZONE` maps to an Iceberg timestamp with UTC adjustment and to Paimon's
`LocalZonedTimestampType`. A `TIMESTAMP` value has no timezone; converting it to a
zoned timestamp depends on the session timezone.
