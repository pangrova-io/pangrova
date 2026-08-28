---
title: "Literal-Only Types"
description: "TIME, TIMESTAMP WITH TIME ZONE, and INTERVAL literal types"
---

# Literal-Only Types

Types that appear in literal syntax or as part of timestamp literals. TIMESTAMP WITH TIME ZONE and INTERVAL have dedicated literal forms; TIME is a data type carried by connector schemas but has no literal syntax.


## TIME

| Property | Value |
|----------|-------|
| Storage | Time of day without timezone |
| Format | `HH:MM:SS` |
| Literal syntax | Not supported (use connector data or VARCHAR) |

---

## TIMESTAMP WITH TIME ZONE

| Property | Value |
|----------|-------|
| Storage | Timestamp with microsecond precision and timezone |
| Internal type | `TIMESTAMP_TZ` |

```sql
SELECT TIMESTAMP WITH TIME ZONE '2024-01-15 10:30:00+08:00' AS ts_tz;
```

---

## INTERVAL

| Property | Value |
|----------|-------|
| Variants | `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND` |
| Use case | Date/time arithmetic |

```sql
-- Year-to-month interval
SELECT INTERVAL '1-6' YEAR TO MONTH;

-- Day-to-second interval
SELECT INTERVAL '3 12:30:00' DAY TO SECOND;

-- Date arithmetic
SELECT event_time + INTERVAL '7' DAY AS next_week FROM events;
SELECT order_date - INTERVAL '1' MONTH AS last_month FROM orders;
```
