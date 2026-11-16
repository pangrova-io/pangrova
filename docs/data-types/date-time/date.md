---
title: "DATE"
description: "Calendar date type: storage, literals, casting, mappings, and timezone rules"
---

# DATE

`DATE` is a calendar date without a time of day or timezone. It is the correct type for
business dates, partition keys, and date-only predicates.

| Property | Value |
|----------|-------|
| Storage | Calendar date (year, month, day) |
| Format | `YYYY-MM-DD` |
| Range | `0001-01-01` through `9999-12-31` |
| Timezone | None; the value does not shift with the session timezone |

## Literals and casts

```sql
SELECT DATE '2024-01-15' AS d;
SELECT CAST('2024-01-15' AS DATE);
SELECT CAST(TIMESTAMP '2024-01-15 10:30:00' AS DATE);  -- truncates to 2024-01-15
```

A string cast must contain a complete valid date; invalid input produces a runtime error.
`DATE` is not a cast target for numeric values.

## Arithmetic and comparison

```sql
SELECT DATE '2024-01-31' + INTERVAL '1' DAY AS next_day;
SELECT d FROM events WHERE d BETWEEN DATE '2024-01-01' AND DATE '2024-03-31';
```

Dates compare chronologically. Date ± interval arithmetic follows the SQL interval rules
of the executing engine; validate month-end and leap-day behavior for the workload.

## Result decoding

Java result decoding returns `java.time.LocalDate`, including DATE values nested inside
ROW, ARRAY, and MAP. Arrow day and millisecond encodings are interpreted using their
declared units; millisecond dates use UTC and do not depend on the client's default
timezone.

## Connector mappings

| Connector | Write mapping | Read mapping |
|-----------|---------------|--------------|
| Apache Iceberg | `DATE` | `DATE` |
| Apache Paimon | `DateType` | `DATE` |
| Lance | Not declared by the managed connector | Not declared |

## Example

```sql
CREATE TABLE events (
  event_id BIGINT,
  event_date DATE
);

INSERT INTO events VALUES (1, DATE '2024-06-01');

SELECT event_date, COUNT(*) AS events
FROM events
GROUP BY event_date
ORDER BY event_date;
```

## Related

- [TIMESTAMP](timestamp.md) for date-time values
- [Date and time functions](../../sql-reference/functions/date-time.md)
- [Type casting](../type-casting.md)
