Pangrova > Guide > Functions > DateTime Functions

# Date/Time Functions

---

## EXTRACT

```sql
EXTRACT(field FROM expr) → BIGINT
```

Extracts a component from a date/timestamp value.

**Fields:** `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`

```sql
SELECT EXTRACT(YEAR FROM order_date) AS year,
       EXTRACT(MONTH FROM order_date) AS month,
       EXTRACT(DAY FROM order_date) AS day
FROM prod.analytics.orders;

SELECT EXTRACT(HOUR FROM event_time) AS hour,
       EXTRACT(MINUTE FROM event_time) AS minute
FROM prod.analytics.events;
```

---

## Date/Time Literals

```sql
DATE 'YYYY-MM-DD'
TIMESTAMP 'YYYY-MM-DD HH:MM:SS[.ffffff]'
TIMESTAMP WITH TIME ZONE 'YYYY-MM-DD HH:MM:SS+HH:MM'
```

```sql
SELECT DATE '2024-06-15';
SELECT TIMESTAMP '2024-06-15 10:30:00.123456';
SELECT TIMESTAMP WITH TIME ZONE '2024-06-15 10:30:00+08:00';
```

---

## INTERVAL Literals and Arithmetic

```sql
INTERVAL 'value' unit [TO unit]
```

**Units:** `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`

```sql
-- Simple intervals
SELECT INTERVAL '30' DAY;
SELECT INTERVAL '2' HOUR;
SELECT INTERVAL '1-6' YEAR TO MONTH;  -- 1 year 6 months
SELECT INTERVAL '3 12:30:00' DAY TO SECOND;  -- 3 days 12h 30m

-- Date arithmetic
SELECT order_date + INTERVAL '7' DAY AS ship_by FROM orders;
SELECT event_time - INTERVAL '1' HOUR AS hour_ago FROM events;
SELECT created_at + INTERVAL '3' MONTH AS renewal_date FROM subscriptions;
```

---

## Current Date/Time

```sql
CURRENT_DATE → DATE
CURRENT_TIMESTAMP → TIMESTAMP
```

```sql
SELECT * FROM orders WHERE order_date = CURRENT_DATE;
SELECT * FROM events WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR;
```

---

## Notes

- TIMESTAMP has microsecond precision by default
- TIMESTAMP_NS provides nanosecond precision for high-frequency data
- INTERVAL arithmetic works with DATE, TIMESTAMP, and TIMESTAMP_TZ
- Time zone-aware timestamps normalize to UTC for storage

**See Also:** [Data Types — Date/Time](../data-types.md#3-datetime-types), [SQL Reference](../sql-reference.md)
