---
title: "BIGINT"
description: "64-bit signed integer: range, casts, connector mappings, and limits"
---

# BIGINT

`BIGINT` is a 64-bit signed integer. Use it for identifiers, event timestamps expressed in
epoch units, and counters that can exceed the 32-bit range.

| Property | Value |
|----------|-------|
| Storage | 64-bit signed integer |
| Range | -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807 |
| Null | Supported; SQL `NULL` is distinct from `0` |
| Alias | None |

## Literals and casts

```sql
SELECT CAST(9223372036854775807 AS BIGINT);
SELECT CAST(-9223372036854775808 AS BIGINT);
```

Widening from `INTEGER` is lossless. Narrowing to a smaller integer width is not; the
cast can lose precision or fail, so validate ranges before narrowing.

## Connector mappings

| Connector | Write mapping | Read mapping |
|-----------|---------------|--------------|
| Apache Iceberg | `LONG` | `BIGINT` |
| Apache Paimon | `BIGINT` | `BIGINT` |
| Lance | Arrow signed 64-bit integer | `BIGINT` |

Timestamps, snapshot identifiers, row counts, and byte offsets in Pangrova metadata use
64-bit values; do not narrow them to `INTEGER` in application code.

## Example

```sql
CREATE TABLE events (
  event_id BIGINT,
  event_time_ms BIGINT
);

INSERT INTO events VALUES (9007199254740993, 1735689600000);

SELECT event_id
FROM events
WHERE event_time_ms >= 1735689600000;
```

## Related

- [Type casting](../type-casting.md)
- [INT / INTEGER](integer.md) for 32-bit values
- [TIMESTAMP](../date-time/timestamp.md) for calendar timestamps
