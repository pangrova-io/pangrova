---
title: "MAP"
description: "Key-value mapping type: declaration, access, nulls, and connector mappings"
---

# MAP

`MAP` stores an unordered key-value mapping. Keys must be a primitive type; values may be
any supported type, including nested ARRAY, MAP, and STRUCT values.

| Property | Value |
|----------|-------|
| Storage | Key-value mapping |
| Key type | Primitive type only |
| Value type | Any supported type, including nested types |
| Duplicate keys | Not representable; a mapping has at most one value per key |
| Null values | Supported when the value type is nullable |

## Declaration

```sql
CREATE TABLE configs (
  id BIGINT,
  settings MAP(VARCHAR, VARCHAR),
  counters MAP(VARCHAR, BIGINT)
);
```

## Construction

```sql
SELECT MAP['host', 'localhost', 'port', '8080'] AS config;
```

The current canonical MAP constructor requires non-null literal keys and literal
values; runtime expressions are rejected. Keys must be unique, and the literal
must be assignable to the declared map type. Use compatible key and value types.

## Access

```sql
SELECT settings['host'] AS host_value
FROM configs;

SELECT id
FROM configs
WHERE settings['region'] = 'eu';
```

A missing key yields `NULL`. Accessing a map that is itself `NULL` yields `NULL`. Use
`IS NULL` when absence must be distinguished from an empty mapping.

## Functions and expansion

Map construction, lookup, and membership helpers are listed in
[Map functions](../../sql-reference/functions/map.md). `UNNEST(map)` expands entries in the
`FROM` clause; see [table functions](../../sql-reference/functions/table-functions.md).

## Connector mappings

| Connector | Write mapping | Read mapping |
|-----------|---------------|--------------|
| Apache Iceberg | Iceberg `MAP` with declared key/value types | `MAP` with preserved key/value types |
| Apache Paimon | Paimon `MAP` | `MAP` with preserved key/value types |
| Lance | Canonical Arrow map schema, subject to Native validation | `MAP` with persisted key/value-type signatures |

## Example

```sql
CREATE TABLE sessions (
  session_id BIGINT,
  attributes MAP(VARCHAR, VARCHAR)
);

INSERT INTO sessions
VALUES (1, MAP['browser', 'firefox', 'region', 'eu']);

SELECT session_id, attributes['region'] AS region
FROM sessions
WHERE attributes['browser'] = 'firefox';
```

## Related

- [ARRAY](array.md) and [STRUCT](struct.md) for the other nested types
- [Map functions](../../sql-reference/functions/map.md)
- [Type casting](../type-casting.md)
