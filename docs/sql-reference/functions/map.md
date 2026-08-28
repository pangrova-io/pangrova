---
title: "Map"
description: "MAP construction and key access"
---

# Map Functions

Functions for constructing and accessing [MAP](../../data-types/complex/map.md) values.

## Map Construction

```sql
MAP[key1, value1, key2, value2, ...]
```

```sql
SELECT MAP['name', 'Alice', 'role', 'admin'] AS profile;
INSERT INTO configs (id, settings) VALUES (1, MAP['timeout', '30', 'retries', '3']);
```

## Map Access

```sql
map_column['key'] → value_type
```

```sql
SELECT settings['timeout'] AS timeout_value FROM configs;
```

## Notes

- Keys must share a common type; values must share a common type
- NULL values are allowed; looking up a missing key returns NULL

**See Also:** [MAP data type](../../data-types/complex/map.md)
