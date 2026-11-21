---
title: "VARIANT"
description: "Semi-structured JSON access with explicit path expressions"
---

# VARIANT


| Property | Value |
|----------|-------|
| Storage | JSON text in the currently wired Iceberg and Paimon paths |
| Insert format | JSON string literal |
| Query access | `variant_get(column, path)` function |

**Declaration:**

```sql
CREATE TABLE events (
    id BIGINT,
    payload VARIANT
) TBLPROPERTIES ('format-version' = '3');
```

**Insert:**

```sql
INSERT INTO events (id, payload) VALUES
    (1, '{"name": "alice", "age": 30}'),
    (2, '{"name": "bob", "address": {"city": "seattle"}}'),
    (3, '{"items": [{"name": "widget", "qty": 5}]}');
```

**Path Expressions:**

`variant_get` uses Pangrova's field/index path syntax. Pass `name`,
`address.city`, or `items[0]` directly; the `$` prefix used by JSONPath
functions is not part of this function's contract.

```sql
-- Dot notation: top-level field
SELECT variant_get(payload, 'name') AS name FROM events;

-- Bracket notation: special characters in key
SELECT variant_get(payload, '[''user-name'']') AS uname FROM events;

-- Nested dot path
SELECT variant_get(payload, 'address.city') AS city FROM events;

-- Array index access
SELECT variant_get(payload, 'items[0]') AS first_item FROM events;

-- Mixed nested path
SELECT variant_get(payload, 'items[0].name') AS item_name FROM events;
```

**Notes:**
- Returns NULL for missing paths or out-of-bounds array indices
- Supports both Iceberg and Paimon connectors
- Iceberg requires `format-version = '3'` for VARIANT columns
- The current Iceberg and Paimon E2E paths store JSON text. Iceberg v3
  shredded VARIANT decoding is not yet wired into the Native scan path.
