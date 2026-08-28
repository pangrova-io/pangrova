---
title: "VARIANT"
description: "Semi-structured JSON with shredded columnar encoding"
---

# VARIANT


| Property | Value |
|----------|-------|
| Storage | Semi-structured JSON with shredded columnar encoding |
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
- Internally uses shredded columnar encoding for efficient selective access
