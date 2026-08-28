---
title: "ARRAY"
description: "Ordered collection type"
---

# ARRAY


| Property | Value |
|----------|-------|
| Storage | Ordered collection of elements |
| Element type | Any supported type |
| Nesting | Supported (e.g., `ARRAY(ARRAY(INT))`) |

**Declaration:**

```sql
CREATE TABLE tags (id BIGINT, labels ARRAY(VARCHAR));
```

**Construction:**

```sql
-- Array literal
SELECT ARRAY['red', 'green', 'blue'] AS colors;

-- Insert array values
INSERT INTO tags VALUES (1, ARRAY['urgent', 'bug']);
```

**Access:**

```sql
-- Access by index (1-based in SQL context)
SELECT labels[1] AS first_label FROM tags;
```

**EXPLODE (table function):**

```sql
-- Unnest array to rows
SELECT t.id, label
FROM tags t, LATERAL TABLE(EXPLODE(t.labels)) AS e(label);

-- With position
SELECT t.id, pos, label
FROM tags t, LATERAL TABLE(POSEXPLODE(t.labels)) AS e(pos, label);
```
