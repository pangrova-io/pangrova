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

-- An empty array takes its element type from the target column
INSERT INTO tags VALUES (2, ARRAY[]);
```

`ARRAY[]` requires a typed context such as an `ARRAY(VARCHAR)` insert column;
an untyped empty array has no element type to infer.

**Connector mappings:**

| Connector | Write mapping | Read mapping |
|-----------|---------------|--------------|
| Apache Iceberg | Iceberg `LIST` with the declared element type | `ARRAY` with preserved element type |
| Apache Paimon | Paimon `ARRAY` | `ARRAY` with preserved element type |
| Lance | Canonical Arrow list schema, subject to Native validation | `ARRAY` with its persisted element-type signature |

Element nullability follows the declared element type. A `NULL` array is distinct from an
empty array; `labels[1]` on an empty array returns `NULL`.

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
