---
title: "STRUCT"
description: "Named-field composite type"
---

# STRUCT


| Property | Value |
|----------|-------|
| Storage | Named field composite type |
| Field access | Dot notation |
| Aliases | `ROW` constructor |

**Declaration:**

```sql
CREATE TABLE contacts (
    id BIGINT,
    address STRUCT(street VARCHAR, city VARCHAR, zip VARCHAR)
);
```

**Construction:**

```sql
-- STRUCT constructor with named fields
SELECT STRUCT(name => 'Alice', age => 30) AS person;

-- ROW constructor (positional)
SELECT ROW('123 Main St', 'Seattle', '98101') AS addr;
```

**Access:**

```sql
-- Dot notation for field access
SELECT address.city, address.zip FROM contacts;
```
