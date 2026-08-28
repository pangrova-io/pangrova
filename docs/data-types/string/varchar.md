---
title: "VARCHAR"
description: "Variable-length UTF-8 string"
---

# VARCHAR


| Property | Value |
|----------|-------|
| Storage | Variable-length UTF-8 string |
| Max length | Optional constraint via `VARCHAR(n)` |
| Alias | `STRING` |

```sql
CREATE TABLE profiles (
    id BIGINT,
    name VARCHAR,          -- Unbounded
    email VARCHAR(255),    -- Max 255 characters
    bio STRING             -- Alias for VARCHAR
);
```

**Operators:**
- Concatenation: `||` or `CONCAT(a, b)`
- Pattern matching: `LIKE`
- Comparison: `=`, `<>`, `<`, `>`, `<=`, `>=`
