---
title: "VARCHAR"
description: "Variable-length UTF-8 string: length rules, operators, mappings, and limits"
---

# VARCHAR

`VARCHAR` is a variable-length UTF-8 string. `VARCHAR(n)` declares a maximum length in
characters; `VARCHAR` without a parameter is unbounded. `STRING` is accepted as an alias.

| Property | Value |
|----------|-------|
| Storage | Variable-length UTF-8 string |
| Length | `VARCHAR(n)` declares `1 <= n` characters; unbounded when omitted |
| Alias | `STRING` |
| Null | Supported; empty string and `NULL` are distinct |

## Length semantics

The declared length is part of the logical type. Iceberg stores `VARCHAR` as unbounded
`STRING`, and Paimon writes it as `VarChar(MAX_LENGTH)`; those connectors do not enforce a
smaller `VARCHAR(n)` limit at write time. Enforce application-level limits when a maximum is
a correctness requirement, and validate the behavior of the selected connector.

## Literals and operators

```sql
SELECT 'hello' || ' ' || 'lakehouse' AS greeting;
SELECT CONCAT('a', 'b') AS joined;
SELECT name FROM profiles WHERE name LIKE 'A%';
```

| Operator | Meaning |
|---|---|
| `||` | Concatenation |
| `CONCAT(a, b, ...)` | Concatenation function |
| `LIKE` | Pattern matching |
| `=`, `<>`, `<`, `>`, `<=`, `>=` | Binary comparison by UTF-8 code point order |

## Casting and parsing

`CAST(expression AS VARCHAR)` renders the value using the SQL textual form. String to
numeric casts parse the complete string; invalid input produces a runtime error rather than
a default. `CAST('2024-01-15' AS DATE)` and `CAST('2024-01-15 10:30:00' AS TIMESTAMP)` use
the documented literal formats.

## Connector mappings

| Connector | Write mapping | Read mapping |
|-----------|---------------|--------------|
| Apache Iceberg | `STRING` | `STRING` → `VARCHAR` (unbounded) |
| Apache Paimon | `VarChar(MAX_LENGTH)` | `VARCHAR` |
| Lance | Not declared by the managed connector | Not declared |

## Example

```sql
CREATE TABLE profiles (
  id BIGINT,
  name VARCHAR,
  email VARCHAR(255),
  bio STRING
);

INSERT INTO profiles VALUES (1, 'Alice', 'alice@example.com', 'Data engineer');

SELECT id, name
FROM profiles
WHERE email LIKE '%@example.com'
ORDER BY name;
```

## Related

- [Type casting](../type-casting.md)
- [String functions](../../sql-reference/functions/string.md)
- [BINARY](../binary.md) for raw byte strings
