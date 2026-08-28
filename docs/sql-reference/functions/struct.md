---
title: "Struct"
description: "STRUCT / ROW construction and field access"
---

# Struct Functions

Functions for constructing and accessing [STRUCT](../../data-types/complex/struct.md) values.

## STRUCT / ROW Construction

```sql
STRUCT(field_name => value, ...)
ROW(value1, value2, ...)
```

```sql
-- Named fields
SELECT STRUCT(name => 'Alice', age => 30) AS person;

-- Positional (ROW)
SELECT ROW('123 Main St', 'Seattle', '98101') AS address;
```

## Struct Field Access

```sql
struct_column.field_name
```

```sql
SELECT address.city, address.zip FROM contacts;
```

## Notes

- STRUCT fields are accessed via dot notation
- ROW produces positional fields referenced by ordinal

**See Also:** [STRUCT data type](../../data-types/complex/struct.md)
