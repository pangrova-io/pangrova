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
ROW(field_name => value, ...)
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
- Inserting `ROW(...)` into a named STRUCT column binds values to fields by their declared order.
- Both constructor names accept named and positional fields. Named fields retain their names through aggregation and subqueries.
- Quote field names containing spaces or quotes; double an embedded quote, as in `ROW("snap""id" => snapshot_id)`.
- Unnamed fields use their positional names, such as `"EXPR$0"`. Duplicate declared names are rejected.

**See Also:** [STRUCT data type](../../data-types/complex/struct.md)
