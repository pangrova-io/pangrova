---
title: "Array"
description: "ARRAY construction, containment predicates, and element access"
---

# Array Functions

Functions for constructing and inspecting [ARRAY](../../data-types/complex/array.md) values.

## Array Construction

```sql
ARRAY[expr1, expr2, ...]
```

```sql
SELECT ARRAY[1, 2, 3] AS numbers;
SELECT ARRAY['red', 'green', 'blue'] AS colors;
INSERT INTO tags (id, labels) VALUES (1, ARRAY['urgent', 'bug', 'p0']);
```

## ARRAY_CONTAINS

```sql
ARRAY_CONTAINS(array, value) → BOOLEAN
```

```sql
SELECT * FROM products WHERE ARRAY_CONTAINS(tags, 'sale');
```

## ARRAY_CONTAINS_ALL

```sql
ARRAY_CONTAINS_ALL(array, value1, value2, ...) → BOOLEAN
```

Returns TRUE if the array contains every supplied value.

```sql
SELECT * FROM products WHERE ARRAY_CONTAINS_ALL(tags, 'premium', 'featured');
```

## Element Access

```sql
array_column[index] → element_type
```

Array indexing is 1-based in SQL context.

```sql
SELECT labels[1] AS first_label FROM tags;
```

## Notes

- NULL elements are allowed in arrays
- `EXPLODE` and `POSEXPLODE` expand arrays into rows — see [Table Functions](table-functions.md)

**See Also:** [ARRAY data type](../../data-types/complex/array.md), [Table Functions](table-functions.md)
