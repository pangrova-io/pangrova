Pangrova > Guide > Functions > Array, Map & Struct Functions

# Array, Map & Struct Functions

---

## Array Construction

```sql
ARRAY[expr1, expr2, ...]
```

```sql
SELECT ARRAY[1, 2, 3] AS numbers;
SELECT ARRAY['red', 'green', 'blue'] AS colors;
INSERT INTO tags (id, labels) VALUES (1, ARRAY['urgent', 'bug', 'p0']);
```

---

## ARRAY_CONTAINS

```sql
ARRAY_CONTAINS(array, value) → BOOLEAN
```

```sql
SELECT * FROM products WHERE ARRAY_CONTAINS(tags, 'sale');
```

---

## ARRAY_CONTAINS_ALL

```sql
ARRAY_CONTAINS_ALL(array, values_array) → BOOLEAN
```

Returns TRUE if the array contains all elements in the values array.

```sql
SELECT * FROM products WHERE ARRAY_CONTAINS_ALL(tags, ARRAY['premium', 'featured']);
```

---

## Map Construction

```sql
MAP[key1, value1, key2, value2, ...]
```

```sql
SELECT MAP['name', 'Alice', 'role', 'admin'] AS profile;
INSERT INTO configs (id, settings) VALUES (1, MAP['timeout', '30', 'retries', '3']);
```

---

## Map Access

```sql
map_column['key'] → value_type
```

```sql
SELECT settings['timeout'] AS timeout_value FROM configs;
```

---

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

---

## Struct Field Access

```sql
struct_column.field_name
```

```sql
SELECT address.city, address.zip FROM contacts;
```

---

## EXPLODE

```sql
EXPLODE(array_expr) → rows
```

Expands an array into individual rows. Used with `LATERAL TABLE`.

```sql
SELECT t.id, item
FROM orders t, LATERAL TABLE(EXPLODE(t.items)) AS e(item);
```

---

## POSEXPLODE

```sql
POSEXPLODE(array_expr) → (pos, value) rows
```

Like EXPLODE but includes a 0-based position index.

```sql
SELECT t.id, pos, item
FROM orders t, LATERAL TABLE(POSEXPLODE(t.items)) AS e(pos, item);
```

---

## GENERATE_SERIES

```sql
GENERATE_SERIES(start, end) → rows of integers
```

Generates a sequence of integers from start to end (inclusive).

```sql
SELECT n FROM TABLE(GENERATE_SERIES(1, 10)) AS s(n);

-- Use for cross-join expansion
SELECT d.id, n
FROM data d, TABLE(GENERATE_SERIES(1, d.repeat_count)) AS s(n);
```

---

## Notes

- Array indexing is 1-based in SQL context
- NULL elements are allowed in arrays and maps
- STRUCT fields are accessed via dot notation
- EXPLODE on NULL array produces zero rows

**See Also:** [Data Types — Complex Types](../data-types.md#4-complex-types), [SQL Reference — Table Functions](../sql-reference.md#table-functions)
