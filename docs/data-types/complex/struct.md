---
title: "STRUCT"
description: "Named-field composite type: declaration, access, nulls, and connector mappings"
---

# STRUCT

`STRUCT` is a composite value with named, typed fields. Fields may use any supported type,
including nested ARRAY, MAP, and STRUCT values. `ROW(...)` is accepted as the positional
constructor form.

| Property | Value |
|----------|-------|
| Storage | Named-field composite value |
| Field access | Dot notation |
| Field order | Declared order; preserved by the connectors below |
| Nulls | The struct or an individual field may be nullable |

## Declaration

```sql
CREATE TABLE contacts (
  id BIGINT,
  address STRUCT(street VARCHAR, city VARCHAR, zip VARCHAR)
);
```

## Construction

```sql
-- Named-field constructor
SELECT STRUCT(name => 'Alice', age => 30) AS person;

-- Positional ROW constructor
SELECT ROW('123 Main St', 'Seattle', '98101') AS addr;
```

The positional form assigns values in declaration order. A type mismatch or a missing
required field fails rather than filling an implicit default.

## Access

```sql
SELECT address.city, address.zip
FROM contacts;

SELECT person.name
FROM (SELECT STRUCT(name => 'Alice', age => 30) AS person);
```

Accessing a field of a `NULL` struct yields `NULL`. A struct is not equal to a ROW value
from another query unless both field order and field values match under the SQL comparison
rules.

## Connector mappings

| Connector | Write mapping | Read mapping |
|-----------|---------------|--------------|
| Apache Iceberg | Iceberg `STRUCT` with declared field order and nullability | `STRUCT` with preserved fields |
| Apache Paimon | Paimon `ROW` | `STRUCT` with preserved fields |
| Lance | Not declared by the managed connector | Not declared |

## Example

```sql
CREATE TABLE orders (
  order_id BIGINT,
  customer STRUCT(name VARCHAR, tier VARCHAR)
);

INSERT INTO orders
VALUES (1, STRUCT(name => 'Alice', tier => 'gold'));

SELECT order_id, customer.name AS customer_name
FROM orders
WHERE customer.tier = 'gold';
```

## Related

- [ARRAY](array.md) and [MAP](map.md)
- [Struct functions](../../sql-reference/functions/struct.md)
- [Type casting](../type-casting.md)
