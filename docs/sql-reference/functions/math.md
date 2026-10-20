---
title: "Math"
description: "Arithmetic operators and numeric functions"
---

# Math Functions

---

## Arithmetic Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `+` | Addition | `price + tax` |
| `-` | Subtraction | `balance - withdrawal` |
| `*` | Multiplication | `quantity * unit_price` |
| `/` | Division | `total / count` |
| `%` | Modulo | `id % 10` |
| `-expr` | Unary negation | `-amount` |

Mixed primitive numeric operands use their common numeric type through explicit
casts in the execution plan. A `BIGINT` plus an `INTEGER` is evaluated as
`BIGINT + BIGINT`, without narrowing the larger operand. NULL operands remain
nullable. Decimal precision/scale follows its separate decimal type rules.

---

## ABS

```sql
ABS(numeric_expr) → same type
```

Returns the absolute value.

```sql
SELECT ABS(-42) AS result;  -- 42
SELECT ABS(price_change) AS magnitude FROM stocks;
```

---

## MOD

```sql
MOD(dividend, divisor) → divisor type for primitive integers
-- or: dividend % divisor
```

```sql
SELECT MOD(17, 5) AS result;  -- 2
SELECT id, MOD(id, 3) AS bucket FROM events;
```

For mixed integer widths, computation uses the common argument type and the
result is explicitly converted to the validated divisor type.

---

## POWER

```sql
POWER(base, exponent) → DOUBLE
```

```sql
SELECT POWER(2, 10) AS result;  -- 1024.0
```

---

## SQRT

```sql
SQRT(numeric_expr) → DOUBLE
```

```sql
SELECT SQRT(144) AS result;  -- 12.0
```

---

## ROUND / FLOOR / CEIL

```sql
ROUND(numeric_expr [, scale]) → same type
FLOOR(numeric_expr) → same type
CEIL(numeric_expr) → same type
```

For `DECIMAL(p,s)`, the canonical `ROUND` return type follows the registered
Native signature: without `scale`, `DECIMAL(min(38,p-s+min(s,1)),0)`; with
`scale`, `DECIMAL(min(38,p+1),s)`. The optional position is explicitly cast to
`INTEGER` in the canonical execution plan. The Planner admits `ROUND` because
the Native registered-function inventory includes its decimal signatures;
runtime binding still validates the actual overload and types. `FLOOR` and
`CEIL` require separate capability qualification.

```sql
SELECT ROUND(3.14159, 2) AS rounded;  -- 3.14
SELECT FLOOR(3.7) AS floored;          -- 3
SELECT CEIL(3.2) AS ceiled;            -- 4
```

---

## Notes

- Division by zero returns NULL (not an error)
- Integer division truncates: `7 / 2 = 3`
- For decimal division, cast to DOUBLE: `CAST(7 AS DOUBLE) / 2 = 3.5`

**See Also:** [Aggregate Functions](aggregate.md), [Type Casting](../../data-types/type-casting.md)


## TO_IEEE754_32 / TO_IEEE754_64

```sql
TO_IEEE754_32(float_expr) → VARBINARY(4)
TO_IEEE754_64(double_expr) → VARBINARY(8)
```

Returns a big-endian IEEE 754 encoding. Positive and negative zero remain
distinct. All NaN payloads of the same width produce one canonical NaN encoding;
infinities and finite values retain their representation. NULL input returns
NULL. Use the function matching the input's floating-point width, with an
explicit cast when conversion is intended.
