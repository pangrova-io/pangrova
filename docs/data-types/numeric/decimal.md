---
title: "DECIMAL"
description: "Fixed-point decimal with up to 38 digits"
---

# DECIMAL


| Property | Value |
|----------|-------|
| Storage | Fixed-point decimal; 64-bit unscaled values through 18 digits, 128-bit through 38 digits |
| Parameters | `p` = precision (total digits), `s` = scale (digits after decimal) |
| Bounds | `1 <= p <= 38`, `0 <= s <= p`; unsupported declarations are rejected, never clamped |
| Defaults | `DECIMAL` / `NUMERIC` = `(38, 0)`; `DECIMAL(p)` / `NUMERIC(p)` = `(p, 0)` |
| Alias | `NUMERIC(p, s)` |

```sql
CREATE TABLE transactions (id BIGINT, amount DECIMAL(10, 2), rate NUMERIC(5, 4));
INSERT INTO transactions VALUES (1, 1234.56, 0.0375);
```

The planner preserves precision, scale and nullability in the canonical Substrait
type, including `DECIMAL(38,18)` and `DECIMAL(38,38)`. Explicit casts do not silently
reduce a declared precision to the supported maximum.

For exact numeric operands with decimal types `(p1,s1)` and `(p2,s2)`, arithmetic
uses the Native scalar signatures:

| Operation | Result precision | Result scale |
|-----------|------------------|--------------|
| Addition/subtraction | `min(38, max(p1-s1,p2-s2) + max(s1,s2) + 1)` | `max(s1,s2)` |
| Multiplication | `min(38, p1+p2)` | `s1+s2` |
| Division | `min(38, p1+s2+max(0,s2-s1))` | `max(s1,s2)` |
| Remainder | `min(p1-s1,p2-s2) + max(s1,s2)` | `max(s1,s2)` |

Multiplication is rejected during planning when `s1+s2 > 38`. Division is rejected
when its required unscaled rescale factor `max(s1,s2)-s1+s2 > 38`. A precision
capped at 38 does not guarantee that every runtime value fits: result overflow
must remain an execution error. Integer-only arithmetic retains its own type
rules. These type and protocol guarantees are not a substitute for Native
result-value and overflow regression tests.
