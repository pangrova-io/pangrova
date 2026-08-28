---
title: "DECIMAL"
description: "Arbitrary-precision fixed-point decimal"
---

# DECIMAL


| Property | Value |
|----------|-------|
| Storage | Arbitrary-precision decimal |
| Parameters | `p` = precision (total digits), `s` = scale (digits after decimal) |
| Alias | `NUMERIC(p, s)` |

```sql
CREATE TABLE transactions (id BIGINT, amount DECIMAL(10, 2), rate NUMERIC(5, 4));
INSERT INTO transactions VALUES (1, 1234.56, 0.0375);
```
