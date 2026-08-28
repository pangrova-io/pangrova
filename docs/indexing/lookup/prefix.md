---
title: "PREFIX"
description: "Prefix index for prefix-based string matching"
---

# PREFIX Index

A prefix index stores a fixed-length prefix of each string value, accelerating
predicates that constrain the beginning of a string such as `LIKE 'abc%'`.

## When to Use

- `LIKE 'prefix%'` anchored-prefix matching
- Composite keys queried by their leading component
- String columns where full values are too wide to index directly

## Syntax

```sql
CREATE INDEX idx_products_id_prefix
ON prod.analytics.products (product_id)
USING PREFIX
WITH ('prefix.length' = '8');
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `prefix.length` | `8` | Number of leading characters indexed per value |

## Query Behavior

```sql
-- Uses the prefix index
SELECT * FROM prod.analytics.products WHERE product_id LIKE 'SKU-2025%';

-- Leading wildcard cannot use the prefix index
SELECT * FROM prod.analytics.products WHERE product_id LIKE '%2025';
```

For unanchored substring search, use
[NGRAM_BLOOM_FILTER](../data-skipping/ngram-bloom-filter.md).

**See Also:** [Index DDL](../index-ddl.md)
