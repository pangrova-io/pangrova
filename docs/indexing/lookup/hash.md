---
title: "HASH"
description: "Open-addressing hash index for fast equality lookups"
---

# HASH Index

A hash index maps values to row positions through an open-addressing hash table,
giving constant-time equality lookups. It does not support range predicates or
ordered iteration.

## When to Use

- Equality-only workloads (`=`, `IN (...)`) on high-cardinality columns
- Point lookups where the lowest lookup latency matters
- Columns never used in range predicates or sorting

## Syntax

```sql
CREATE INDEX idx_users_email
ON prod.analytics.users (email)
USING HASH;
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `hash.load_factor` | `0.7` | Hash table load factor target |

## Query Behavior

```sql
SELECT * FROM prod.analytics.users WHERE email = 'alice@example.com';
```

For columns that also need range queries, use [BTREE](btree.md) instead.

**See Also:** [Index DDL](../index-ddl.md), [BLOOM_FILTER](../data-skipping/bloom-filter.md) for cheaper membership-based skipping
