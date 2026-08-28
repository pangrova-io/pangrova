---
title: "BLOOM_FILTER"
description: "Probabilistic membership index for equality predicates"
---

# BLOOM_FILTER Index

A bloom filter index answers "can this data block contain value X?" with no false
negatives and a bounded false-positive rate. Query planning uses it to skip data
blocks that provably cannot match an equality predicate.

## When to Use

- High-cardinality columns filtered with `=` or `IN (...)`
- Point lookups where a full lookup index would be too large
- Accepting a small false-positive probability (candidates are verified on read)

## Syntax

```sql
CREATE INDEX idx_events_user
ON prod.analytics.events (user_id)
USING BLOOM_FILTER
WITH (
  'bloom.fpp' = '0.01',
  'bloom.expected_entries' = '1000000'
);
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `bloom.expected_entries` | `10000` | Expected distinct entries per index block; drives filter sizing |
| `bloom.fpp` | `0.01` | Target false-positive probability (0 < fpp < 1) |

## Query Behavior

```sql
-- Skips blocks whose bloom filter cannot contain the value
SELECT * FROM prod.analytics.events WHERE user_id = 'u-123';

-- IN-lists are evaluated per value
SELECT * FROM prod.analytics.events WHERE user_id IN ('u-1', 'u-2', 'u-3');
```

A positive bloom test only narrows the scan; matching rows are still verified against
actual column values.

**See Also:** [Index DDL](../index-ddl.md), [NGRAM_BLOOM_FILTER](ngram-bloom-filter.md) for substring matching
