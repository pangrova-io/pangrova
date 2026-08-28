---
title: "NGRAM_BLOOM_FILTER"
description: "N-gram bloom filter index for substring (LIKE) pattern matching"
---

# NGRAM_BLOOM_FILTER Index

An n-gram bloom filter index indexes fixed-length character n-grams of each value in
a bloom filter. It enables data skipping for substring predicates such as
`LIKE '%pattern%'` that cannot use min/max statistics.

## When to Use

- `LIKE '%...%'` substring search over text columns
- Log message / payload scanning where the pattern position is unknown
- Patterns at least as long as the configured n-gram size

## Syntax

```sql
CREATE INDEX idx_events_payload
ON prod.analytics.events (payload)
USING NGRAM_BLOOM_FILTER
WITH (
  'ngram.size' = '3',
  'ngram.fpp' = '0.01'
);
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `ngram.size` | `3` | N-gram length extracted from each value |
| `ngram.expected_entries` | `50000` | Expected distinct n-grams per index block |
| `ngram.fpp` | `0.01` | Target false-positive probability |

## Query Behavior

```sql
-- Skips blocks that cannot contain the substring
SELECT * FROM prod.analytics.events WHERE payload LIKE '%timeout%';
```

Patterns shorter than `ngram.size` cannot be answered by the index and fall back to
a regular scan.

**See Also:** [Index DDL](../index-ddl.md), [BLOOM_FILTER](bloom-filter.md) for equality predicates
