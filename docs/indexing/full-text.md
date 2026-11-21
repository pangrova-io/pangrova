---
title: "Full-Text"
description: "INVERTED index for token-based full-text search with BM25 scoring"
---

# Full-Text (INVERTED) Index

An inverted index maps tokens to the rows containing them. `MATCH(column, 'query')`
is a boolean predicate; `BM25_SCORE(column, 'query')` returns the DOUBLE relevance
score.

## When to Use

- Keyword search over text columns (descriptions, messages, documents)
- Relevance-ranked retrieval, including hybrid retrieval with
  [Vector Search](../sql-reference/functions/vector-search.md)
- Text compatible with the bundled `standard-v1` analyzer; a dedicated CJK analyzer is not available

## Syntax

```sql
CREATE INDEX idx_products_desc
ON prod.analytics.products (description)
USING INVERTED
WITH ('analyzer' = 'standard-v1');
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `analyzer` | `standard-v1` | Only `standard-v1` is accepted by the current build |

`inverted.analyzer` and `inverted.store_positions` are not wired; use the bare
`analyzer` property. Position storage is not implemented.

## Query Behavior — MATCH()

`MATCH(column, 'query')` returns BOOLEAN and acts as a filter. Use
`BM25_SCORE(column, 'query')` when a DOUBLE score is needed; in `SELECT` it
exposes the relevance score for ordering.

```sql
-- Keyword filter
SELECT product_id, name
FROM prod.analytics.products
WHERE MATCH(description, 'wireless noise cancelling');

-- Relevance ranking
SELECT product_id, name, BM25_SCORE(description, 'wireless headphones') AS score
FROM prod.analytics.products
WHERE MATCH(description, 'wireless headphones')
ORDER BY score DESC
LIMIT 10;
```

## Notes

- The query text is tokenized with the same analyzer configured on the index.
- `MATCH()` is recognized during planning and rewritten into a full-text scoring
  node; multi-term queries merge posting lists before scoring.

**See Also:** [Index DDL](index-ddl.md), [Vector Search](../sql-reference/functions/vector-search.md) for hybrid full-text + vector retrieval
