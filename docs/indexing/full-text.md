---
title: "Full-Text"
description: "INVERTED index for token-based full-text search with BM25 scoring"
---

# Full-Text (INVERTED) Index

An inverted index maps tokens to the rows containing them. The `MATCH()` function
evaluates full-text predicates against the index and returns a BM25 relevance score.

## When to Use

- Keyword search over text columns (descriptions, messages, documents)
- Relevance-ranked retrieval, including hybrid retrieval with
  [Vector Search](../sql-reference/functions/vector-search.md)
- CJK text with the `cjk` analyzer

## Syntax

```sql
CREATE INDEX idx_products_desc
ON prod.analytics.products (description)
USING INVERTED
WITH (
  'inverted.analyzer' = 'standard',
  'inverted.store_positions' = 'false'
);
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `inverted.analyzer` | `standard` | Tokenizer: `standard` or `cjk` |
| `inverted.store_positions` | `false` | Store token positions in posting lists |

## Query Behavior — MATCH()

`MATCH(column, 'query')` returns a `DOUBLE` BM25 score (not a boolean). In `WHERE`
it acts as a filter — a positive score means the document matches; in `SELECT` it
exposes the relevance score for ordering.

```sql
-- Keyword filter
SELECT product_id, name
FROM prod.analytics.products
WHERE MATCH(description, 'wireless noise cancelling');

-- Relevance ranking
SELECT product_id, name, MATCH(description, 'wireless headphones') AS score
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
