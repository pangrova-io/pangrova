---
title: "Vector Search"
description: "ANN similarity search: distance functions, HNSW/IVFPQ indexes, hybrid search, RERANK BY"
---

# Vector Search

Pangrova has Native vector distance, index, and retrieval implementations. SQL
recognition, exact distance ordering, ANN binding, full-text retrieval, and model
reranking are distinct execution paths.

## Current Execution Boundaries

- ANN rewriting requires an eligible ascending distance order, positive limit,
  literal query vector, and matching index. An unbound query can execute ordinary
  exact SQL; a bound ANN task must obey its explicit fallback policy.
- A `WHERE` clause does not prove that predicates reach the ANN backend as prefilters.
  The strategy-selection helper is not wired as a general default adaptive planner.
- Hybrid rewriting covers bounded positive `MATCH` plus vector-distance patterns.
  Arbitrary OR/NOT, GROUP BY/HAVING, or retrieval combinations are not blanket support.
- Multi-vector source expressions require per-source bindings; the current collector
  rejects the ambiguous multi-vector form shown below.
- Text-to-vector shorthand is not wired by default. Explicit model invocation,
  artifacts, and provider dependencies must be verified for the query used.

These examples are query shapes and syntax references, not measured recall/latency
or a qualification of every combination. See
[Current Capability Status](../../getting-started/capability-status.md).

---

## Distance Functions

The distance-order functions below use **lower values for greater similarity**.
`COSINE_SIMILARITY` is a separate similarity score whose larger values indicate
greater similarity; it is not the ascending distance key for ANN rewriting.

### L2_DISTANCE

**Synopsis:**

```sql
L2_DISTANCE(vector1 VECTOR, vector2 VECTOR) → DOUBLE
```

**Description:** Euclidean (L2) distance: `sqrt(sum((a[i] - b[i])^2))`. Range is `[0, +∞)`. Zero means identical vectors.

**Example:**

```sql
SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist
FROM products
ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC
LIMIT 10;
```

---

### COSINE_DISTANCE

**Synopsis:**

```sql
COSINE_DISTANCE(vector1 VECTOR, vector2 VECTOR) → DOUBLE
```

**Description:** Cosine distance: `1 - dot(a, b) / (||a|| * ||b||)`. Range is `[0, 2]` for non-zero vectors. Returns `1.0` when either vector is zero (undefined direction).

**Example:**

```sql
SELECT id, cosine_distance(embedding, ARRAY[0.707, 0.707, 0.0, 0.0]) AS dist
FROM documents
ORDER BY cosine_distance(embedding, ARRAY[0.707, 0.707, 0.0, 0.0]) ASC
LIMIT 5;
```

---

### INNER_PRODUCT

**Synopsis:**

```sql
INNER_PRODUCT(vector1 VECTOR, vector2 VECTOR) → DOUBLE
```

**Description:** Negative inner (dot) product: `-dot(a, b)`. Lower values indicate more similar vectors. Best used with normalized vectors; for unnormalized vectors, prefer L2 or COSINE.

**Example:**

```sql
SELECT id, inner_product(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS score
FROM products
ORDER BY inner_product(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC
LIMIT 10;
```

---

### COSINE_SIMILARITY

**Synopsis:**

```sql
COSINE_SIMILARITY(vector1 VECTOR, vector2 VECTOR) → DOUBLE
```

**Description:** Cosine similarity: `dot(a, b) / (||a|| * ||b||)`. Range is `[-1, 1]`. Higher values indicate greater similarity. Use for projection/display (not ORDER BY for KNN — use COSINE_DISTANCE instead).

**Example:**

```sql
-- Use in SELECT for display (higher = more similar)
SELECT id, cosine_similarity(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS similarity
FROM products
ORDER BY cosine_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC
LIMIT 10;
```

---

## Vector Index Management

Vector indexes accelerate KNN queries by enabling Approximate Nearest Neighbor (ANN) search instead of brute-force scanning.

### CREATE INDEX USING VECTOR

**Synopsis:**

```sql
CREATE INDEX [IF NOT EXISTS] index_name ON table_name(vector_column)
    USING VECTOR
    WITH ('property' = 'value' [, ...])
    [FORCE];
```

**Properties:**

| Property | Values | Default | Description |
|----------|--------|---------|-------------|
| `algorithm` | `'HNSW'`, `'IVFPQ'`, `'DISK_ANN'` | `'HNSW'` | Index algorithm |
| `metric` | `'L2'`, `'COSINE'`, `'INNER_PRODUCT'` | `'L2'` | IVFPQ supports all three; DISK_ANN requires `L2` |
| `max_connections` | Integer | 16 | HNSW: max edges per node |
| `ef_construction` | Integer | 200 | HNSW: construction depth |
| `ivfpq.nlist` | Integer | 64 | IVF coarse clusters |
| `ivfpq.m` | Integer | greatest divisor of dimension <= 32 | PQ sub-vectors |
| `ivfpq.nbits` | Integer | 8 | IVFPQ: bits per sub-vector quantizer |

**Examples:**

```sql
-- Create HNSW index with L2 metric
CREATE INDEX idx_embedding ON articles(embedding)
    USING VECTOR
    WITH ('algorithm'='HNSW', 'metric'='L2');

-- Create HNSW with cosine metric and custom parameters
CREATE INDEX idx_cos ON products(embedding)
    USING VECTOR
    WITH ('algorithm'='HNSW', 'metric'='COSINE', 'max_connections'='32', 'ef_construction'='400');

-- Create IVFPQ index for large datasets
CREATE INDEX idx_pq ON documents(embedding)
    USING VECTOR
    WITH ('algorithm'='IVFPQ', 'metric'='L2', 'ivfpq.nlist'='256', 'ivfpq.m'='8', 'ivfpq.nbits'='8');
```

---

### DROP INDEX

**Synopsis:**

```sql
DROP INDEX [IF EXISTS] index_name ON table_name;
```

**Example:**

```sql
DROP INDEX idx_embedding ON articles;
```

---

### ALTER INDEX REBUILD

**Synopsis:**

```sql
ALTER INDEX index_name ON table_name REBUILD;
```

**Description:** Triggers a full rebuild of the vector index. Use after large batch inserts or when recall degrades.

**Example:**

```sql
ALTER INDEX idx_embedding ON articles REBUILD;
```

---

### SHOW INDEXES

**Synopsis:**

```sql
SHOW INDEXES FROM table_name;
```

**Description:** Lists all indexes on the specified table including vector indexes.

**Example:**

```sql
SHOW INDEXES FROM articles;
-- Returns: index_name, index_type, columns, state, algorithm, metric
```

---

## Query Patterns

### Basic KNN Search

The fundamental pattern: ORDER BY a distance function with LIMIT K.

```sql
-- Find 10 most similar articles
SELECT id, title, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist
FROM articles
ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC
LIMIT 10;
```

An index alone does not guarantee ANN routing. The query must match the eligible
rewrite shape and resolve a compatible index artifact. Without an ANN binding,
ordinary exact distance ordering can remain in the Native relational plan. A failure
in an already bound ANN task is governed by its explicit policy, not silent exact fallback.

---

### Filtered Vector Search

Combine relational predicates with distance ordering. This expresses a filtered SQL
result; it does not establish that filtering happens inside HNSW traversal.

```sql
-- Find similar electronics products
SELECT id, name, l2_distance(embedding, ARRAY[0.5, 0.5, 0.0, 0.0]) AS dist
FROM products
WHERE category = 'electronics' AND price > 10.0
ORDER BY l2_distance(embedding, ARRAY[0.5, 0.5, 0.0, 0.0]) ASC
LIMIT 10;
```

**Planner limitation:** the source contains strategy-selection components, but the
default production path does not establish automatic selectivity-based routing among
`ANN_THEN_FILTER`, `FILTERED_ANN`, and `FILTER_THEN_BRUTEFORCE`. Inspect the actual
plan and validate correctness/recall under filters. No fixed selectivity threshold
or optimal strategy is claimed by this reference.

---

### Hybrid Search (Vector + Full-Text)

Combine vector similarity with full-text relevance scoring using `MATCH()`.

**Implicit hybrid search** (auto-detected when both vector ORDER BY and MATCH are present):

```sql
-- Automatically applies RRF(k=60) fusion
SELECT id, title FROM documents
WHERE MATCH(content, 'machine learning')
ORDER BY l2_distance(embedding, ARRAY[0.5, 0.3, 0.2, 0.0]) ASC
LIMIT 10;
```

**Explicit hybrid search** with RERANK BY:

```sql
SELECT id, title FROM documents
WHERE MATCH(content, 'machine learning')
ORDER BY l2_distance(embedding, ARRAY[0.5, 0.3, 0.2, 0.0]) ASC
LIMIT 10
RERANK BY rrf(60);
```

---

### RERANK BY Clause

**Synopsis:**

```sql
SELECT ... ORDER BY distance_func(...) ASC LIMIT k
RERANK BY rerank_strategy [(ON (col1, col2, ...))];
```

The RERANK BY clause specifies how to fuse scores from multiple retrieval sources.

#### RRF (Reciprocal Rank Fusion)

```sql
RERANK BY rrf(k)
```

Fuses candidates using the formula: `score(d) = sum(1 / (k + rank_i(d)))` where `rank_i` is the 1-based rank from source `i`. Default k=60.

```sql
SELECT id FROM documents
ORDER BY l2_distance(embedding, query_vec) ASC
LIMIT 20
RERANK BY rrf(60);
```

#### LINEAR_COMBINATION

```sql
RERANK BY linear_combination(weight1, weight2)
```

Weighted linear combination of normalized scores from two sources.

```sql
SELECT id FROM documents
WHERE MATCH(content, 'search query')
ORDER BY cosine_distance(embedding, query_vec) ASC
LIMIT 20
RERANK BY linear_combination(0.7, 0.3);
```

#### CROSS_ENCODER

```sql
RERANK BY cross_encoder('model_name', top_n)
```

Re-scores top candidates using a cross-encoder model that jointly evaluates query-document pairs.

```sql
SELECT id FROM documents
ORDER BY l2_distance(embedding, query_vec) ASC
LIMIT 100
RERANK BY cross_encoder('ms-marco-MiniLM', 20);
```

#### CASCADE

```sql
RERANK BY cascade(strategy1, strategy2 [, ...])
```

Applies multiple reranking strategies sequentially. Each stage refines the results of the previous stage.

```sql
SELECT id FROM documents
ORDER BY l2_distance(embedding, query_vec) ASC
LIMIT 100
RERANK BY cascade(rrf(60), cross_encoder('reranker-v2', 10));
```

#### Multi-Vector ON Clause

**Status: recognized syntax with missing per-source execution bindings.**
The current collector rejects this multi-vector form; it is not a runnable example:

```sql
SELECT id FROM documents
ORDER BY l2_distance(text_embedding, query_vec) ASC
LIMIT 20
RERANK BY rrf(60) ON (text_embedding, image_embedding);
```

**Notes:**
- RERANK BY is incompatible with GROUP BY/HAVING
- Requires at least one retrieval source (vector ORDER BY or MATCH in WHERE)
- Implicit hybrid search defaults to RRF(k=60) when both vector and MATCH are present

---

## Auto-Embedding Pipeline

Generated-column and embedding components exist, but model admission, materialization,
publication, and asynchronous refresh/recovery need validation for the selected path.
The following illustrates the intended lifecycle; it is not proof that SYNC, ASYNC,
or BACKFILL modes are all complete. See
[AI Functions — Auto-Embedding](ai-functions.md#auto-embedding-generated-columns).

```sql
-- Create table with auto-embedding
CREATE TABLE articles (
    id BIGINT,
    title VARCHAR,
    content VARCHAR,
    embedding VECTOR(1024) GENERATED ALWAYS AS (
        ai_embed('bge_m3', title, content)
    ) EMBEDDING MODE SYNC
);

-- Create vector index
CREATE INDEX idx_articles_vec ON articles(embedding)
    USING VECTOR WITH ('algorithm'='HNSW', 'metric'='COSINE');

-- Insert triggers automatic embedding
INSERT INTO articles (id, title, content)
VALUES (1, 'Introduction to ML', 'Machine learning is a subset of AI...');

-- Query shape after successful materialization and index publication
SELECT id, title, cosine_distance(embedding, ARRAY[...]) AS dist
FROM articles
ORDER BY cosine_distance(embedding, ARRAY[...]) ASC
LIMIT 5;
```

---

## Evaluation Guidance

Use a fixed model version and dimension matching the stored vectors. Validate actual
Native capability inventory, index artifact generation, source snapshot, and query
plan before measuring performance. Compare exact results with ANN results on the
same authorized dataset, including filtered and updated data.

Measure recall, latency, memory, build time, provider calls, and refresh lag. HNSW
and IVFPQ parameters require workload measurements; no universal size/selectivity
threshold, model dimension recommendation, or speed advantage is established here.
Incremental index updates and generated-embedding modes also require their own
publication and recovery evidence.

---

## Cross-References

| Topic | Guide |
|-------|-------|
| AI Functions & Embedding Models | [AI Functions](ai-functions.md) |
| VECTOR data type | [VECTOR data type](../../data-types/ai-native/vector.md) |
| SQL DDL (CREATE TABLE, INDEX) | [DDL](../ddl.md) |
| Vector distance function details | [Functions — Vector](vector.md) |
