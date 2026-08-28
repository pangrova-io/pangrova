---
title: "Vector Search"
description: "ANN similarity search: distance functions, HNSW/IVFPQ indexes, hybrid search, RERANK BY"
---

# Vector Search

Pangrova provides native vector similarity search with HNSW and IVFPQ indexing algorithms, SIMD-optimized distance computation (AVX-512, AVX2, NEON), and distributed scatter-gather execution. Vector search integrates seamlessly with SQL queries, enabling KNN search, filtered search, and hybrid search (vector + full-text) with reranking.

---

## Distance Functions

All distance functions follow the convention that **lower values indicate greater similarity**, so results can be uniformly sorted in ascending order regardless of the chosen metric.

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
| `metric` | `'L2'`, `'COSINE'`, `'INNER_PRODUCT'` | `'L2'` | Distance metric (IVFPQ supports `L2` only) |
| `hnsw.M` | Integer | 16 | HNSW: max edges per node |
| `hnsw.efConstruction` | Integer | 200 | HNSW: construction search depth |
| `hnsw.efSearch` | Integer | 64 | HNSW: query-time search depth |
| `ivfpq.nlist` | Integer | 256 | IVFPQ: number of Voronoi cells |
| `ivfpq.m` | Integer | 8 | IVFPQ: number of sub-vectors |
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
    WITH ('algorithm'='HNSW', 'metric'='COSINE', 'hnsw.M'='32', 'hnsw.efConstruction'='400');

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

When a VECTOR index exists on the column, the planner automatically routes to ANN search. Without an index, brute-force scan is used as fallback.

---

### Filtered Vector Search

Combine WHERE clauses with vector ORDER BY for pre-filtered similarity search.

```sql
-- Find similar electronics products
SELECT id, name, l2_distance(embedding, ARRAY[0.5, 0.5, 0.0, 0.0]) AS dist
FROM products
WHERE category = 'electronics' AND price > 10.0
ORDER BY l2_distance(embedding, ARRAY[0.5, 0.5, 0.0, 0.0]) ASC
LIMIT 10;
```

**Execution Strategy Selection:**

The planner automatically selects the optimal execution strategy based on estimated filter selectivity:

| Selectivity | Strategy | Description |
|-------------|----------|-------------|
| High (> 50%) | `ANN_THEN_FILTER` | Run ANN with over-fetched K, then post-filter |
| Moderate (20-50%) | `FILTERED_ANN` | Evaluate filter inline during HNSW graph traversal |
| Low (< 20%) | `FILTER_THEN_BRUTEFORCE` | Apply filter first, brute-force scan survivors |

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

Target specific vector columns for reranking:

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

Automate vector generation using generated columns. See [AI Functions — Auto-Embedding](ai-functions.md#auto-embedding-generated-columns) for full details.

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

-- Search immediately works
SELECT id, title, cosine_distance(embedding, ARRAY[...]) AS dist
FROM articles
ORDER BY cosine_distance(embedding, ARRAY[...]) ASC
LIMIT 5;
```

---

## Best Practices

### Dimension Selection

| Use Case | Recommended Dimension | Model Examples |
|----------|----------------------|----------------|
| General text search | 768–1024 | bge-m3, text-embedding-3-small |
| High-precision retrieval | 1536–3072 | text-embedding-3-large |
| Image embeddings | 512–768 | CLIP ViT |
| Lightweight/mobile | 256–384 | all-MiniLM-L6 |

### Index Algorithm Selection

| Scenario | Algorithm | Why |
|----------|-----------|-----|
| < 1M vectors, high recall | HNSW | Best recall/latency tradeoff |
| > 10M vectors, memory-constrained | IVFPQ | Compressed storage, lower memory |
| Rapidly changing data | HNSW | Supports incremental inserts |
| Static datasets | IVFPQ | Optimized for batch-loaded data |

### Performance Tuning

1. **Index build**: Increase `efConstruction` for better recall at the cost of build time
2. **Query latency**: Tune `efSearch` (query-time parameter) to balance recall vs speed
3. **Batch inserts**: Perform bulk inserts before creating the index, or use `ALTER INDEX REBUILD` after
4. **Filtered search**: For highly selective filters (< 5% rows), pre-filter then brute-force is faster
5. **Hybrid search**: Use RRF for simple fusion; use cross-encoder for highest quality reranking

### SYNC vs ASYNC Embedding

| Scenario | Mode | Rationale |
|----------|------|-----------|
| Real-time search required | SYNC | Vectors available immediately |
| High-throughput ingestion | ASYNC | Non-blocking inserts |
| Batch ETL pipelines | ASYNC + BACKFILL | Separate compute from load |
| Interactive applications | SYNC | Consistent user experience |

---

## Cross-References

| Topic | Guide |
|-------|-------|
| AI Functions & Embedding Models | [AI Functions](ai-functions.md) |
| VECTOR data type | [VECTOR data type](../../data-types/ai-native/vector.md) |
| SQL DDL (CREATE TABLE, INDEX) | [DDL](../ddl.md) |
| Vector distance function details | [Functions — Vector](vector.md) |
