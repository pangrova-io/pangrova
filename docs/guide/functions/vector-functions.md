Pangrova > Guide > Functions > Vector Functions

# Vector Functions

SIMD-optimized distance functions for vector similarity search. All functions follow the convention that **lower values indicate greater similarity**.

---

## L2_DISTANCE

```sql
L2_DISTANCE(vector1, vector2) → DOUBLE
```

**Description:** Euclidean (L2) distance: \(\sqrt{\sum_{i}(a_i - b_i)^2}\)

**Range:** `[0, +∞)`. Zero means identical vectors.

**Properties:** Non-negativity, identity (`L2_DISTANCE(a, a) = 0`), symmetry.

```sql
SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist
FROM articles
ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC
LIMIT 10;
```

---

## COSINE_DISTANCE

```sql
COSINE_DISTANCE(vector1, vector2) → DOUBLE
```

**Description:** Cosine distance: `1 - dot(a, b) / (||a|| * ||b||)`

**Range:** `[0, 2]` for non-zero vectors. Returns `1.0` for zero vectors.

**Best for:** Comparing direction regardless of magnitude (text embeddings).

```sql
SELECT id, cosine_distance(embedding, ARRAY[0.5, 0.5, 0.0, 0.0]) AS dist
FROM documents
ORDER BY cosine_distance(embedding, ARRAY[0.5, 0.5, 0.0, 0.0]) ASC
LIMIT 5;
```

---

## INNER_PRODUCT

```sql
INNER_PRODUCT(vector1, vector2) → DOUBLE
```

**Description:** Negative dot product: `-dot(a, b)`

**Range:** `(-∞, +∞)`. Lower values = more similar.

**Best for:** Normalized vectors where dot product represents similarity.

```sql
SELECT id, inner_product(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS score
FROM products
ORDER BY inner_product(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC
LIMIT 10;
```

---

## COSINE_SIMILARITY

```sql
COSINE_SIMILARITY(vector1, vector2) → DOUBLE
```

**Description:** Cosine similarity: `dot(a, b) / (||a|| * ||b||)`

**Range:** `[-1, 1]`. Higher values = more similar.

**Use case:** Display/projection (use COSINE_DISTANCE for ORDER BY in KNN).

```sql
-- Use for display alongside COSINE_DISTANCE for ordering
SELECT id, cosine_similarity(embedding, query_vec) AS similarity
FROM products
ORDER BY cosine_distance(embedding, query_vec) ASC
LIMIT 10;
```

---

## Implementation Details

- **SIMD optimization:** AVX-512 (16 floats/iter), AVX2 (8 floats/iter), ARM NEON, scalar fallback
- **Automatic dispatch:** Compiled for the best available instruction set
- **Index integration:** When a VECTOR index exists with matching metric, the planner routes to ANN search
- **Thread safety:** All functions are stateless and thread-safe

---

## Metric Selection Guide

| Metric | SQL Function | Index Metric | Use Case |
|--------|-------------|--------------|----------|
| Euclidean | `L2_DISTANCE` | `'L2'` | General purpose, unnormalized vectors |
| Cosine | `COSINE_DISTANCE` | `'COSINE'` | Text embeddings, direction-based similarity |
| Dot product | `INNER_PRODUCT` | `'INNER_PRODUCT'` | Pre-normalized vectors, MaxIP search |

**See Also:** [Vector Search](../vector-search.md), [Data Types — VECTOR](../data-types.md#vectordim--element_type)
