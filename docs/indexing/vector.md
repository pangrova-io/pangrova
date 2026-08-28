---
title: "Vector"
description: "VECTOR index for approximate nearest neighbor (ANN) search — HNSW and IVFPQ"
---

# Vector Index

A vector index enables approximate nearest neighbor (ANN) search over
[VECTOR](../data-types/ai-native/vector.md) columns. Pangrova implements three
algorithms — **HNSW** (graph-based, high recall, default), **IVFPQ** (quantized,
memory-efficient), and **DISK_ANN** (Vamana graph, SSD-resident) — selected at
index creation time.

## When to Use

- Similarity search over embeddings (`ORDER BY distance(...) LIMIT k`)
- Semantic retrieval for RAG pipelines, optionally hybrid with
  [full-text search](full-text.md)
- High-dimensional columns where exact brute-force search is too slow

## Syntax

```sql
-- HNSW index (default algorithm, default metric L2)
CREATE INDEX idx_embedding ON embeddings(embedding)
USING VECTOR
WITH ('algorithm' = 'HNSW', 'metric' = 'L2', 'hnsw.M' = '16', 'hnsw.efConstruction' = '200');

-- IVFPQ index (L2 metric only)
CREATE INDEX idx_embedding_pq ON embeddings(embedding)
USING VECTOR
WITH ('algorithm' = 'IVFPQ', 'metric' = 'L2', 'ivfpq.nlist' = '256', 'ivfpq.m' = '8', 'ivfpq.nbits' = '8');

-- Drop / inspect
DROP INDEX idx_embedding ON embeddings;
SHOW INDEXES ON embeddings;
```

Index options are string key/value pairs. Common build-time options: `algorithm`
(`HNSW` default, `IVFPQ`, `DISK_ANN`), `metric` (`L2` default, `COSINE`,
`INNER_PRODUCT`; IVFPQ supports `L2` only), plus the algorithm-specific parameters
below.

## HNSW Parameters

| Parameter | Description | Default | Range |
|-----------|-------------|---------|-------|
| `hnsw.M` | Max connections per node per layer | 16 | [4, 64] |
| `hnsw.efConstruction` | Candidate list size during build | 200 | [M, 4096] |
| `hnsw.efSearch` | Candidate list size during search (query-time) | 64 | [topK, 4096] |

Trade-offs: higher `M` → better recall, more memory, slower build; higher
`efConstruction` → better graph quality, slower build; higher `efSearch` → better
recall, slower search.

## IVFPQ Parameters

| Parameter | Description | Default | Range |
|-----------|-------------|---------|-------|
| `ivfpq.nlist` | Number of IVF clusters (Voronoi cells) | 256 | [1, 65536] |
| `ivfpq.nprobe` | Clusters searched per query (query-time) | 16 | [1, nlist] |
| `ivfpq.m` | Number of PQ sub-quantizers | 8 | dimension must be divisible by m |
| `ivfpq.nbits` | Bits per sub-quantizer code | 8 | 4, 8, or 16 |

## HNSW vs IVFPQ

| Criterion | HNSW | IVFPQ |
|-----------|------|-------|
| Recall@10 | 95–99% | 85–95% |
| Memory per 100K vectors (dim=768) | ~12MB (graph-only) | ~2.3MB |
| Supported metrics | L2, COSINE, INNER_PRODUCT | L2 only |
| Incremental insert | Supported | Supported (encodes into the existing codebook) |
| Best for | High recall, < 10M vectors | Memory-constrained, > 10M vectors |

**Recommendation:** default to HNSW. Use IVFPQ when memory is constrained or the
vector count exceeds 10M per file. DISK_ANN targets SSD-resident graphs but does
not support incremental updates — changes require a full rebuild.

## ANN Query Pattern

The planner recognizes `ORDER BY <distance> LIMIT k` over a `VECTOR` column as an
ANN search and routes it through the vector index:

```sql
SELECT id, content, l2_distance(embedding, ARRAY[0.1, 0.2, ...]) AS distance
FROM embeddings
ORDER BY l2_distance(embedding, ARRAY[0.1, 0.2, ...])
LIMIT 10;

-- Pre-filter + ANN hybrid
SELECT id, content
FROM embeddings
WHERE category = 'tech' AND created_at > '2024-01-01'
ORDER BY cosine_distance(embedding, ARRAY[0.1, 0.2, ...])
LIMIT 10;

-- Explicit index hint (optional)
SELECT /*+ VECTOR_INDEX(embeddings, idx_embedding) */ id, content
FROM embeddings
ORDER BY l2_distance(embedding, ARRAY[0.1, 0.2, ...])
LIMIT 10;
```

**See Also:** [Vector Search](../sql-reference/functions/vector-search.md) for the full query-side guide, [Vector Functions](../sql-reference/functions/vector.md), [Indexing Architecture](../architecture/indexing.md) for the design
