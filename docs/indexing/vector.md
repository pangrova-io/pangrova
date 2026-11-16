---
title: "Vector Index"
description: "Native durable VECTOR indexes: HNSW, IVF-PQ, and DiskANN"
---

# Vector Index

A vector index enables approximate nearest neighbor (ANN) search over
[VECTOR](../data-types/ai-native/vector.md) columns. Pangrova implements three
algorithms — **HNSW** (graph-based, high recall, default), **IVFPQ** (quantized,
memory-efficient), and **DISK_ANN** (Vamana graph with the full-precision vectors
kept on disk) — selected at index creation time.

Build and retrieval execute exclusively in Native workers. The Coordinator
only resolves canonical capabilities, creates durable build jobs, binds an
immutable artifact generation, and dispatches ordinary tasks. Pangrova has no
Java HNSW, IVF-PQ, DiskANN, or brute-force search engine.

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
WITH ('algorithm' = 'HNSW', 'metric' = 'L2',
      'max_connections' = '16', 'ef_construction' = '200');

-- IVFPQ index (L2, COSINE, or INNER_PRODUCT)
CREATE INDEX idx_embedding_pq ON embeddings(embedding)
USING VECTOR
WITH ('algorithm' = 'IVF_PQ', 'metric' = 'L2',
      'coarse_clusters' = '256', 'pq_subvectors' = '8',
      'pq_bits_per_code' = '8');

-- Drop / inspect
DROP INDEX idx_embedding ON embeddings;
SHOW INDEXES ON embeddings;
```

Index options are string key/value pairs. Common build-time options: `algorithm`
(`HNSW` default, `IVF_PQ`/`IVFPQ`, `DISK_ANN`) and `metric`. HNSW and IVF-PQ
support `L2` (default), `COSINE`, and `INNER_PRODUCT`. DiskANN requires `L2`.
An unsupported metric fails before a build job is persisted. `EUCLIDEAN` is
normalized to `L2`, and the canonical metric is always persisted in metadata.

HNSW and IVF-PQ use version 2 artifact formats and capability contracts. The
metric is encoded in the index bytes; build properties, query options, and
loaded index bytes must agree. Version 1 indexes must be rebuilt; there is no
legacy reader. Native candidate ranking and exact rescoring use the same metric
(Euclidean distance, cosine distance, or negative inner product; lower is better).
IVF-PQ trains residual quantizers with Euclidean reconstruction error and ranks
reconstructed vectors using the configured metric.

The SQL spelling `IVFPQ` is normalized to `IVF_PQ` before metadata is persisted.
The SQL options `ivfpq.nlist`, `ivfpq.m`, and `ivfpq.nbits` map to
`coarse_clusters`, `pq_subvectors`, and `pq_bits_per_code`; conflicting values
for a SQL option and its canonical equivalent are rejected.

An empty source snapshot publishes an explicit zero-object generation through
the same durable publication fence. No Native work pages are required because
the snapshot contains no source objects. Nonempty builds still require every
source page and its complete output bundle before publication.

## HNSW Parameters

| Parameter | Description | Default | Range |
|-----------|-------------|---------|-------|
| `max_connections` | Max connections per node | 16 | [2, 512] |
| `ef_construction` | Candidate list size during build | 200 | [max_connections, 100000] |
| `ef_search` | Candidate list size during search | dynamic | [topK, 100000] |

Trade-offs: higher `M` → better recall, more memory, slower build; higher
`efConstruction` → better graph quality, slower build; higher `efSearch` → better
recall, slower search.

## IVFPQ Parameters

| Parameter | Description | Default | Range |
|-----------|-------------|---------|-------|
| `coarse_clusters` | Number of IVF clusters | 64 | [1, 4096] |
| `nprobes` | Clusters searched per query | min(8, coarse_clusters) | [1, coarse_clusters] |
| `pq_subvectors` | Number of PQ sub-vectors | largest divisor ≤ 32 | must divide dimension |
| `pq_bits_per_code` | Bits per PQ code | 8 | [1, 8] |

## HNSW vs IVFPQ

| Criterion | HNSW | IVFPQ |
|-----------|------|-------|
| Recall@10 | 95–99% | 85–95% |
| Memory per 100K vectors (dim=768) | ~12MB (graph-only) | ~2.3MB |
| Supported durable format metric | L2, COSINE, INNER_PRODUCT | L2, COSINE, INNER_PRODUCT |
| Update model | Immutable generation rebuild | Immutable generation rebuild |
| Best for | High recall, < 10M vectors | Memory-constrained, > 10M vectors |

**Recommendation:** default to HNSW. Use IVF-PQ when memory is constrained.
DiskANN keeps the full-precision vectors in a companion on-disk file and rereads
them for exact rescoring, while the adjacency lists and PQ codes are loaded into
process memory when the artifact opens; a fully disk-resident layout is a planned
artifact-format change, not current behaviour. All three publish immutable,
snapshot-pinned generations; refreshes never mutate a serving artifact in place.

## ANN Query Pattern

The planner recognizes `ORDER BY <distance> LIMIT k` over a `VECTOR` column as an
ANN search and routes it through the vector index:

```sql
SELECT id, content, l2_distance(embedding, ARRAY[0.1, 0.2, ...]) AS distance
FROM embeddings
ORDER BY l2_distance(embedding, ARRAY[0.1, 0.2, ...])
LIMIT 10;

-- Pre-filtered DiskANN (L2)
SELECT id, content
FROM embeddings
WHERE category = 'tech' AND created_at > '2024-01-01'
ORDER BY l2_distance(embedding, ARRAY[0.1, 0.2, ...])
LIMIT 10;

```

The planner stores the selected Native retrieval `CapabilityRef` in the plan.
The Coordinator binds a compatible `ArtifactFormatRef`, complete source-object
coverage, checksums, row-locator contract, and generation. Workers that do not
advertise the exact capability and format are ineligible. Missing or incomplete
artifacts cannot silently return an empty or partial Top-K; behavior follows the
explicit execution policy.

Filtered DiskANN requires a complete published source-object generation for
the task's scan. If file pruning leaves only part of that generation, the
query uses the exact Native scan. A supported full-source predicate executes
the filtered Native ANN operator; predicates outside its typed plan shape also
use the exact scan. `search_list_size` bounds graph exploration per source
object. A selective filter can need more exploration to fill `LIMIT k`; when
the bound is insufficient, the task reports an error rather than returning a
short or silently substituted result. Size this bound for the workload and
verify Recall and latency on the intended data distribution.

**See Also:** [Vector Search](../sql-reference/functions/vector-search.md) for the full query-side guide, [Vector Functions](../sql-reference/functions/vector.md), [Indexing Architecture](../architecture/indexing.md) for the design
