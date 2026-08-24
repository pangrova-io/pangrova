---
title: Indexing Architecture
sidebar_label: Indexing
---

# Pangrova — Indexing Architecture

> Author: Pangrova Team | Date: 2026-03-22
> Merged from: `21-vector-index-design.md` + `26-snowflake-inspired-indexing-redesign.md`

## 1. Overview

Vector Index provides Approximate Nearest Neighbor (ANN) search capability for Pangrova, enabling Top-K similarity retrieval on high-dimensional vector columns. Unlike the existing 10 traditional index types that use SKIP/KEEP binary filtering semantics, Vector Index produces scored, ordered row sets — a fundamentally different query paradigm.

**Core Design Principles**:
- Vector Index is the 11th index type in pangrova-indexing, reusing IndexEngineRegistry, IndexStorageManager, and IndexBuildCoordinator infrastructure
- Query semantics extend from "filter split" (prune irrelevant files) to "search + rank" (find nearest neighbors and sort by distance)
- CPU-intensive vector computation is prioritized for the Native layer (C++/Velox); Java handles index management and scheduling
- Phased implementation: Phase 0 Type System → Phase 1 Distance Functions → Phase 2 SPI Extension → Phase 3 Algorithms → Phase 4 Query Integration
- **Unified SPI**: Vector search extends `IndexStrategyProvider` rather than introducing a parallel SPI, keeping IndexEngineRegistry as the single discovery mechanism
- **Integrated dispatch**: Vector queries flow through the existing QueryDispatcher/DAGDispatchEngine pipeline, not a separate dispatcher
- **Snowflake L0/L1 alignment**: Pre-filter predicates (WHERE clause) still go through the three-layer pruning pipeline (L0 statistics → L1 native eval → L2 Pangrova index); only the ANN search itself is a new execution path on the Worker

## 2. Dependency Chain

Vector Index depends on the following infrastructure in order:

```
Phase 0: VECTOR type system (§3)
    ↓
Phase 1: Distance functions (§4)
    ↓
Phase 2: SPI extension — IndexType.VECTOR + VectorSearchRequest (§5)
    ↓
Phase 3: Algorithm implementation — HNSW / IVFPQ (§6-7)
    ↓
Phase 4: Query integration — Planner → Coordinator → Worker full pipeline (§8)
```

## 3. VECTOR Type System

### 3.1 Type Definition

VECTOR is a parameterized composite type, similar to `DECIMAL(precision, scale)`.

```java
package org.pangrova.spi.connector.data.type;

/**
 * Fixed-dimension floating-point vector type for embedding storage.
 *
 * <p>Stored as a contiguous array of 32-bit or 64-bit IEEE 754 floats.
 * Dimension is fixed at table creation time and immutable.</p>
 *
 * @param dimension   number of elements (1..65535)
 * @param elementType FLOAT (32-bit) or DOUBLE (64-bit), default FLOAT
 */
public record VectorType(int dimension, ElementType elementType) implements Type {

    public enum ElementType { FLOAT, DOUBLE }

    public VectorType {
        if (dimension < 1 || dimension > 65535) {
            throw new IllegalArgumentException(
                "dimension must be in [1, 65535], got: " + dimension);
        }
        if (elementType == null) elementType = ElementType.FLOAT;
    }

    /** Convenience constructor with default FLOAT element type. */
    public VectorType(int dimension) {
        this(dimension, ElementType.FLOAT);
    }

    @Override
    public String getDisplayName() {
        return elementType == ElementType.FLOAT
            ? "VECTOR(" + dimension + ")"
            : "VECTOR(" + dimension + ", " + elementType + ")";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("VECTOR",
            List.of(String.valueOf(dimension), elementType.name()));
    }

    /** Byte size of a single vector value. */
    public int bytesPerVector() {
        return dimension * (elementType == ElementType.FLOAT ? 4 : 8);
    }
}
```

### 3.2 PangrovaType Enum Extension

```java
// In PangrovaType.java — add:
/** Fixed-dimension floating-point vector for ANN search. */
VECTOR
```

**Type bridge strategy**: `PangrovaTypeBridge.toType(VECTOR)` returns `null` (dimension is unknown without DDL context). The actual `VectorType` instance is constructed during DDL parsing when the dimension is available. `fromSqlTypeName("VECTOR(128)")` parses dimension from parentheses and returns `new VectorType(128)`. This avoids the need for a sentinel `VectorType(0)` that would violate the constructor's dimension >= 1 invariant.

### 3.3 SQL Syntax

```sql
-- DDL: create table with vector column
CREATE TABLE embeddings (
    id BIGINT,
    content VARCHAR,
    embedding VECTOR(768),                    -- 768-dim, default FLOAT
    embedding_f64 VECTOR(384, DOUBLE)         -- 384-dim, DOUBLE precision
);

-- DML: insert vector literal
INSERT INTO embeddings VALUES (
    1, 'hello world', ARRAY[0.1, 0.2, ..., 0.768]
);
```

### 3.4 Storage Mapping

| Format | Physical Representation |
|--------|------------------------|
| Parquet | `fixed_size_list(dimension, float/double)` with `VECTOR` logical type annotation |
| ORC | `list(float/double)` with metadata annotation `pangrova.vector.dimension=N` |
| Arrow | `FixedSizeListArray` with `float`/`double` child |
| Protobuf | `repeated float values = 1; int32 dimension = 2;` |

### 3.5 Validation Rules

- INSERT/UPDATE: vector dimension must match column definition; mismatch throws `DimensionMismatchException`
- NULL vectors allowed (represents missing embedding)
- Vector elements must not be NaN or Infinity (validated during index build)

### 3.6 Module Impact

| Module | Change |
|--------|--------|
| pangrova-spi | Add `VectorType` record in `o.a.spi.connector.data.type` |
| pangrova-spi | Add `VECTOR` to `PangrovaType` enum |
| pangrova-spi | Add `PangrovaTypeBridge` mapping (returns null for VECTOR; DDL parser constructs VectorType) |
| pangrova-parser | ANTLR grammar: `VECTOR '(' INT (',' (FLOAT\|DOUBLE))? ')'` |
| pangrova-protocol | Protobuf `VectorValue` message; Substrait `FixedSizeList` mapping |
| pangrova-connector-iceberg | Parquet `fixed_size_list` ↔ `VectorType` conversion |
| pangrova-connector-paimon | ORC `list` + metadata ↔ `VectorType` conversion |

## 4. Distance Functions

### 4.1 Supported Metrics

```java
package org.pangrova.spi.index;

/** Distance metric for vector similarity computation. */
public enum DistanceMetric {
    /** Euclidean distance: sqrt(sum((a[i] - b[i])^2)). Lower = more similar. */
    L2,
    /** Cosine distance: 1 - cosine_similarity. Lower = more similar. */
    COSINE,
    /** Inner product (negative): -dot(a, b). Lower = more similar (for normalized vectors). */
    INNER_PRODUCT
}
```

> **Package**: `o.a.spi.index` — alongside IndexType, IndexDescriptor, IndexStrategyProvider. DistanceMetric is an index-domain concept, not a connector-adapter concept (`o.a.spi.connector.index` is for FileSystemAccessor, ColumnValueReader, CommitListener etc.).

### 4.2 SQL Functions

```sql
-- Distance functions (return DOUBLE)
SELECT l2_distance(a.embedding, b.embedding) FROM ...;
SELECT cosine_distance(a.embedding, b.embedding) FROM ...;
SELECT inner_product(a.embedding, b.embedding) FROM ...;

-- Convenience: cosine similarity (= 1 - cosine_distance)
SELECT cosine_similarity(a.embedding, b.embedding) FROM ...;
```

### 4.3 Execution Strategy

Distance computation is CPU-intensive. Two execution layers:

| Layer | Implementation | Use Case |
|-------|---------------|----------|
| Java (pangrova-indexing) | `VectorDistanceCalculator` — pure Java loop | Index build, small-batch computation, testing |
| Native (pangrova-worker-native) | C++ SIMD (AVX2/AVX-512/NEON) | Query-time large-batch distance computation |

> **Module placement**: `VectorDistanceCalculator` resides in `o.a.indexing.engine.vector` (not pangrova-common). Rationale: (1) pangrova-common is a leaf module with no dependencies — it cannot reference `DistanceMetric` from pangrova-spi; (2) distance calculation is index-domain business logic, not a shared utility; (3) if Worker needs a Java fallback, it can access it through pangrova-indexing's dependency chain.

```java
package org.pangrova.indexing.engine.vector;

/** Pure-Java vector distance calculator. Thread-safe, stateless. */
public final class VectorDistanceCalculator {

    public static double l2Distance(float[] a, float[] b) {
        assert a.length == b.length;
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    public static double cosineDistance(float[] a, float[] b) {
        assert a.length == b.length;
        double dot = 0.0, normA = 0.0, normB = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        double denom = Math.sqrt(normA) * Math.sqrt(normB);
        return denom == 0.0 ? 1.0 : 1.0 - dot / denom;
    }

    public static double innerProduct(float[] a, float[] b) {
        assert a.length == b.length;
        double dot = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
        }
        return -dot; // negate so lower = more similar
    }

    private VectorDistanceCalculator() {}
}
```

## 5. SPI Extensions

### 5.1 IndexType Extension

```java
public enum IndexType {
    // ... existing 10 types ...
    /** Vector ANN index for approximate nearest neighbor search (HNSW/IVFPQ). */
    VECTOR
}
```

IndexType grows from 10 → 11 values. IndexData sealed interface adds `VectorIndexData` permit.

### 5.2 VectorIndexData

> **Package**: `o.a.spi.index.data.vector` — in a dedicated sub-package after the §12.2 split. The `IndexData` sealed interface in `o.a.spi.index.data` adds `VectorIndexData` to its permits clause.

```java
package org.pangrova.spi.index.data.vector;

/**
 * Vector index data container. Unlike other IndexData types that store
 * per-file filter structures, VectorIndexData stores a graph/quantization
 * structure for ANN search within a single data file's vectors.
 *
 * <p>The actual index structure (HNSW graph or IVF centroids + PQ codes)
 * is stored as an opaque byte array. The algorithm-specific interpretation
 * is handled by the corresponding IndexStrategyProvider implementation.</p>
 *
 * <p>The {@code indexPayload} byte array is NOT defensively copied on
 * construction for performance reasons (payloads can be 12MB+). Callers
 * MUST NOT modify the array after passing it to the constructor. The
 * accessor also returns the internal reference — treat as read-only.</p>
 *
 * @param algorithmType  HNSW or IVFPQ
 * @param dimension      vector dimension
 * @param metric         distance metric used during build
 * @param vectorCount    number of vectors indexed
 * @param indexPayload   serialized algorithm-specific index structure (caller must not modify)
 * @param buildParams    parameters used during build (for rebuild consistency)
 */
public record VectorIndexData(
        VectorAlgorithmType algorithmType,
        int dimension,
        DistanceMetric metric,
        int vectorCount,
        byte[] indexPayload,
        Map<String, String> buildParams) implements IndexData {

    public enum VectorAlgorithmType { HNSW, IVFPQ }

    public VectorIndexData {
        // No defensive copy of indexPayload — see Javadoc.
        // This is consistent with other large-payload IndexData types.
        buildParams = Map.copyOf(buildParams);
        if (dimension < 1) throw new IllegalArgumentException("dimension must be positive");
        if (vectorCount < 0) throw new IllegalArgumentException("vectorCount must be non-negative");
        if (indexPayload == null) throw new IllegalArgumentException("indexPayload must not be null");
    }

    @Override public IndexType indexType() { return IndexType.VECTOR; }

    @Override public long estimatedSizeInBytes() {
        return indexPayload.length + buildParams.size() * 64L;
    }
}
```

### 5.3 VectorSearchRequest (NOT an IndexPredicate)

Traditional IndexPredicate is a boolean filtering contract — all permits (EqualTo, RangeQuery, InList, etc.) feed into `evaluate() → IndexEvalResult(SKIP/KEEP/UNKNOWN)`. Vector ANN search has fundamentally different semantics: it returns scored Top-K rows, not a boolean decision.

**Design decision**: `VectorSearchRequest` is a standalone record in `o.a.spi.index`, NOT a permit of the `IndexPredicate` sealed interface. This avoids:
- Forcing all existing `IndexStrategyProvider.evaluate()` implementations to handle an incompatible case
- Breaking the exhaustive pattern matching guarantee of the sealed interface
- Semantic pollution (mixing "should I skip this file?" with "find me the K nearest rows")

```java
package org.pangrova.spi.index;

/**
 * Request for approximate nearest neighbor search on a vector index.
 *
 * <p>This is NOT an IndexPredicate. Vector search returns scored rows,
 * not SKIP/KEEP decisions. It flows through a separate execution path
 * on the Worker (vectorSearch on IndexStrategyProvider).</p>
 *
 * @param column       the vector column to search
 * @param queryVector  the query vector (defensive copy on construction)
 * @param topK         number of nearest neighbors to return
 * @param metric       distance metric (must match the index's metric)
 */
public record VectorSearchRequest(
        String column,
        float[] queryVector,
        int topK,
        DistanceMetric metric) {

    public VectorSearchRequest {
        if (column == null || column.isBlank())
            throw new IllegalArgumentException("column required");
        if (queryVector == null || queryVector.length == 0)
            throw new IllegalArgumentException("queryVector required");
        if (topK < 1)
            throw new IllegalArgumentException("topK must be >= 1");
        if (metric == null)
            throw new IllegalArgumentException("metric required");
        queryVector = queryVector.clone(); // defensive copy (query vectors are small)
    }
}
```

### 5.4 VectorSearchResult

```java
package org.pangrova.spi.index;

/**
 * Result of a vector ANN search within a single data file.
 *
 * @param dataFilePath  the data file searched
 * @param neighbors     top-K nearest neighbors, sorted by distance ascending
 */
public record VectorSearchResult(
        String dataFilePath,
        List<ScoredRow> neighbors) {

    public VectorSearchResult {
        neighbors = List.copyOf(neighbors);
    }

    /**
     * A single row with its distance score.
     *
     * <p>Ordering uses {@link #compareTo} (Double.compare semantics).
     * Note: record-generated {@code equals}/{@code hashCode} use
     * {@code Double.equals} semantics (NaN==NaN, +0.0≠-0.0), which
     * differs from {@code compareTo}. For merge-heap correctness,
     * only {@code compareTo} is used; {@code equals} is not relied upon
     * in the merge path.</p>
     *
     * @param rowOffset  row position within the data file
     * @param distance   distance to query vector (lower = more similar)
     */
    public record ScoredRow(long rowOffset, double distance)
            implements Comparable<ScoredRow> {

        @Override
        public int compareTo(ScoredRow other) {
            return Double.compare(this.distance, other.distance);
        }
    }
}
```

> **Package**: Both `VectorSearchRequest` and `VectorSearchResult` reside in `o.a.spi.index` — alongside IndexDescriptor, IndexType, DistanceMetric. They are index-domain value objects.

### 5.5 IndexStrategyProvider Extension (Unified SPI)

**Design decision**: Instead of introducing a parallel `VectorIndexStrategy` SPI, we extend the existing `IndexStrategyProvider` with default methods for vector search. This keeps `IndexEngineRegistry` as the single discovery mechanism and allows `IndexBuildCoordinator` to dispatch vector builds through the same pipeline.

```java
// Added to existing IndexStrategyProvider interface:

/**
 * Returns true if this provider supports vector ANN search.
 * Only VECTOR type providers return true. Default: false.
 */
default boolean supportsVectorSearch() { return false; }

/**
 * Executes an ANN search on the given index data.
 *
 * <p>Only callable when {@link #supportsVectorSearch()} returns true.
 * The default implementation throws UnsupportedOperationException.</p>
 *
 * @param indexData     the vector index data for a single data file
 * @param request       the search request (query vector, topK, metric)
 * @param searchParams  algorithm-specific search-time parameters (e.g., efSearch, nprobe)
 * @return top-K neighbors sorted by distance ascending
 * @throws UnsupportedOperationException if this provider does not support vector search
 */
default List<VectorSearchResult.ScoredRow> vectorSearch(
        IndexData indexData,
        VectorSearchRequest request,
        Map<String, String> searchParams) {
    throw new UnsupportedOperationException(
        supportedType() + " does not support vector search");
}
```

**How it works**:
- A single `VectorStrategyProvider` implements `IndexStrategyProvider` with `supportedType() = VECTOR`
- `evaluate()` returns `KEEP` for vector indexes (they don't do boolean filtering)
- `vectorSearch()` performs the actual ANN search, delegating to internal `HnswEngine` or `IvfpqEngine` based on `IndexDescriptor.properties().get("algorithm")`
- `IndexEngineRegistry.getProvider(IndexType.VECTOR)` returns the vector provider — no separate registry needed
- `IndexBuildCoordinator` calls `provider.build()` as usual; the vector provider internally reads float[] vectors from the ColumnValueReader output

**Algorithm dispatch**: `IndexEngineRegistry` enforces exactly one provider per `IndexType` (duplicate registration throws `IllegalStateException`). Therefore, a single `VectorStrategyProvider` is registered for `IndexType.VECTOR` and delegates internally to HNSW or IVFPQ logic based on `IndexDescriptor.properties().get("algorithm")`. `HnswEngine` and `IvfpqEngine` mentioned in §16 are internal algorithm implementations (package-private classes in `o.a.indexing.engine.vector`), NOT separate `IndexStrategyProvider` registrations. The public `VectorStrategyProvider` composes them via delegation.

### 5.6 ColumnValueReader Adaptation

The existing `ColumnValueReader.readColumns()` returns `Iterator<List<Comparable<?>>>`. Vector build needs `Iterator<float[]>`. Rather than polluting the generic SPI, the vector provider performs the conversion internally:

```java
// Inside VectorStrategyProvider (pangrova-indexing):
private Iterator<float[]> toVectorIterator(
        Iterator<List<Comparable<?>>> rows, int expectedDimension) {
    return new Iterator<>() {
        @Override public boolean hasNext() { return rows.hasNext(); }
        @Override public float[] next() {
            List<Comparable<?>> row = rows.next();
            // Row contains a single column value: the vector
            // Connector encodes vectors as List<Float> or float[]
            Object value = row.get(0);
            float[] vector = convertToFloatArray(value);
            if (vector.length != expectedDimension) {
                throw new DimensionMismatchException(expectedDimension, vector.length);
            }
            return vector;
        }
    };
}
```

This keeps `ColumnValueReader` SPI unchanged — the conversion is an implementation detail of the vector provider.

### 5.7 Query Semantics Difference

| Aspect | Traditional Index (10 types) | Vector Index |
|--------|------------------------------|-------------|
| Request type | IndexPredicate (EqualTo, RangeQuery, ...) | VectorSearchRequest (standalone) |
| Evaluation result | IndexEvalResult (SKIP/KEEP/UNKNOWN) | VectorSearchResult (scored row list) |
| Granularity | File-level skip decision | Row-level ranking |
| Composition | Multiple indexes AND/OR | Single vector column per query |
| Pipeline position | IndexSplitFilter (before task dispatch) | Worker task execution (during scan) |
| Correctness guarantee | No false negatives (never skip matching rows) | Approximate (recall < 100%, tunable) |
| SPI method | `IndexStrategyProvider.evaluate()` | `IndexStrategyProvider.vectorSearch()` |

## 6. HNSW Algorithm (Hierarchical Navigable Small World)

### 6.1 Overview

HNSW is a graph-based ANN algorithm that builds a multi-layer navigable small-world graph. Upper layers are sparse (long-range connections for fast navigation), lower layers are dense (short-range connections for precision). Search starts from the top layer and greedily descends.

### 6.2 Parameters

| Parameter | Description | Default | Range |
|-----------|-------------|---------|-------|
| `hnsw.M` | Max connections per node per layer | 16 | [4, 64] |
| `hnsw.efConstruction` | Dynamic candidate list size during build | 200 | [M, 4096] |
| `hnsw.efSearch` | Dynamic candidate list size during search (query-time) | 64 | [topK, 4096] |

Trade-offs:
- Higher M → better recall, more memory, slower build
- Higher efConstruction → better graph quality, slower build
- Higher efSearch → better recall, slower search

### 6.3 Build Algorithm

```
ALGORITHM buildHNSW(vectors, dimension, metric, M, efConstruction)
INPUT: vectors of type Iterator<float[]>, dimension, metric, M, efConstruction
OUTPUT: HnswGraph (multi-layer graph structure)

BEGIN
    maxLevel ← 0
    entryPoint ← null
    mL ← 1.0 / ln(M)  // level generation factor

    FOR EACH vector v WITH index i IN vectors DO
        level ← floor(-ln(random()) * mL)
        IF level > maxLevel THEN maxLevel ← level

        IF entryPoint IS null THEN
            entryPoint ← createNode(i, v, level)
            CONTINUE
        END IF

        // Phase 1: Greedy search from top to (level + 1)
        currentNode ← entryPoint
        FOR l FROM maxLevel DOWNTO (level + 1) DO
            currentNode ← greedyClosest(currentNode, v, l, metric)
        END FOR

        // Phase 2: Insert at each layer from level down to 0
        FOR l FROM min(level, maxLevel) DOWNTO 0 DO
            candidates ← searchLayer(currentNode, v, efConstruction, l, metric)
            neighbors ← selectNeighbors(candidates, M)
            FOR EACH neighbor n IN neighbors DO
                connect(i, n, l)
                connect(n, i, l)
                IF connectionCount(n, l) > (l == 0 ? 2*M : M) THEN
                    shrinkConnections(n, l, M, metric)
                END IF
            END FOR
            currentNode ← closestIn(candidates)
        END FOR
    END FOR

    RETURN HnswGraph(entryPoint, maxLevel, allNodes, allEdges)
END
```

### 6.4 Search Algorithm

```
ALGORITHM searchHNSW(graph, queryVector, topK, efSearch, metric)
INPUT: graph (HnswGraph), queryVector, topK, efSearch, metric
OUTPUT: List<ScoredRow> of size topK, sorted by distance ascending

BEGIN
    currentNode ← graph.entryPoint

    // Phase 1: Greedy descent from top layer to layer 1
    FOR l FROM graph.maxLevel DOWNTO 1 DO
        currentNode ← greedyClosest(currentNode, queryVector, l, metric)
    END FOR

    // Phase 2: Beam search at layer 0
    candidates ← searchLayer(currentNode, queryVector, efSearch, 0, metric)

    RETURN candidates.topK(topK).map(node → ScoredRow(node.rowOffset, node.distance))
END
```

### 6.5 Memory Footprint

Per-file HNSW index memory: `N * M * 2 * 4 bytes` (adjacency lists) + `N * dim * 4 bytes` (vectors, if stored in index).

For 100K vectors, dim=768, M=16: ~300MB per file index. This is significant — vectors should NOT be duplicated in the index; instead, store only the graph structure and read vectors from the data file on demand during search.

**Optimization**: Store graph-only index (adjacency lists + entry point), load vectors lazily from data file via `ColumnValueReader`. This reduces index size to ~12MB for the same scenario.

### 6.6 Serialization Format

```
[Header: magic(4) | version(1) | algorithmType(1) | dimension(4) | metric(1) | reserved(5)]
[GraphMeta: maxLevel(4) | entryPointId(4) | nodeCount(4) | M(4) | efConstruction(4)]
[Level offsets: levelCount(4) | offset[0..maxLevel](4 each)]
[Adjacency lists per level: for each node → neighborCount(2) | neighborIds(4 each)]
[Payload CRC32(4)]
```

## 7. IVFPQ Algorithm (Inverted File with Product Quantization)

### 7.1 Overview

IVFPQ combines IVF (Inverted File) which partitions the vector space into clusters using k-means, and PQ (Product Quantization) which compresses vectors within each cluster. This achieves much lower memory usage than HNSW at the cost of slightly lower recall.

### 7.2 Parameters

| Parameter | Description | Default | Range |
|-----------|-------------|---------|-------|
| `ivfpq.nlist` | Number of IVF clusters (Voronoi cells) | 256 | [1, 65536] |
| `ivfpq.nprobe` | Number of clusters to search (query-time) | 16 | [1, nlist] |
| `ivfpq.m` | Number of PQ sub-quantizers | 8 | dimension must be divisible by m |
| `ivfpq.nbits` | Bits per sub-quantizer code | 8 | [4, 8, 16] |

Trade-offs:
- Higher nlist → finer partitioning, better recall with same nprobe, slower build
- Higher nprobe → more clusters searched, better recall, slower search
- Higher m → finer quantization, better recall, more memory for codebook
- Higher nbits → larger codebook (2^nbits centroids per sub-space), better precision

### 7.3 Build Algorithm

```
ALGORITHM buildIVFPQ(vectors, dimension, metric, nlist, m, nbits)
INPUT: vectors (all vectors loaded into memory), dimension, metric, nlist, m, nbits
OUTPUT: IvfpqIndex (centroids + PQ codebook + inverted lists)

BEGIN
    // Step 1: Train IVF centroids via k-means
    centroids ← kMeans(vectors, nlist, metric, maxIterations=20)

    // Step 2: Assign each vector to nearest centroid
    assignments ← new Map<int, List<(rowOffset, float[])>>()
    FOR EACH vector v WITH index i IN vectors DO
        clusterId ← argmin(distance(v, centroids[j]) for j in 0..nlist-1)
        assignments[clusterId].add((i, v))
    END FOR

    // Step 3: Train PQ codebook
    subDim ← dimension / m
    codebook ← new float[m][2^nbits][subDim]
    FOR s FROM 0 TO m-1 DO
        subVectors ← extractSubSpace(vectors, s * subDim, (s+1) * subDim)
        codebook[s] ← kMeans(subVectors, 2^nbits, L2, maxIterations=20)
    END FOR

    // Step 4: Encode vectors using PQ codes
    invertedLists ← new Map<int, List<(rowOffset, byte[m])>>()
    FOR EACH (clusterId, vectorList) IN assignments DO
        FOR EACH (rowOffset, v) IN vectorList DO
            residual ← v - centroids[clusterId]
            codes ← new byte[m]
            FOR s FROM 0 TO m-1 DO
                subResidual ← residual[s*subDim .. (s+1)*subDim]
                codes[s] ← argmin(distance(subResidual, codebook[s][j]) for j in 0..2^nbits-1)
            END FOR
            invertedLists[clusterId].add((rowOffset, codes))
        END FOR
    END FOR

    RETURN IvfpqIndex(centroids, codebook, invertedLists)
END
```

### 7.4 Search Algorithm

```
ALGORITHM searchIVFPQ(index, queryVector, topK, nprobe, metric)
INPUT: index (IvfpqIndex), queryVector, topK, nprobe, metric
OUTPUT: List<ScoredRow> of size topK

BEGIN
    // Step 1: Find nprobe nearest centroids
    centroidDistances ← [distance(queryVector, c) for c in index.centroids]
    probeClusters ← topK(centroidDistances, nprobe)

    // Step 2: Precompute distance tables for PQ (ADC)
    distanceTables ← new double[m][2^nbits]
    FOR s FROM 0 TO m-1 DO
        querySubVector ← queryVector[s*subDim .. (s+1)*subDim]
        FOR j FROM 0 TO 2^nbits-1 DO
            distanceTables[s][j] ← distance(querySubVector, index.codebook[s][j])
        END FOR
    END FOR

    // Step 3: Scan inverted lists using ADC
    candidates ← new MaxHeap(topK)
    FOR EACH clusterId IN probeClusters DO
        FOR EACH (rowOffset, codes) IN index.invertedLists[clusterId] DO
            approxDist ← 0.0
            FOR s FROM 0 TO m-1 DO
                approxDist ← approxDist + distanceTables[s][codes[s]]
            END FOR
            candidates.insertIfCloser(ScoredRow(rowOffset, approxDist))
        END FOR
    END FOR

    RETURN candidates.sorted()
END
```

### 7.5 Memory Footprint

IVFPQ is designed for memory efficiency:
- Centroids: `nlist * dimension * 4 bytes` (e.g., 256 * 768 * 4 = 768KB)
- Codebook: `m * 2^nbits * (dimension/m) * 4 bytes` (e.g., 8 * 256 * 96 * 4 = 768KB)
- PQ codes: `N * m bytes` (e.g., 100K * 8 = 800KB)
- Total for 100K vectors, dim=768: ~2.3MB (vs ~300MB for brute-force, ~12MB for HNSW graph-only)

### 7.6 HNSW vs IVFPQ Selection Guide

| Criterion | HNSW | IVFPQ |
|-----------|------|-------|
| Recall@10 | 95-99% | 85-95% |
| Memory per 100K vectors (dim=768) | ~12MB (graph-only) | ~2.3MB |
| Build time | O(N * log(N) * M * efConstruction) | O(N * nlist * k-means-iters) + O(N * m) |
| Search latency | O(log(N) * efSearch) | O(nprobe * N/nlist * m) |
| Best for | High recall requirements, < 10M vectors | Memory-constrained, > 10M vectors |
| Incremental insert | Supported (add node to graph) | Not supported (requires retrain) |

**Recommendation**: Default to HNSW for most use cases. Use IVFPQ when memory is constrained or vector count exceeds 10M per file.

## 8. Query Integration

### 8.1 Relationship to Snowflake Three-Layer Pruning

The existing three-layer pruning architecture (doc `26-snowflake-inspired-indexing-redesign.md`) handles boolean filtering:
- L0: Write-time statistics (min/max/NDV) — file column statistics
- L1: Search Access Path (BTree/Bloom/Bitmap) — Pangrova indexes
- L2: Runtime pruning (JOIN/top-k/LIMIT) — planner-driven

Vector Index does NOT participate in L0/L1/L2 split pruning because ANN search is not a boolean filter — it cannot produce SKIP/KEEP decisions. Instead:

```
Query: SELECT ... WHERE category='tech' ORDER BY l2_distance(embedding, ?) LIMIT 10

Pipeline:
  1. Pre-filter predicates (WHERE category='tech') flow through L0/L1/L2 as usual
     → Prunes splits using traditional indexes (BTree on category, etc.)
  2. Surviving splits are dispatched to Workers via existing QueryDispatcher
  3. Worker executes ANN search on each surviving split's data files
     → Uses VectorSearchExecutor (new component, invoked during task execution)
  4. Coordinator merges per-file Top-K into global Top-K
     → Uses VectorResultMerger (new component, invoked during result collection)
```

This means pre-filter + ANN is naturally composable: traditional pruning reduces the file set, then ANN search runs only on surviving files.

### 8.2 Index Coverage Window and Brute-Force Fallback

When a data file has no vector index (new file not yet indexed, or index build in progress), the file's vectors are NOT silently excluded from ANN search. Unlike traditional indexes where missing index = conservative KEEP (scan the file), missing vector index = vectors completely absent from results, causing recall degradation.

**Fallback strategy**: For data files without a VECTOR index in READY state, the Worker performs brute-force distance computation (read all vectors via ColumnValueReader, compute distances, return Top-K). This ensures:
- Recall is not degraded by incomplete index coverage
- New data is immediately queryable (consistent with Snowflake's "availability window" concept)
- Only the optimization path is affected, not correctness

```
Worker execution per data file:
  IF vectorIndex exists AND state == READY THEN
      result ← provider.vectorSearch(indexData, request, searchParams)  // ANN path
  ELSE
      result ← bruteForceScan(dataFile, request)  // fallback path
  END IF
```

The brute-force fallback is expensive but correct. As index coverage approaches 100%, the fallback is rarely triggered. The `IndexCoverageWindow` mechanism (existing) tracks coverage ratio and can trigger priority builds for uncovered files.

### 8.3 SQL Syntax for ANN Search

```sql
-- Basic ANN search: find 10 nearest neighbors
SELECT id, content, l2_distance(embedding, ARRAY[0.1, 0.2, ...]) AS distance
FROM embeddings
ORDER BY l2_distance(embedding, ARRAY[0.1, 0.2, ...])
LIMIT 10;

-- With pre-filter: ANN search within a filtered subset
SELECT id, content, cosine_distance(embedding, ARRAY[0.1, 0.2, ...]) AS distance
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

### 8.4 DDL for Vector Index

```sql
-- Create HNSW index (default)
CREATE INDEX idx_embedding ON embeddings(embedding)
    USING VECTOR
    WITH (algorithm = 'HNSW', metric = 'L2', M = 16, efConstruction = 200);

-- Create IVFPQ index
CREATE INDEX idx_embedding_pq ON embeddings(embedding)
    USING VECTOR
    WITH (algorithm = 'IVFPQ', metric = 'COSINE', nlist = 256, m = 8, nbits = 8);

-- Drop
DROP INDEX idx_embedding ON embeddings;

-- Show (includes vector-specific info)
SHOW INDEXES FROM embeddings;
-- Output: index_name | index_type | columns | state | algorithm | metric | vector_count | ...
```

### 8.5 Planner Recognition (ANN Pattern Detection)

Planner detects `ORDER BY distance_function(...) LIMIT K` and rewrites to a VectorSearchNode.

```
Recognition rule:
  IF  query contains ORDER BY <distance_func>(column, literal_vector)
  AND query contains LIMIT K (K > 0)
  AND column has VECTOR index in READY state (or brute-force fallback enabled)
  AND distance_func matches index's metric
  THEN rewrite to VectorSearchNode
```

```java
package org.pangrova.planner.vector;

/**
 * Detects ORDER BY distance_function(vector_col, query_vector) LIMIT K
 * patterns and rewrites them into VectorSearchNode plan nodes.
 *
 * <p>Invoked during the logical optimization phase, after predicate pushdown
 * but before physical planning. Activates when a VECTOR index exists
 * on the target column in READY state, or when brute-force fallback is
 * acceptable (no index but VECTOR column exists).</p>
 *
 * <p>Thread-safe, stateless.</p>
 */
public final class VectorSearchRewriter {

    public Optional<VectorSearchNode> tryRewrite(
            LogicalSort sortNode,
            LogicalLimit limitNode,
            IndexLookupFunction indexLookup) {
        // 1. Extract distance function call from sort key
        // 2. Validate: single sort key, ASC order, distance function recognized
        // 3. Extract column name, query vector literal, metric
        // 4. Check VECTOR index exists on column with matching metric
        // 5. Build VectorSearchNode with topK = limit value
        // ...
    }
}
```

```java
package org.pangrova.planner.vector;

/**
 * Logical plan node representing an ANN vector search.
 *
 * <p>Replaces the original TableScan + Sort + Limit subtree when
 * VectorSearchRewriter detects the ANN pattern. Flows through the
 * existing QueryDispatcher pipeline — NOT a separate dispatch path.</p>
 *
 * @param tableName     fully qualified table name
 * @param vectorColumn  the vector column to search
 * @param queryVector   the query vector (from SQL literal)
 * @param topK          number of nearest neighbors
 * @param metric        distance metric
 * @param indexId       the VECTOR index to use (nullable if brute-force only)
 * @param preFilters    optional pre-filter predicates (WHERE clause, for traditional L0/L1/L2 pruning)
 * @param hybridStrategy the pre-filter + ANN execution strategy
 */
public record VectorSearchNode(
        String tableName,
        String vectorColumn,
        float[] queryVector,
        int topK,
        DistanceMetric metric,
        String indexId,
        List<IndexPredicate> preFilters,
        VectorHybridSearchStrategy hybridStrategy) {

    public VectorSearchNode {
        queryVector = queryVector.clone();
        preFilters = preFilters == null ? List.of() : List.copyOf(preFilters);
    }
}
```

### 8.6 Coordinator Dispatch (Integrated Pipeline)

**Design decision**: Vector queries flow through the existing `QueryDispatcher` / `DAGDispatchEngine` pipeline. There is NO separate `VectorSearchDispatcher`. Rationale:
- Reuses existing fault tolerance, retry, timeout, and metrics infrastructure
- Pre-filter predicates naturally compose with traditional split pruning
- Result collection and merge integrate with existing transport layer

The Coordinator's role in vector search:

```
Dispatch flow (within existing QueryDispatcher):
  1. Planner produces VectorSearchNode
  2. QueryDispatcher recognizes VectorSearchNode, applies pre-filter predicates
     through IndexSplitFilter (L0/L1/L2 traditional pruning) to reduce split set
  3. Dispatches VectorSearchTask to Workers holding surviving splits
     (VectorSearchTask is a new task type in pangrova-protocol, carrying query vector + params)
  4. Workers execute per-file ANN search (§8.7) and return VectorSearchResult
  5. Coordinator collects per-file results via VectorResultMerger
```

```java
package org.pangrova.coordinator.query.execution;

/**
 * Merges per-file/per-worker vector search results into a global Top-K.
 *
 * <p>Invoked by the existing result collection pipeline when the query
 * plan contains a VectorSearchNode. NOT a standalone dispatcher.</p>
 *
 * <p>Thread-safe, stateless.</p>
 */
public final class VectorResultMerger {

    /**
     * Merges partial Top-K results into global Top-K.
     *
     * <p>Uses a max-heap of size K. Total candidates = numFiles * K,
     * heap operations are O(log K), so merge is O(numFiles * K * log K).</p>
     *
     * @param partialResults per-file search results from workers
     * @param topK           global Top-K to return
     * @return merged global Top-K result
     */
    public VectorSearchResult merge(
            List<VectorSearchResult> partialResults, int topK) {
        PriorityQueue<VectorSearchResult.ScoredRow> heap =
                new PriorityQueue<>(Comparator.comparingDouble(
                        VectorSearchResult.ScoredRow::distance).reversed());

        for (VectorSearchResult partial : partialResults) {
            for (VectorSearchResult.ScoredRow row : partial.neighbors()) {
                heap.offer(row);
                if (heap.size() > topK) {
                    heap.poll();
                }
            }
        }

        List<VectorSearchResult.ScoredRow> globalTopK = new ArrayList<>(heap);
        globalTopK.sort(Comparator.comparingDouble(
                VectorSearchResult.ScoredRow::distance));
        return new VectorSearchResult("global", globalTopK);
    }
}
```

### 8.7 Worker Execution

Worker receives VectorSearchTask, loads the local VECTOR index, and executes ANN search.

```java
package org.pangrova.worker.execution;

/**
 * Executes ANN search on a single data file using the loaded vector index.
 *
 * <p>Execution flow:</p>
 * <ol>
 *   <li>Load VectorIndexData from IndexStorageManager (with cache)</li>
 *   <li>Resolve IndexStrategyProvider for VECTOR type from IndexEngineRegistry</li>
 *   <li>If index exists and READY: call provider.vectorSearch()</li>
 *   <li>If index missing: brute-force fallback via ColumnValueReader</li>
 *   <li>Return per-file Top-K as VectorSearchResult</li>
 * </ol>
 *
 * <p>Thread-safe. One instance per worker, reused across queries.</p>
 */
public final class VectorSearchExecutor {

    private final IndexEngineRegistry engineRegistry;
    private final IndexStorageManager storageManager;
    private final IndexCache cache;

    public VectorSearchExecutor(
            IndexEngineRegistry engineRegistry,
            IndexStorageManager storageManager,
            IndexCache cache) {
        this.engineRegistry = engineRegistry;
        this.storageManager = storageManager;
        this.cache = cache;
    }

    public VectorSearchResult execute(VectorSearchTask task) {
        // 1. Try to load index data
        Optional<VectorIndexData> indexData = loadVectorIndex(task);

        if (indexData.isPresent()) {
            // 2a. ANN path: use index
            IndexStrategyProvider provider =
                    engineRegistry.getProvider(IndexType.VECTOR);
            VectorSearchRequest request = new VectorSearchRequest(
                    task.vectorColumn(), task.queryVector(),
                    task.topK(), task.metric());
            Map<String, String> searchParams = buildSearchParams(task);
            List<VectorSearchResult.ScoredRow> neighbors =
                    provider.vectorSearch(indexData.get(), request, searchParams);
            return new VectorSearchResult(task.dataFilePath(), neighbors);
        } else {
            // 2b. Brute-force fallback: scan all vectors
            return bruteForceScan(task);
        }
    }
}
```

### 8.8 Pre-Filter + ANN Hybrid Strategy

When SQL contains both WHERE clause and ORDER BY distance() LIMIT K, the system must combine traditional filtering with ANN search.

```
Strategy selection:
  selectivity = estimated_rows_after_filter / total_rows

  IF selectivity > 0.5 THEN
      strategy = ANN_THEN_FILTER
      // Over-fetch from ANN, then post-filter
  ELSE
      strategy = FILTER_THEN_BRUTEFORCE
      // Filter first, then brute-force distance on survivors
  END IF
```

| Strategy | Flow | Best When |
|----------|------|-----------|
| ANN_THEN_FILTER | ANN search → filter results → pad if needed | selectivity > 50% |
| FILTER_THEN_BRUTEFORCE | Traditional filter → brute-force distance on survivors | selectivity < 50% |

**Recall caveat for ANN_THEN_FILTER**: The over-fetch multiplier (`topK / selectivity * 1.5`) is heuristic. When the filter removes many ANN results, the final result set may have fewer than K rows or miss some true nearest neighbors. This is an inherent trade-off of approximate search with post-filtering. VP-22 (§14) documents this: "ANN_THEN_FILTER recall ≤ base ANN recall; degradation proportional to filter selectivity."

```java
package org.pangrova.planner.vector;

/**
 * Decides the execution strategy when both pre-filters and ANN search
 * are present in the same query.
 */
public final class VectorHybridSearchPlanner {

    private static final double ANN_FIRST_THRESHOLD = 0.5;

    public VectorHybridSearchStrategy selectStrategy(
            double estimatedSelectivity, int topK, long totalRows) {
        if (estimatedSelectivity > ANN_FIRST_THRESHOLD) {
            int overFetchK = (int) Math.min(
                    topK / estimatedSelectivity * 1.5, totalRows);
            return new VectorHybridSearchStrategy(
                    VectorHybridSearchStrategy.Mode.ANN_THEN_FILTER, overFetchK);
        }
        return new VectorHybridSearchStrategy(
                VectorHybridSearchStrategy.Mode.FILTER_THEN_BRUTEFORCE, topK);
    }
}
```

```java
package org.pangrova.planner.vector;

/**
 * Hybrid search execution strategy for combined pre-filter + ANN queries.
 *
 * @param mode        ANN_THEN_FILTER or FILTER_THEN_BRUTEFORCE
 * @param effectiveK  adjusted K for over-fetching (only for ANN_THEN_FILTER)
 */
public record VectorHybridSearchStrategy(Mode mode, int effectiveK) {
    public enum Mode { ANN_THEN_FILTER, FILTER_THEN_BRUTEFORCE }
}
```

> **Naming**: `VectorHybridSearchStrategy` (not `HybridStrategy`) to avoid cross-module name collision (C2 naming rule) and clarify domain context.

## 9. Build Integration

### 9.1 Vector Index Build vs Traditional Index Build

| Aspect | Traditional Index Build | Vector Index Build |
|--------|------------------------|--------------------|
| Memory model | Streaming (one row at a time) | Batch (HNSW: incremental insert; IVFPQ: all vectors in memory for k-means) |
| CPU profile | Light (hash/compare) | Heavy (distance computation, graph construction) |
| Build granularity | Per data file, independent | Per data file (HNSW); potentially cross-file for IVFPQ training |
| Incremental | Trivially append new rows | HNSW: supported; IVFPQ: requires full retrain |
| Memory budget | Low (KB-MB per file) | High (MB-GB per file depending on vector count and dimension) |

### 9.2 Build Flow

```
VectorIndexBuild (per data file):
  1. IndexBuildCoordinator receives build request for VECTOR index
  2. Resolves IndexStrategyProvider for VECTOR type via IndexEngineRegistry
  3. Provider internally detects algorithm type from IndexDescriptor.properties
  4. Provider reads vectors via ColumnValueReader → toVectorIterator() conversion
  5. Validates dimensions (all must match VectorType.dimension)
  6. Checks memory budget (VectorBuildMemoryEstimator)
  7. Calls algorithm-specific build (HNSW or IVFPQ)
  8. Wraps result in VectorIndexData
  9. Persists via IndexStorageManager
  10. Updates IndexMetadataStore with new IndexFileMapping
```

This flow is identical to traditional index builds from IndexBuildCoordinator's perspective — it calls `provider.build()` and gets back `IndexData`. The vector-specific logic (memory estimation, algorithm dispatch) is encapsulated inside the provider.

### 9.3 Memory Budget Control

```java
package org.pangrova.indexing.engine.vector;

/**
 * Validates that a vector index build request fits within the configured
 * memory budget. Rejects builds that would exceed the limit.
 */
public final class VectorBuildMemoryEstimator {

    public long estimatePeakMemory(
            long vectorCount, int dimension, Map<String, String> params) {
        String algorithm = params.getOrDefault("algorithm", "HNSW");
        return switch (algorithm) {
            case "HNSW" -> estimateHnswMemory(vectorCount, dimension, params);
            case "IVFPQ" -> estimateIvfpqMemory(vectorCount, dimension, params);
            default -> throw new IllegalArgumentException(
                    "Unknown vector algorithm: " + algorithm);
        };
    }

    private long estimateHnswMemory(
            long vectorCount, int dimension, Map<String, String> params) {
        int m = Integer.parseInt(params.getOrDefault("M", "16"));
        long vectorBytes = vectorCount * dimension * 4L;
        long graphBytes = vectorCount * m * 2L * 4L;
        return vectorBytes + graphBytes;
    }

    private long estimateIvfpqMemory(
            long vectorCount, int dimension, Map<String, String> params) {
        int nlist = Integer.parseInt(params.getOrDefault("nlist", "256"));
        int m = Integer.parseInt(params.getOrDefault("m", "8"));
        int nbits = Integer.parseInt(params.getOrDefault("nbits", "8"));
        long vectorBytes = vectorCount * dimension * 4L;
        long centroidBytes = (long) nlist * dimension * 4L;
        long codebookBytes = (long) m * (1L << nbits) * (dimension / m) * 4L;
        return vectorBytes + centroidBytes + codebookBytes;
    }
}
```

## 10. Incremental Update

### 10.1 HNSW Incremental Insert

HNSW naturally supports incremental insertion. Per-file independent indexes are the standard model (consistent with Snowflake independent storage):

```
CommitListener receives CommitEvent (new data files):
  FOR EACH newDataFile IN event.addedFiles() DO
      // Build independent per-file HNSW index
      buildPerFileIndex(newDataFile, descriptor)
  END FOR
```

Each data file has its own HNSW graph. At query time, all per-file graphs are searched in parallel, and the Coordinator merges global Top-K.

**Why not cross-file graph merging**: Merging HNSW graphs across files requires recomputing all edge distances — equivalent to full rebuild. Per-file model aligns with Iceberg/Paimon snapshot semantics. File compaction makes old indexes orphans, cleaned by GC.

### 10.2 IVFPQ Incremental Limitation

IVFPQ centroids and PQ codebook are globally trained. New data may shift optimal cluster distribution.

```
CommitListener receives CommitEvent (new data files):
  FOR EACH newDataFile IN event.addedFiles() DO
      // Encode new vectors using existing codebook (no retrain)
      encodeWithExistingCodebook(newDataFile, descriptor)

      // When cumulative new vectors exceed 30% of original training set,
      // mark index as STALE to trigger full rebuild
      IF newVectorRatio > 0.3 THEN
          markStale(descriptor)
      END IF
  END FOR
```

### 10.3 File Compaction Handling

Iceberg/Paimon compaction rewrites data files. Impact on Vector Index:

| Event | Action |
|-------|--------|
| Data file deleted (compaction source) | GC cleans corresponding vector index file |
| New data file created (compaction output) | CommitListener triggers vector index build for new file |
| Snapshot expiration | GC cleans vector index files associated with expired snapshots |

This is identical to traditional index compaction handling — no special logic needed.


## 11. Module Dependency Compliance

Vector Index touches 8 modules. All dependencies must comply with `architecture-governance-rules.md` red lines.

| Module | New Components | Dependencies Used | Compliance |
|--------|---------------|-------------------|------------|
| pangrova-spi | `VectorType`, `DistanceMetric`, `VectorSearchRequest`, `VectorSearchResult`, `VectorIndexData`, `VectorAlgorithmType`, `VECTOR` enum value in `IndexType` and `PangrovaType` | pangrova-common | ✅ SPI is leaf+common only |
| pangrova-parser | ANTLR grammar for `VECTOR(dim)` type, `CREATE INDEX ... USING VECTOR`, distance function syntax | pangrova-spi, pangrova-common | ✅ Parser allowed deps |
| pangrova-planner | `VectorSearchRewriter`, `VectorSearchNode`, `VectorHybridSearchPlanner`, `VectorHybridSearchStrategy` | pangrova-parser, pangrova-spi, pangrova-common, pangrova-security | ✅ Planner allowed deps |
| pangrova-indexing | `VectorDistanceCalculator`, `VectorBuildMemoryEstimator`, `VectorStrategyProvider` (public, registered), `HnswEngine`, `IvfpqEngine` (package-private delegates) | pangrova-spi, pangrova-common | ✅ Indexing depends on SPI+common |
| pangrova-coordinator | `VectorResultMerger` (in `o.a.coordinator.query.execution`) | pangrova-spi, pangrova-planner, pangrova-worker (transport only) | ✅ Coordinator allowed deps |
| pangrova-worker | `VectorSearchExecutor` (in `o.a.worker.execution`) | pangrova-spi, pangrova-protocol, pangrova-common | ✅ Worker allowed deps |
| pangrova-worker-native | SIMD distance functions (C++), vector scan operator | pangrova-protocol (JNI/Protobuf only) | ✅ JNI contract only |
| pangrova-protocol | `VectorSearchTask` protobuf message, `VectorValue` protobuf message | pangrova-spi, pangrova-common | ✅ Protocol allowed deps |

**Dependency direction verification**:
- No reverse dependency: pangrova-spi does NOT depend on pangrova-indexing or pangrova-worker
- No circular dependency: pangrova-indexing → pangrova-spi (one-way)
- pangrova-coordinator → pangrova-worker: only transport types (VectorSearchTask dispatch), consistent with existing `o.a.worker.transport.*` compile-scope rule
- pangrova-worker does NOT depend on pangrova-indexing directly; it accesses `IndexStrategyProvider` via SPI (ServiceLoader), and `IndexStorageManager`/`IndexCache` are injected at bootstrap

**Key constraint**: `VectorDistanceCalculator` is in `o.a.indexing.engine.vector`, NOT in pangrova-common. pangrova-common cannot reference `DistanceMetric` (which lives in pangrova-spi), and distance calculation is index-domain logic, not a shared utility.


## 12. Package Structure

### 12.1 Package File Count Audit

Governance rule: ≤8 healthy, 9-15 review, >15 mandatory split, >25 blocks merge.

**Existing packages (current state, before vector additions)**:

| Package | Current Files | Status |
|---------|--------------|--------|
| `o.a.spi.index` | 8 | ✅ Healthy |
| `o.a.spi.index.data` | 16 | ⚠️ Over 15 — mandatory split zone |
| `o.a.spi.index.predicate` | 13 | 🔍 Review zone |
| `o.a.spi.connector.index` | 11 | 🔍 Review zone |

**After vector additions**:

| Package | Added Files | New Total | Status | Action |
|---------|------------|-----------|--------|--------|
| `o.a.spi.index` | +3 (`DistanceMetric`, `VectorSearchRequest`, `VectorSearchResult`) | 11 | 🔍 Review zone | Acceptable — all are index-domain value objects |
| `o.a.spi.index.data` | +1 (`VectorIndexData` with inner `VectorAlgorithmType`) | 17 | ⚠️ Mandatory split | See §12.2 |
| `o.a.spi.index.predicate` | 0 (VectorSearchRequest is NOT a predicate) | 13 | 🔍 No change | — |
| `o.a.spi.connector.index` | 0 | 11 | 🔍 No change | — |
| `o.a.spi.connector.data.type` | +1 (`VectorType`) | +1 | ✅ | — |

**New packages**:

| Package | Files | Status |
|---------|-------|--------|
| `o.a.indexing.engine.vector` | 5 (`VectorDistanceCalculator`, `VectorBuildMemoryEstimator`, `VectorStrategyProvider`, `HnswEngine`, `IvfpqEngine`) | ✅ Healthy |
| `o.a.planner.vector` | 4 (`VectorSearchRewriter`, `VectorSearchNode`, `VectorHybridSearchPlanner`, `VectorHybridSearchStrategy`) | ✅ Healthy |

### 12.2 o.a.spi.index.data Split Plan

`o.a.spi.index.data` is already at 16 files and adding `VectorIndexData` pushes it to 17. This package must be split.

**Split strategy**: Group by index category.

| Sub-package | Files | Responsibility |
|-------------|-------|---------------|
| `o.a.spi.index.data` | `IndexData` (sealed interface), `BTreeIndexData`, `BTreePage`, `BloomFilterIndexData`, `HashIndexData` | Core data interface + equality/range index data |
| `o.a.spi.index.data.text` | `InvertedIndexData`, `NGramBloomIndexData`, `GinIndexData`, `PrefixIndexData`, `PrefixEntry` | Text-oriented index data |
| `o.a.spi.index.data.spatial` | `RTreeIndexData`, `RTreeNode`, `BoundingBox`, `SparseIndexData`, `SparseEntry` | Spatial and sparse index data |
| `o.a.spi.index.data.bitmap` | `BitmapIndexData` | Bitmap index data |
| `o.a.spi.index.data.vector` | `VectorIndexData`, `VectorAlgorithmType` | Vector ANN index data |

Post-split counts: `data` = 5, `data.text` = 5, `data.spatial` = 5, `data.bitmap` = 1, `data.vector` = 2. All ≤8.

> **Note**: `data.bitmap` has only 1 file — this is acceptable because BitmapIndexData is semantically distinct and the sub-package prevents the parent from re-bloating. If future bitmap variants are added (e.g., RoaringBitmapIndexData), they naturally land here.

**Sealed interface update**: `IndexData` permits clause must reference the new sub-package locations. This is a source-compatible change (no API break, only import paths change).

### 12.2.1 o.a.indexing.serde Split Plan

`o.a.indexing.serde` is already at 14 files. Adding `HnswSerializer` + `IvfpqSerializer` pushes it to 16 (mandatory split zone).

**Split strategy**: Extract vector serializers into a sub-package.

| Sub-package | Files | Responsibility |
|-------------|-------|---------------|
| `o.a.indexing.serde` | 14 existing files (IndexSerializer, SerdeCodec, TypeSerializer, TypeSerializerRegistry, + 10 type-specific serializers) | Traditional index serialization |
| `o.a.indexing.serde.vector` | `HnswSerializer`, `IvfpqSerializer` | Vector index serialization |

Post-split: `serde` = 14 (review zone, no new additions), `serde.vector` = 2. The parent package is already at review zone but not growing further.

### 12.2.2 o.a.coordinator.query.execution Note

`o.a.coordinator.query.execution` is at 14 files. Adding `VectorResultMerger` brings it to 15 — the review zone upper bound. No split required now, but future additions to this package should trigger a split review.

### 12.3 Full Package Map (Vector-Related)

```
pangrova-spi/
  o.a.spi.connector.data.type/
    VectorType.java                    (new)
    PangrovaType.java                    (modified: +VECTOR)
  o.a.spi.index/
    DistanceMetric.java                (new)
    VectorSearchRequest.java           (new)
    VectorSearchResult.java            (new)
    IndexType.java                     (modified: +VECTOR)
    IndexStrategyProvider.java         (modified: +vectorSearch default methods)
    IndexDescriptor.java               (existing)
    IndexEvalResult.java               (existing)
    IndexState.java                    (existing)
    IndexCoverageWindow.java           (existing)
    IndexCoverageStats.java            (existing)
    IndexCostEstimate.java             (existing)
  o.a.spi.index.data/
    IndexData.java                     (modified: +VectorIndexData permit)
    ... (existing permits, post-split)
  o.a.spi.index.data.vector/
    VectorIndexData.java               (new)

pangrova-parser/
  o.a.parser.ast.type/
    (ANTLR grammar changes for VECTOR type)
  o.a.parser.ast.statement/
    (CREATE INDEX ... USING VECTOR support)

pangrova-planner/
  o.a.planner.vector/
    VectorSearchRewriter.java          (new)
    VectorSearchNode.java              (new)
    VectorHybridSearchPlanner.java     (new)
    VectorHybridSearchStrategy.java    (new)

pangrova-indexing/
  o.a.indexing.engine.vector/
    VectorDistanceCalculator.java      (new)
    VectorBuildMemoryEstimator.java    (new)
    VectorStrategyProvider.java        (new — public, registered via ServiceLoader)
    HnswEngine.java                    (new — package-private, HNSW algorithm impl)
    IvfpqEngine.java                   (new — package-private, IVFPQ algorithm impl)

pangrova-coordinator/
  o.a.coordinator.query.execution/
    VectorResultMerger.java            (new)

pangrova-worker/
  o.a.worker.execution/
    VectorSearchExecutor.java          (new)

pangrova-protocol/
  protobuf: vector_search.proto        (new: VectorSearchTask, VectorValue messages)
```


## 13. Configuration

### 13.1 VectorIndexConfig (Split into Nested Records)

Original design had 14 constructor parameters — violates the max-4 constructor deps rule. Split into nested immutable records.

```java
package org.pangrova.indexing.config;

/**
 * Configuration for vector index build and search.
 * Immutable, loaded from YAML config.
 *
 * @param hnsw     HNSW algorithm parameters
 * @param ivfpq    IVFPQ algorithm parameters
 * @param hybrid   hybrid search strategy parameters
 * @param build    build-time resource limits
 */
public record VectorIndexConfig(
        HnswConfig hnsw,
        IvfpqConfig ivfpq,
        HybridConfig hybrid,
        BuildConfig build) {

    /**
     * HNSW algorithm configuration.
     *
     * @param defaultM              max connections per node per layer (default 16)
     * @param defaultEfConstruction dynamic candidate list size during build (default 200)
     * @param defaultEfSearch       dynamic candidate list size during search (default 64)
     */
    public record HnswConfig(
            int defaultM,
            int defaultEfConstruction,
            int defaultEfSearch) {

        public HnswConfig {
            if (defaultM < 4 || defaultM > 64)
                throw new IllegalArgumentException("M must be in [4, 64]");
            if (defaultEfConstruction < defaultM || defaultEfConstruction > 4096)
                throw new IllegalArgumentException("efConstruction must be in [M, 4096]");
            if (defaultEfSearch < 1 || defaultEfSearch > 4096)
                throw new IllegalArgumentException("efSearch must be in [1, 4096]");
        }

        public static HnswConfig defaults() {
            return new HnswConfig(16, 200, 64);
        }
    }

    /**
     * IVFPQ algorithm configuration.
     *
     * @param defaultNlist  number of IVF clusters (default 256)
     * @param defaultNprobe number of clusters to search (default 16)
     * @param defaultM      number of PQ sub-quantizers (default 8)
     * @param defaultNbits  bits per sub-quantizer code (default 8)
     */
    public record IvfpqConfig(
            int defaultNlist,
            int defaultNprobe,
            int defaultM,
            int defaultNbits) {

        public IvfpqConfig {
            if (defaultNlist < 1 || defaultNlist > 65536)
                throw new IllegalArgumentException("nlist must be in [1, 65536]");
            if (defaultNprobe < 1 || defaultNprobe > defaultNlist)
                throw new IllegalArgumentException("nprobe must be in [1, nlist]");
            if (defaultNbits != 4 && defaultNbits != 8 && defaultNbits != 16)
                throw new IllegalArgumentException("nbits must be 4, 8, or 16");
        }

        public static IvfpqConfig defaults() {
            return new IvfpqConfig(256, 16, 8, 8);
        }
    }

    /**
     * Hybrid search strategy configuration.
     *
     * @param annFirstSelectivityThreshold selectivity above which ANN_THEN_FILTER is used (default 0.5)
     * @param overFetchMultiplier          multiplier for over-fetching in ANN_THEN_FILTER (default 1.5)
     */
    public record HybridConfig(
            double annFirstSelectivityThreshold,
            double overFetchMultiplier) {

        public HybridConfig {
            if (annFirstSelectivityThreshold <= 0 || annFirstSelectivityThreshold >= 1)
                throw new IllegalArgumentException("threshold must be in (0, 1)");
            if (overFetchMultiplier < 1.0)
                throw new IllegalArgumentException("overFetchMultiplier must be >= 1.0");
        }

        public static HybridConfig defaults() {
            return new HybridConfig(0.5, 1.5);
        }
    }

    /**
     * Build-time resource limits.
     *
     * @param maxMemoryBytes          max memory per vector index build (default 2GB)
     * @param bruteForceFallbackEnabled whether brute-force fallback is enabled for uncovered files (default true)
     */
    public record BuildConfig(
            long maxMemoryBytes,
            boolean bruteForceFallbackEnabled) {

        public BuildConfig {
            if (maxMemoryBytes < 1024 * 1024)
                throw new IllegalArgumentException("maxMemoryBytes must be >= 1MB");
        }

        public static BuildConfig defaults() {
            return new BuildConfig(2L * 1024 * 1024 * 1024, true);
        }
    }

    public static VectorIndexConfig defaults() {
        return new VectorIndexConfig(
                HnswConfig.defaults(),
                IvfpqConfig.defaults(),
                HybridConfig.defaults(),
                BuildConfig.defaults());
    }
}
```

### 13.2 YAML Configuration Example

```yaml
pangrova:
  indexing:
    vector:
      hnsw:
        default-m: 16
        default-ef-construction: 200
        default-ef-search: 64
      ivfpq:
        default-nlist: 256
        default-nprobe: 16
        default-m: 8
        default-nbits: 8
      hybrid:
        ann-first-selectivity-threshold: 0.5
        over-fetch-multiplier: 1.5
      build:
        max-memory-bytes: 2147483648
        brute-force-fallback-enabled: true
```


## 14. Correctness Properties

These properties define the invariants that must hold for vector index correctness. Each property maps to one or more test cases.

### 14.1 Type System Properties (VP-01 to VP-05)

| ID | Property | Invariant |
|----|----------|-----------|
| VP-01 | Dimension bounds | `VectorType(d)` succeeds iff `1 ≤ d ≤ 65535` |
| VP-02 | Dimension immutability | Once a VECTOR column is created, its dimension cannot be altered |
| VP-03 | Dimension mismatch rejection | INSERT/UPDATE with wrong dimension throws `DimensionMismatchException` |
| VP-04 | NULL vector allowed | NULL embedding is valid; excluded from index build and ANN search |
| VP-05 | NaN/Infinity rejection | Vector elements containing NaN or Infinity are rejected during index build |

### 14.2 Distance Function Properties (VP-06 to VP-10)

| ID | Property | Invariant |
|----|----------|-----------|
| VP-06 | L2 non-negativity | `l2Distance(a, b) ≥ 0` for all valid vectors a, b |
| VP-07 | L2 identity | `l2Distance(a, a) == 0` for all valid vectors a |
| VP-08 | L2 symmetry | `l2Distance(a, b) == l2Distance(b, a)` |
| VP-09 | Cosine range | `0 ≤ cosineDistance(a, b) ≤ 2` for all non-zero vectors |
| VP-10 | Inner product consistency | `innerProduct(a, b) == -dot(a, b)` |

### 14.3 Index Build Properties (VP-11 to VP-16)

| ID | Property | Invariant |
|----|----------|-----------|
| VP-11 | Build determinism | Same input vectors + same parameters → same index structure (modulo random seed) |
| VP-12 | Empty file handling | Data file with 0 vectors → no index built, no error |
| VP-13 | Single vector handling | Data file with 1 vector → valid index, search returns that vector for any query |
| VP-14 | Memory budget enforcement | Build rejects if estimated peak memory exceeds `maxMemoryBytes` |
| VP-15 | IVFPQ dimension divisibility | IVFPQ build rejects if `dimension % m != 0` |
| VP-16 | Serialization round-trip | `deserialize(serialize(indexData)) == indexData` for all valid index data |

### 14.4 Search Properties (VP-17 to VP-22)

| ID | Property | Invariant |
|----|----------|-----------|
| VP-17 | Top-K count | `vectorSearch(request).size() ≤ request.topK()` |
| VP-18 | Distance ordering | Results are sorted by distance ascending (lower = more similar) |
| VP-19 | Self-search | Searching with a vector that exists in the index returns that vector at distance ≈ 0 |
| VP-20 | Brute-force equivalence | For small datasets (< 1000 vectors), ANN recall@K ≥ 95% vs brute-force |
| VP-21 | Metric consistency | Search metric must match index build metric; mismatch throws `MetricMismatchException` |
| VP-22 | ANN_THEN_FILTER recall caveat | ANN_THEN_FILTER recall ≤ base ANN recall; degradation is proportional to `(1 - selectivity)`. When selectivity < 0.1, FILTER_THEN_BRUTEFORCE is strictly preferred regardless of threshold config. This is an inherent limitation of post-filtering on approximate results. |

### 14.5 Integration Properties (VP-23 to VP-28)

| ID | Property | Invariant |
|----|----------|-----------|
| VP-23 | Global Top-K merge correctness | `merge(partials, K).size() == min(K, totalCandidates)` and all returned rows are globally top-K |
| VP-24 | Pre-filter composability | `WHERE clause + ORDER BY distance() LIMIT K` produces correct results: pre-filter reduces file set, ANN runs on survivors |
| VP-25 | Brute-force fallback activation | Files without VECTOR index in READY state trigger brute-force scan, not silent exclusion |
| VP-26 | Index coverage monotonicity | As more files get indexed, brute-force fallback frequency monotonically decreases |
| VP-27 | Compaction consistency | After file compaction, old vector indexes are GC'd and new files get new indexes |
| VP-28 | Concurrent build safety | Multiple concurrent vector index builds on different files do not interfere |


## 15. Testing Strategy

### 15.1 Property Tests (jqwik)

No mocks. All property tests use `*PropertyTest.java` naming and reside in `property` sub-packages.

| Test Class | Package | Properties Covered | Module |
|------------|---------|-------------------|--------|
| `VectorTypePropertyTest` | `o.a.spi.property.connector.data.type` | VP-01, VP-02 | pangrova-spi |
| `DistanceMetricPropertyTest` | `o.a.spi.property.index` | VP-06 to VP-10 | pangrova-spi |
| `VectorSearchRequestPropertyTest` | `o.a.spi.property.index` | Defensive copy, validation | pangrova-spi |
| `VectorIndexDataPropertyTest` | `o.a.spi.property.index.data.vector` | VP-16 (serialization round-trip) | pangrova-spi |
| `VectorDistanceCalculatorPropertyTest` | `o.a.indexing.property.engine.vector` | VP-06 to VP-10 (Java impl) | pangrova-indexing |
| `HnswEnginePropertyTest` | `o.a.indexing.property.engine.vector` | VP-11 to VP-14, VP-17 to VP-20 | pangrova-indexing |
| `IvfpqEnginePropertyTest` | `o.a.indexing.property.engine.vector` | VP-11 to VP-16, VP-17 to VP-20 | pangrova-indexing |
| `VectorResultMergerPropertyTest` | `o.a.coordinator.property.query.execution` | VP-23 | pangrova-coordinator |
| `VectorHybridSearchPropertyTest` | `o.a.planner.property.vector` | VP-22, VP-24 | pangrova-planner |

### 15.2 Unit Tests

| Test Class | Package | Focus | Module |
|------------|---------|-------|--------|
| `VectorTypeTest` | `o.a.spi.connector.data.type` | Constructor validation, display name, byte size | pangrova-spi |
| `VectorSearchRewriterTest` | `o.a.planner.vector` | ANN pattern detection, edge cases | pangrova-planner |
| `VectorBuildMemoryEstimatorTest` | `o.a.indexing.engine.vector` | Memory estimation accuracy | pangrova-indexing |
| `VectorSearchExecutorTest` | `o.a.worker.execution` | ANN path vs brute-force fallback routing | pangrova-worker |
| `VectorIndexConfigTest` | `o.a.indexing.config` | Config validation, defaults | pangrova-indexing |

### 15.3 Integration Tests

| Test Class | Package | Focus | Module |
|------------|---------|-------|--------|
| `VectorIndexBuildIntegrationTest` | `o.a.indexing.engine.vector` | End-to-end build: ColumnValueReader → build → serialize → deserialize → search | pangrova-indexing |
| `VectorSearchPipelineIntegrationTest` | `o.a.coordinator.query.execution` | Planner → Coordinator dispatch → Worker execution → result merge | pangrova-coordinator |

### 15.4 E2E Tests (TestContainers)

| Test Class | Package | Focus | Module |
|------------|---------|-------|--------|
| `VectorIndexE2ETest` | `o.a.testing.e2e.index` | Full SQL pipeline: CREATE TABLE with VECTOR → INSERT → CREATE INDEX → ANN query → verify results | pangrova-testing |
| `VectorHybridSearchE2ETest` | `o.a.testing.e2e.index` | Pre-filter + ANN combined query, both strategies | pangrova-testing |
| `VectorBruteForceFallbackE2ETest` | `o.a.testing.e2e.index` | Query before index build completes → brute-force fallback → correct results | pangrova-testing |

### 15.5 Native Tests (C++)

| Test File | Focus |
|-----------|-------|
| `VectorDistanceSimdTest.cpp` | SIMD distance functions: L2, cosine, inner product; correctness vs Java reference |
| `VectorScanOperatorTest.cpp` | Native vector scan operator: brute-force and indexed paths |


## 16. Phased Implementation Plan

### Phase 0: VECTOR Type System

| Task | Module | Files | Depends On |
|------|--------|-------|------------|
| T0.1 Add `VectorType` record | pangrova-spi | `VectorType.java` in `o.a.spi.connector.data.type` | — |
| T0.2 Add `VECTOR` to `PangrovaType` enum | pangrova-spi | `PangrovaType.java` | — |
| T0.3 `PangrovaTypeBridge` mapping (returns null; DDL parser constructs VectorType) | pangrova-spi | `PangrovaTypeBridge.java` | T0.1, T0.2 |
| T0.4 ANTLR grammar: `VECTOR '(' INT (',' (FLOAT\|DOUBLE))? ')'` | pangrova-parser | Grammar files | T0.1 |
| T0.5 Protobuf `VectorValue` message | pangrova-protocol | `vector_value.proto` | T0.1 |
| T0.6 Iceberg `fixed_size_list` ↔ `VectorType` conversion | pangrova-connector-iceberg | Parquet mapping | T0.1 |
| T0.7 Paimon ORC `list` + metadata ↔ `VectorType` conversion | pangrova-connector-paimon | ORC mapping | T0.1 |
| T0.8 Property tests for VectorType | pangrova-spi (test) | `VectorTypePropertyTest.java` | T0.1 |

### Phase 1: Distance Functions

| Task | Module | Files | Depends On |
|------|--------|-------|------------|
| T1.1 Add `DistanceMetric` enum | pangrova-spi | `DistanceMetric.java` in `o.a.spi.index` | — |
| T1.2 `VectorDistanceCalculator` (pure Java) | pangrova-indexing | `VectorDistanceCalculator.java` in `o.a.indexing.engine.vector` | T1.1 |
| T1.3 Distance SQL functions registration | pangrova-planner | Function registry | T1.1 |
| T1.4 C++ SIMD distance functions | pangrova-worker-native | `VectorDistance.cpp/.h` | T1.1 |
| T1.5 Property tests for distance functions | pangrova-spi, pangrova-indexing (test) | `DistanceMetricPropertyTest.java`, `VectorDistanceCalculatorPropertyTest.java` | T1.1, T1.2 |

### Phase 2: SPI Extension

| Task | Module | Files | Depends On |
|------|--------|-------|------------|
| T2.1 Add `VECTOR` to `IndexType` enum | pangrova-spi | `IndexType.java` | Phase 0 |
| T2.2 Add `VectorIndexData` record + `VectorAlgorithmType` | pangrova-spi | `VectorIndexData.java` in `o.a.spi.index.data.vector` | T2.1 |
| T2.3 Update `IndexData` sealed permits (add `VectorIndexData`) | pangrova-spi | `IndexData.java` | T2.2 |
| T2.4 Split `o.a.spi.index.data` package (§12.2) | pangrova-spi | Multiple files moved to sub-packages | T2.3 |
| T2.5 Add `VectorSearchRequest` record | pangrova-spi | `VectorSearchRequest.java` in `o.a.spi.index` | T1.1 |
| T2.6 Add `VectorSearchResult` record | pangrova-spi | `VectorSearchResult.java` in `o.a.spi.index` | — |
| T2.7 Extend `IndexStrategyProvider` with `vectorSearch()` default methods | pangrova-spi | `IndexStrategyProvider.java` | T2.5, T2.6 |
| T2.8 Protobuf `VectorSearchTask` message | pangrova-protocol | `vector_search.proto` | T2.5 |
| T2.9 Property tests for VectorSearchRequest, VectorIndexData | pangrova-spi (test) | `VectorSearchRequestPropertyTest.java`, `VectorIndexDataPropertyTest.java` | T2.5, T2.2 |

### Phase 3: Algorithm Implementation

| Task | Module | Files | Depends On |
|------|--------|-------|------------|
| T3.1 `VectorStrategyProvider` (single registered provider, delegates to HNSW/IVFPQ) | pangrova-indexing | `VectorStrategyProvider.java` in `o.a.indexing.engine.vector` | Phase 2 |
| T3.2 `HnswEngine` + `IvfpqEngine` (package-private algorithm implementations) | pangrova-indexing | `HnswEngine.java`, `IvfpqEngine.java` in `o.a.indexing.engine.vector` | Phase 2 |
| T3.3 HNSW serialization format | pangrova-indexing | `HnswSerializer.java` in `o.a.indexing.serde.vector` | T3.1 |
| T3.4 IVFPQ serialization format | pangrova-indexing | `IvfpqSerializer.java` in `o.a.indexing.serde.vector` | T3.2 |
| T3.5 `VectorBuildMemoryEstimator` | pangrova-indexing | `VectorBuildMemoryEstimator.java` in `o.a.indexing.engine.vector` | T3.1, T3.2 |
| T3.6 Register `VectorStrategyProvider` in `IndexEngineRegistry` | pangrova-indexing | ServiceLoader config | T3.1 |
| T3.7 `VectorIndexConfig` (nested records) | pangrova-indexing | `VectorIndexConfig.java` in `o.a.indexing.config` | — |
| T3.8 Property tests for HNSW and IVFPQ | pangrova-indexing (test) | `HnswEnginePropertyTest.java`, `IvfpqEnginePropertyTest.java` | T3.1, T3.2 |
| T3.9 Integration test: build → serialize → deserialize → search | pangrova-indexing (test) | `VectorIndexBuildIntegrationTest.java` | T3.1-T3.4 |

### Phase 4: Query Integration

| Task | Module | Files | Depends On |
|------|--------|-------|------------|
| T4.1 `VectorSearchRewriter` (planner ANN pattern detection) | pangrova-planner | `VectorSearchRewriter.java` in `o.a.planner.vector` | Phase 2 |
| T4.2 `VectorSearchNode` (logical plan node) | pangrova-planner | `VectorSearchNode.java` in `o.a.planner.vector` | T4.1 |
| T4.3 `VectorHybridSearchPlanner` + `VectorHybridSearchStrategy` | pangrova-planner | 2 files in `o.a.planner.vector` | T4.1 |
| T4.4 `VectorResultMerger` (coordinator result merge) | pangrova-coordinator | `VectorResultMerger.java` in `o.a.coordinator.query.execution` | Phase 2 |
| T4.5 `VectorSearchExecutor` (worker execution) | pangrova-worker | `VectorSearchExecutor.java` in `o.a.worker.execution` | Phase 2, Phase 3 |
| T4.6 Brute-force fallback in `VectorSearchExecutor` | pangrova-worker | Same file as T4.5 | T4.5 |
| T4.7 C++ vector scan operator (native execution) | pangrova-worker-native | `VectorScanOperator.cpp/.h` | T1.4 |
| T4.8 DDL support: `CREATE INDEX ... USING VECTOR` | pangrova-coordinator | `IndexDdlExecutor.java` (modify) | Phase 2 |
| T4.9 Property tests for VectorResultMerger, VectorHybridSearch | pangrova-coordinator, pangrova-planner (test) | `VectorResultMergerPropertyTest.java`, `VectorHybridSearchPropertyTest.java` | T4.3, T4.4 |
| T4.10 E2E tests | pangrova-testing | `VectorIndexE2ETest.java`, `VectorHybridSearchE2ETest.java`, `VectorBruteForceFallbackE2ETest.java` | All phases |

### Implementation Priority

```
Phase 0 (Type System)     ████████░░  ~2 weeks
Phase 1 (Distance)        ████░░░░░░  ~1 week
Phase 2 (SPI Extension)   ████████░░  ~2 weeks
Phase 3 (Algorithms)      ████████████████  ~4 weeks (HNSW first, IVFPQ second)
Phase 4 (Integration)     ████████████████  ~4 weeks (planner → coordinator → worker → E2E)
                                            Total: ~13 weeks
```

> Phase 3 and Phase 4 can partially overlap: T4.1-T4.3 (planner) can start once Phase 2 is complete, while T3.1-T3.4 (algorithms) are still in progress.
