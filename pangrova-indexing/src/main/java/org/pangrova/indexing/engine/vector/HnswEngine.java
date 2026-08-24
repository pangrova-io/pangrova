package org.pangrova.indexing.engine.vector;

import org.pangrova.spi.index.data.vector.VectorIndexData;
import org.pangrova.spi.index.data.vector.VectorIndexData.VectorAlgorithmType;
import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.FilteredSearchStrategy;
import org.pangrova.spi.index.vector.VectorSearchRequest;
import org.pangrova.spi.index.vector.VectorSearchResult;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.IntStream;

import static org.pangrova.indexing.engine.vector.HnswGraphOperations.buildGraph;
import static org.pangrova.indexing.engine.vector.HnswGraphOperations.clamp;
import static org.pangrova.indexing.engine.vector.HnswGraphOperations.distance;
import static org.pangrova.indexing.engine.vector.HnswGraphOperations.greedyClosestMapped;
import static org.pangrova.indexing.engine.vector.HnswGraphOperations.insertNodeBounded;
import static org.pangrova.indexing.engine.vector.HnswGraphOperations.insertNodeFromLevel;
import static org.pangrova.indexing.engine.vector.HnswGraphOperations.intProp;
import static org.pangrova.indexing.engine.vector.HnswGraphOperations.randomLevel;
import static org.pangrova.indexing.engine.vector.HnswGraphOperations.searchLayerMapped;

/**
 * HNSW (Hierarchical Navigable Small World) graph algorithm engine.
 *
 * <p>Package-private: accessed only through {@link VectorStrategyProvider}.
 * Multi-layer graph construction with greedy descent and neighbor selection.</p>
 *
 * <p>Core graph construction and layer-search primitives are in
 * {@link HnswGraphOperations}.</p>
 *
 * <p>Parameters:
 * <ul>
 *   <li>{@code hnsw.M} — max connections per node per layer, range [4, 64], default 16</li>
 *   <li>{@code hnsw.efConstruction} — build-time candidate list size, range [M, 4096], default 200</li>
 *   <li>{@code hnsw.efSearch} — search-time candidate list size, range [topK, 4096], default 64</li>
 * </ul>
 *
 * <p>Known limitations:
 * <ul>
 *   <li>Higher-level (>=1) graph construction is serial due to shared entry-point
 *       mutation. Level-0 insertion is parallelized for datasets > 1000 vectors,
 *       giving ~3-4x build speedup on multi-core machines.</li>
 * </ul>
 */
final class HnswEngine {

    private static final String PROP_M = "hnsw.M";
    private static final String PROP_EF_CONSTRUCTION = "hnsw.efConstruction";
    private static final String PROP_EF_SEARCH = "hnsw.efSearch";
    private static final int DEFAULT_M = 16;
    private static final int DEFAULT_EF_CONSTRUCTION = 200;
    private static final int DEFAULT_EF_SEARCH = 64;
    private static final int MIN_M = 4;
    private static final int MAX_M = 64;
    private static final int MAX_EF = 4096;

    /**
     * Incrementally inserts {@code newVectors} into an existing HNSW graph.
     *
     * <p>Layered insertion strategy:
     * <ul>
     *   <li>Level-0 connections are inserted in parallel: each new node's
     *       searchLayer at level 0 only reads existing-node state and writes
     *       its own adjacency, so concurrency is safe.</li>
     *   <li>Higher-level (>=1) connections are inserted serially because
     *       bidirectional connect + shrink mutates shared adjacency lists,
     *       and the entry point may move when a new node exceeds the current
     *       maxLevel.</li>
     * </ul>
     *
     * <p>Replaces the previous {@link UnsupportedOperationException} path
     * in {@code VectorStrategyProvider.buildIncremental} for HNSW.</p>
     */
    VectorIndexData buildIncremental(VectorIndexData existing, List<float[]> newVectors,
                                     Map<String, String> properties) {
        if (newVectors.isEmpty()) {
            return existing;
        }
        int dimension = existing.dimension();
        requireMatchingDimension(newVectors, dimension);
        int m = clamp(intProp(properties, PROP_M, DEFAULT_M), MIN_M, MAX_M);
        int efConstruction = clamp(intProp(properties, PROP_EF_CONSTRUCTION,
                DEFAULT_EF_CONSTRUCTION), m, MAX_EF);
        int maxM0 = 2 * m;
        double mL = 1.0 / Math.log(m);
        DistanceMetric metric = existing.metric();

        HnswGraph graph = HnswGraphCodec.deserialize(existing.indexPayload());
        HnswGraphOperations.InsertionParams params =
                new HnswGraphOperations.InsertionParams(graph, metric, m, maxM0, efConstruction);
        int baseId = graph.size();
        // Incremental nodes continue the nodeId sequence as their reported row
        // offset, preserving the pre-existing incremental semantics. Full
        // (non-incremental) builds carry true physical file offsets via build().
        long[] newRowOffsets = new long[newVectors.size()];
        for (int i = 0; i < newRowOffsets.length; i++) {
            newRowOffsets[i] = baseId + i;
        }
        graph.appendVectors(newVectors, newRowOffsets);
        int totalNodeCount = graph.size();

        int[] newLevels = new int[newVectors.size()];
        for (int i = 0; i < newVectors.size(); i++) {
            newLevels[i] = randomLevel(mL);
            graph.ensureLevel(baseId + i, newLevels[i]);
        }

        insertLevelZero(params, baseId, newVectors);
        insertHigherLevels(params, baseId, newVectors, newLevels);

        byte[] payload = HnswGraphCodec.serialize(graph, dimension, metric, m, efConstruction);
        return new VectorIndexData(VectorAlgorithmType.HNSW, dimension, metric,
                totalNodeCount, payload, Map.copyOf(properties));
    }

    private static void requireMatchingDimension(List<float[]> vectors, int dimension) {
        for (float[] v : vectors) {
            if (v.length != dimension) {
                throw new IllegalArgumentException(
                        "Incremental vector dimension " + v.length
                                + " does not match existing index dimension " + dimension);
            }
        }
    }

    /**
     * Inserts all new nodes at level 0. Parallel for large batches: safe because
     * HnswGraph operations are synchronized and each node's level-0 searchLayer
     * only reads existing-node state while writing its own adjacency. For large
     * batches, parallelism provides ~3x speedup on multi-core machines.
     */
    private static void insertLevelZero(HnswGraphOperations.InsertionParams params,
                                        int baseId, List<float[]> newVectors) {
        int count = newVectors.size();
        if (count > HnswGraphOperations.PARALLEL_THRESHOLD) {
            try {
                HnswGraphOperations.BUILD_POOL.submit(() ->
                    IntStream.range(0, count).parallel().forEach(i ->
                        insertNodeBounded(baseId + i, newVectors.get(i), 0, params))
                ).get();
            } catch (Exception e) {
                throw new RuntimeException("Parallel HNSW incremental level-0 build failed", e);
            }
            return;
        }
        for (int i = 0; i < count; i++) {
            insertNodeBounded(baseId + i, newVectors.get(i), 0, params);
        }
    }

    /** Inserts new nodes into levels >= 1 serially (shared adjacency + entry point). */
    private static void insertHigherLevels(HnswGraphOperations.InsertionParams params,
                                           int baseId, List<float[]> newVectors, int[] newLevels) {
        for (int i = 0; i < newVectors.size(); i++) {
            int level = newLevels[i];
            if (level < 1) {
                continue;
            }
            insertNodeFromLevel(baseId + i, newVectors.get(i), level, 1, params);
        }
    }

    /**
     * Builds an HNSW index from the given vectors.
     *
     * @return VectorIndexData with serialized graph in indexPayload
     */
    VectorIndexData build(List<float[]> vectors, long[] rowOffsets, int dimension,
                          DistanceMetric metric, Map<String, String> properties) {
        if (vectors.isEmpty()) {
            return emptyIndex(dimension, metric, properties);
        }
        int m = clamp(intProp(properties, PROP_M, DEFAULT_M), MIN_M, MAX_M);
        int efConstruction = clamp(intProp(properties, PROP_EF_CONSTRUCTION, DEFAULT_EF_CONSTRUCTION), m, MAX_EF);

        HnswGraph graph = buildGraph(vectors, rowOffsets, dimension, metric, m, efConstruction);
        byte[] payload = HnswGraphCodec.serialize(graph, dimension, metric, m, efConstruction);
        return new VectorIndexData(VectorAlgorithmType.HNSW, dimension, metric,
                vectors.size(), payload, Map.copyOf(properties));
    }

    /**
     * Searches the HNSW index for the top-K nearest neighbors.
     *
     * <p>Uses memory-mapped incremental loading: only the entry point and
     * level-0 adjacency are loaded eagerly. Deeper levels and vectors are
     * accessed on demand from the OS page cache, avoiding full deserialization.</p>
     */
    List<VectorSearchResult.ScoredRow> search(VectorIndexData indexData, VectorSearchRequest request,
                                               Map<String, String> searchParams) {
        int efSearch = clamp(intProp(searchParams, PROP_EF_SEARCH, DEFAULT_EF_SEARCH),
                request.topK(), MAX_EF);
        byte[] payload = indexData.indexPayload();
        if (payload.length == 0) {
            return List.of();
        }
        try {
            MappedHnswGraph graph = MappedHnswGraph.fromPayload(payload);
            if (graph.nodeCount() == 0) {
                return List.of();
            }
            return searchMappedGraph(graph, request.queryVector(), request.topK(),
                    efSearch, indexData.metric());
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to open memory-mapped HNSW index", e);
        }
    }

    /**
     * Filtered vector search: applies pre-filter, iterative-scan, or predicate-aware
     * strategy to combine ANN search with a row-level allow-set.
     *
     * <p>When {@code allowedRows} is null or the strategy is POST_FILTER,
     * falls back to the standard unfiltered search.</p>
     */
    List<VectorSearchResult.ScoredRow> search(VectorIndexData indexData, VectorSearchRequest request,
                                               Map<String, String> searchParams,
                                               Set<Long> allowedRows) {
        if (allowedRows == null || allowedRows.isEmpty()) {
            return search(indexData, request, searchParams);
        }

        int efSearch = clamp(intProp(searchParams, PROP_EF_SEARCH, DEFAULT_EF_SEARCH),
                request.topK(), MAX_EF);
        byte[] payload = indexData.indexPayload();
        if (payload.length == 0) {
            return List.of();
        }

        FilteredSearchStrategy strategy = request.effectiveFilteredSearchStrategy();
        if (strategy == FilteredSearchStrategy.POST_FILTER) {
            return search(indexData, request, searchParams);
        }

        try {
            MappedHnswGraph graph = MappedHnswGraph.fromPayload(payload);
            if (graph.nodeCount() == 0) {
                return List.of();
            }
            return switch (strategy) {
                case PRE_FILTER -> searchWithPreFilter(
                        graph, request, efSearch, indexData.metric(), allowedRows);
                case ITERATIVE_SCAN -> searchIterative(
                        graph, request, indexData.metric(), allowedRows, searchParams);
                case PREDICATE_AWARE -> searchPredicateAware(
                        graph, request, efSearch, indexData.metric(), allowedRows);
                case POST_FILTER -> searchMappedGraph(
                        graph, request.queryVector(), request.topK(), efSearch, indexData.metric());
            };
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to open memory-mapped HNSW index for filtered search", e);
        }
    }

    // --- Filtered search strategies ---

    private List<VectorSearchResult.ScoredRow> searchWithPreFilter(
            MappedHnswGraph graph, VectorSearchRequest request, int efSearch,
            DistanceMetric metric, Set<Long> allowedRows) {
        int overFetchK = Math.min(request.topK() * 4, graph.nodeCount());
        int ef = Math.max(efSearch, overFetchK);
        List<VectorSearchResult.ScoredRow> candidates = searchMappedGraph(
                graph, request.queryVector(), overFetchK, ef, metric);
        List<VectorSearchResult.ScoredRow> filtered = new ArrayList<>();
        for (VectorSearchResult.ScoredRow row : candidates) {
            if (allowedRows.contains(row.rowOffset())) {
                filtered.add(row);
                if (filtered.size() >= request.topK()) break;
            }
        }
        return filtered;
    }

    private List<VectorSearchResult.ScoredRow> searchIterative(
            MappedHnswGraph graph, VectorSearchRequest request,
            DistanceMetric metric, Set<Long> allowedRows,
            Map<String, String> searchParams) {
        int baseEf = clamp(intProp(searchParams, PROP_EF_SEARCH, DEFAULT_EF_SEARCH),
                request.topK(), MAX_EF);
        List<VectorSearchResult.ScoredRow> collected = new ArrayList<>();
        Set<Long> seen = new HashSet<>();

        for (int attempt = 0; attempt < 3 && collected.size() < request.topK(); attempt++) {
            int ef = Math.min(baseEf * (1 << attempt), MAX_EF);
            int fetchK = Math.min(request.topK() * (2 << attempt), graph.nodeCount());
            List<VectorSearchResult.ScoredRow> candidates = searchMappedGraph(
                    graph, request.queryVector(), fetchK, ef, metric);
            for (VectorSearchResult.ScoredRow row : candidates) {
                if (seen.add(row.rowOffset()) && allowedRows.contains(row.rowOffset())) {
                    collected.add(row);
                    if (collected.size() >= request.topK()) break;
                }
            }
        }
        collected.sort(Comparator.comparingDouble(VectorSearchResult.ScoredRow::distance));
        return collected.size() <= request.topK()
                ? collected
                : collected.subList(0, request.topK());
    }

    private List<VectorSearchResult.ScoredRow> searchPredicateAware(
            MappedHnswGraph graph, VectorSearchRequest request, int efSearch,
            DistanceMetric metric, Set<Long> allowedRows) {
        int ef = Math.max(efSearch, request.topK() * 2);
        List<HnswGraphOperations.ScoredNode> candidates = searchLayerFilteredMapped(
                graph, request.queryVector(), ef, metric, allowedRows);
        List<VectorSearchResult.ScoredRow> results = new ArrayList<>();
        for (HnswGraphOperations.ScoredNode sn : candidates) {
            results.add(new VectorSearchResult.ScoredRow(graph.rowOffset(sn.nodeId()), sn.distance()));
            if (results.size() >= request.topK()) break;
        }
        return results;
    }

    private static List<HnswGraphOperations.ScoredNode> searchLayerFilteredMapped(
            MappedHnswGraph graph, float[] query, int ef,
            DistanceMetric metric, Set<Long> allowedRows) {
        if (graph.nodeCount() == 0) {
            return List.of();
        }
        FilteredSearchState state = new FilteredSearchState(graph, query, ef, metric, allowedRows);
        seedFilteredEntry(state);
        while (!state.candidates.isEmpty()) {
            HnswGraphOperations.ScoredNode current = state.candidates.poll();
            if (shouldStopFilteredSearch(state, current)) {
                break;
            }
            processFilteredNeighbors(state, current);
        }

        List<HnswGraphOperations.ScoredNode> sorted = new ArrayList<>(state.results);
        sorted.sort(Comparator.comparingDouble(HnswGraphOperations.ScoredNode::distance));
        return sorted;
    }

    /** Mutable working state for filtered layer-0 search. */
    private static final class FilteredSearchState {
        final MappedHnswGraph graph;
        final float[] query;
        final int ef;
        final DistanceMetric metric;
        final Set<Long> allowedRows;
        final PriorityQueue<HnswGraphOperations.ScoredNode> candidates = new PriorityQueue<>(
                Comparator.comparingDouble(HnswGraphOperations.ScoredNode::distance));
        final PriorityQueue<HnswGraphOperations.ScoredNode> results = new PriorityQueue<>(
                Comparator.comparingDouble((HnswGraphOperations.ScoredNode sn) -> sn.distance())
                        .reversed());
        final Set<Integer> visited = new HashSet<>();

        FilteredSearchState(MappedHnswGraph graph, float[] query, int ef,
                            DistanceMetric metric, Set<Long> allowedRows) {
            this.graph = graph;
            this.query = query;
            this.ef = ef;
            this.metric = metric;
            this.allowedRows = allowedRows;
        }
    }

    private static void seedFilteredEntry(FilteredSearchState state) {
        int entryId = 0;
        double entryDist = distance(state.query, state.graph.vector(entryId), state.metric);
        state.candidates.add(new HnswGraphOperations.ScoredNode(entryId, entryDist));
        state.visited.add(entryId);
        if (state.allowedRows.contains(state.graph.rowOffset(entryId))) {
            state.results.add(new HnswGraphOperations.ScoredNode(entryId, entryDist));
        }
    }

    private static boolean shouldStopFilteredSearch(FilteredSearchState state,
                                                    HnswGraphOperations.ScoredNode current) {
        return state.results.size() >= state.ef
                && current.distance() > state.results.peek().distance();
    }

    private static void processFilteredNeighbors(FilteredSearchState state,
                                                 HnswGraphOperations.ScoredNode current) {
        List<Integer> neighbors = state.graph.neighbors(current.nodeId(), 0);
        for (int neighborId : neighbors) {
            if (!isVisitableNeighbor(state, neighborId)) {
                continue;
            }
            double dist = distance(state.query, state.graph.vector(neighborId), state.metric);
            if (state.results.size() < state.ef || dist < state.results.peek().distance()) {
                state.candidates.add(new HnswGraphOperations.ScoredNode(neighborId, dist));
                addFilteredResult(state, neighborId, dist);
            }
        }
    }

    private static boolean isVisitableNeighbor(FilteredSearchState state, int neighborId) {
        return neighborId >= 0 && neighborId < state.graph.nodeCount()
                && state.visited.add(neighborId);
    }

    private static void addFilteredResult(FilteredSearchState state, int nodeId, double dist) {
        if (!state.allowedRows.contains(state.graph.rowOffset(nodeId))) {
            return;
        }
        state.results.add(new HnswGraphOperations.ScoredNode(nodeId, dist));
        if (state.results.size() > state.ef) {
            state.results.poll();
        }
    }

    // --- Mapped graph search entry point ---

    private static List<VectorSearchResult.ScoredRow> searchMappedGraph(
            MappedHnswGraph graph, float[] query, int topK, int efSearch,
            DistanceMetric metric) {
        int current = graph.entryPoint();
        for (int level = graph.maxLevel(); level >= 1; level--) {
            current = greedyClosestMapped(graph, current, query, level, metric);
        }
        List<HnswGraphOperations.ScoredNode> candidates = searchLayerMapped(
                graph, current, query, efSearch, 0, metric);
        int resultSize = Math.min(topK, candidates.size());
        List<VectorSearchResult.ScoredRow> results = new ArrayList<>(resultSize);
        for (int i = 0; i < resultSize; i++) {
            HnswGraphOperations.ScoredNode sn = candidates.get(i);
            results.add(new VectorSearchResult.ScoredRow(graph.rowOffset(sn.nodeId()), sn.distance()));
        }
        return results;
    }

    // --- Utility ---

    private static VectorIndexData emptyIndex(int dimension, DistanceMetric metric,
                                              Map<String, String> properties) {
        return new VectorIndexData(VectorAlgorithmType.HNSW, dimension, metric,
                0, new byte[0], Map.copyOf(properties));
    }
}
