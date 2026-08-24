package org.pangrova.indexing.engine.vector;

import org.pangrova.spi.index.vector.DistanceMetric;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

/**
 * Core HNSW graph construction, insertion, and layer search operations.
 *
 * <p>Extracted from {@link HnswEngine} to keep both classes under 500 lines.
 * All methods are static and stateless, operating on {@link HnswGraph} or
 * {@link MappedHnswGraph} instances passed as parameters.</p>
 */
final class HnswGraphOperations {

    private HnswGraphOperations() {}

    /** Distance-scored node reference used during graph construction and search. */
    record ScoredNode(int nodeId, double distance) {}

    /** Fixed tuning parameters shared by all node insertion operations. */
    record InsertionParams(HnswGraph graph, DistanceMetric metric,
                           int m, int maxM0, int efConstruction) {}

    /** Mutable priority-queue state shared by layer search expansion. */
    private static final class LayerSearchState {
        final Set<Integer> visited = new HashSet<>();
        final PriorityQueue<ScoredNode> candidates = new PriorityQueue<>(
                Comparator.comparingDouble(s -> s.distance));
        final PriorityQueue<ScoredNode> results = new PriorityQueue<>(
                Comparator.comparingDouble(s -> -s.distance));
    }

    /** Parallelism threshold: use parallel insertion for datasets > this size. */
    static final int PARALLEL_THRESHOLD = 1000;

    /** Dedicated ForkJoinPool for HNSW build to avoid starving the common pool. */
    static final ForkJoinPool BUILD_POOL = new ForkJoinPool(
            Math.max(2, Runtime.getRuntime().availableProcessors() - 1),
            pool -> {
                ForkJoinWorkerThread t =
                        ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                t.setName("hnsw-build-" + t.getPoolIndex());
                t.setDaemon(true);
                return t;
            }, null, true);

    /**
     * Hard cap on a node's random level. HNSW levels are geometrically
     * distributed, so exceeding ~30 is astronomically unlikely; the cap defends
     * against a pathological {@code mL} and bounds adjacency-list allocation.
     */
    static final int MAX_RANDOM_LEVEL = 62;

    // --- Graph construction ---

    static HnswGraph buildGraph(List<float[]> vectors, long[] rowOffsets, int dimension,
                                DistanceMetric metric, int m, int efConstruction) {
        int maxM0 = 2 * m;
        double mL = 1.0 / Math.log(m);
        HnswGraph graph = new HnswGraph(vectors, rowOffsets, dimension);
        InsertionParams params = new InsertionParams(graph, metric, m, maxM0, efConstruction);

        graph.setEntryPoint(0);
        graph.ensureLevel(0, 0);

        int n = vectors.size();

        if (n <= PARALLEL_THRESHOLD) {
            // Small graph: serial insertion (overhead of parallelism not worth it)
            for (int i = 1; i < n; i++) {
                int nodeLevel = randomLevel(mL);
                graph.ensureLevel(i, nodeLevel);
                insertNode(i, vectors.get(i), nodeLevel, params);
            }
            return graph;
        }

        // Seed graph: insert first 'seedSize' nodes serially to establish connectivity
        int seedSize = Math.min(2 * m, n);
        int[] levels = new int[n];
        for (int i = 1; i < seedSize; i++) {
            levels[i] = randomLevel(mL);
            graph.ensureLevel(i, levels[i]);
            insertNode(i, vectors.get(i), levels[i], params);
        }

        // Pre-compute levels and ensure level structures for remaining nodes
        for (int i = seedSize; i < n; i++) {
            levels[i] = randomLevel(mL);
            graph.ensureLevel(i, levels[i]);
        }

        // Phase 1: Parallel level-0 insertion for remaining nodes.
        // HnswGraph methods are synchronized, ensuring thread-safe concurrent access.
        // Slightly reduced graph quality vs serial (stale neighbor reads) — acceptable
        // trade-off for ~3-4x build speedup on multi-core machines.
        try {
            BUILD_POOL.submit(() ->
                IntStream.range(seedSize, n).parallel().forEach(i ->
                    insertNodeBounded(i, vectors.get(i), 0, params))
            ).get();
        } catch (Exception e) {
            throw new RuntimeException("Parallel HNSW level-0 build failed", e);
        }

        // Phase 2: Serial higher-level insertion (entry point may change, shared state)
        for (int i = seedSize; i < n; i++) {
            int level = levels[i];
            if (level >= 1) {
                insertNodeFromLevel(i, vectors.get(i), level, 1, params);
            }
        }

        return graph;
    }

    static void insertNode(int nodeId, float[] vector, int nodeLevel, InsertionParams params) {
        HnswGraph graph = params.graph();
        DistanceMetric metric = params.metric();
        int entryPoint = graph.entryPoint();
        int maxLevel = graph.maxLevel();

        int current = entryPoint;
        for (int level = maxLevel; level > nodeLevel; level--) {
            current = greedyClosest(graph, current, vector, level, metric);
        }

        for (int level = Math.min(nodeLevel, maxLevel); level >= 0; level--) {
            List<ScoredNode> candidates = searchLayer(
                    graph, current, vector, params.efConstruction(), level, metric);
            int maxConn = (level == 0) ? params.maxM0() : params.m();
            List<ScoredNode> neighbors = selectNeighborsDiverse(candidates, maxConn, graph, metric);
            connectBidirectional(graph, nodeId, neighbors, level, maxConn, metric);
            if (!candidates.isEmpty()) {
                current = candidates.get(0).nodeId;
            }
        }

        if (nodeLevel > maxLevel) {
            graph.setEntryPoint(nodeId);
        }
    }

    static void insertNodeBounded(int nodeId, float[] vector,
                                  int targetLevel, InsertionParams params) {
        HnswGraph graph = params.graph();
        DistanceMetric metric = params.metric();
        int entryPoint = graph.entryPoint();
        int maxLevel = graph.maxLevel();
        int current = entryPoint;
        for (int level = maxLevel; level > targetLevel; level--) {
            current = greedyClosest(graph, current, vector, level, metric);
        }
        List<ScoredNode> candidates = searchLayer(graph, current, vector,
                params.efConstruction(), targetLevel, metric);
        int maxConn = (targetLevel == 0) ? params.maxM0() : params.m();
        List<ScoredNode> neighbors = selectNeighborsDiverse(candidates, maxConn, graph, metric);
        connectBidirectional(graph, nodeId, neighbors, targetLevel, maxConn, metric);
    }

    static void insertNodeFromLevel(int nodeId, float[] vector,
                                    int targetLevel, int startLevel,
                                    InsertionParams params) {
        HnswGraph graph = params.graph();
        DistanceMetric metric = params.metric();
        int entryPoint = graph.entryPoint();
        int maxLevel = graph.maxLevel();
        int current = entryPoint;
        for (int level = maxLevel; level > targetLevel; level--) {
            current = greedyClosest(graph, current, vector, level, metric);
        }
        for (int level = Math.min(targetLevel, maxLevel); level >= startLevel; level--) {
            List<ScoredNode> candidates = searchLayer(graph, current, vector,
                    params.efConstruction(), level, metric);
            int maxConn = (level == 0) ? params.maxM0() : params.m();
            List<ScoredNode> neighbors = selectNeighborsDiverse(candidates, maxConn, graph, metric);
            connectBidirectional(graph, nodeId, neighbors, level, maxConn, metric);
            if (!candidates.isEmpty()) {
                current = candidates.get(0).nodeId;
            }
        }
        if (targetLevel > maxLevel) {
            graph.setEntryPoint(nodeId);
        }
    }

    // --- Layer operations ---

    static int greedyClosest(HnswGraph graph, int entryId, float[] query,
                              int level, DistanceMetric metric) {
        int current = entryId;
        double currentDist = distance(graph.vector(current), query, metric);
        boolean improved = true;
        while (improved) {
            improved = false;
            for (int neighbor : graph.neighbors(current, level)) {
                double d = distance(graph.vector(neighbor), query, metric);
                if (d < currentDist) {
                    current = neighbor;
                    currentDist = d;
                    improved = true;
                }
            }
        }
        return current;
    }

    static List<ScoredNode> searchLayer(HnswGraph graph, int entryId, float[] query,
                                         int ef, int level, DistanceMetric metric) {
        LayerSearchState state = new LayerSearchState();

        double entryDist = distance(graph.vector(entryId), query, metric);
        ScoredNode entrySn = new ScoredNode(entryId, entryDist);
        state.candidates.add(entrySn);
        state.results.add(entrySn);
        state.visited.add(entryId);

        expandCandidates(graph, query, ef, level, metric, state);

        List<ScoredNode> sorted = new ArrayList<>(state.results);
        sorted.sort(Comparator.comparingDouble(s -> s.distance));
        return sorted;
    }

    private static void expandCandidates(HnswGraph graph, float[] query, int ef, int level,
                                          DistanceMetric metric, LayerSearchState state) {
        while (!state.candidates.isEmpty()) {
            ScoredNode closest = state.candidates.poll();
            if (state.results.size() >= ef && closest.distance > state.results.peek().distance) {
                break;
            }
            processNeighbors(graph, query, ef, level, metric, state, closest);
        }
    }

    private static void processNeighbors(HnswGraph graph, float[] query, int ef, int level,
                                          DistanceMetric metric, LayerSearchState state,
                                          ScoredNode closest) {
        for (int neighbor : graph.neighbors(closest.nodeId, level)) {
            if (!state.visited.add(neighbor)) {
                continue;
            }
            double d = distance(graph.vector(neighbor), query, metric);
            if (d < state.results.peek().distance || state.results.size() < ef) {
                ScoredNode sn = new ScoredNode(neighbor, d);
                state.candidates.add(sn);
                state.results.add(sn);
                if (state.results.size() > ef) {
                    state.results.poll();
                }
            }
        }
    }

    // --- Neighbor selection and connection ---

    static List<ScoredNode> selectNeighborsDiverse(List<ScoredNode> candidates, int maxConn,
                                                     HnswGraph graph, DistanceMetric metric) {
        if (candidates.size() <= maxConn) {
            return new ArrayList<>(candidates);
        }
        List<ScoredNode> selected = new ArrayList<>(maxConn);
        for (ScoredNode candidate : candidates) {
            if (selected.size() >= maxConn) {
                break;
            }
            if (isDiverseAmongSelected(graph.vector(candidate.nodeId), candidate.distance,
                    selected, graph, metric)) {
                selected.add(candidate);
            }
        }
        backfillFromCandidates(selected, candidates, maxConn);
        return selected;
    }

    private static boolean isDiverseAmongSelected(float[] candidateVec, double candidateDistance,
                                                  List<ScoredNode> selected, HnswGraph graph,
                                                  DistanceMetric metric) {
        for (ScoredNode sel : selected) {
            if (distance(candidateVec, graph.vector(sel.nodeId), metric) < candidateDistance) {
                return false;
            }
        }
        return true;
    }

    private static void backfillFromCandidates(List<ScoredNode> selected,
                                               List<ScoredNode> candidates, int maxConn) {
        for (ScoredNode candidate : candidates) {
            if (selected.size() >= maxConn) {
                return;
            }
            if (!selected.contains(candidate)) {
                selected.add(candidate);
            }
        }
    }

    static void connectBidirectional(HnswGraph graph, int nodeId, List<ScoredNode> neighbors,
                                      int level, int maxConn, DistanceMetric metric) {
        for (ScoredNode neighbor : neighbors) {
            graph.addConnection(nodeId, neighbor.nodeId, level);
            graph.addConnection(neighbor.nodeId, nodeId, level);
            shrinkIfNeeded(graph, neighbor.nodeId, level, maxConn, metric);
        }
    }

    static void shrinkIfNeeded(HnswGraph graph, int nodeId, int level,
                                int maxConn, DistanceMetric metric) {
        List<Integer> connections = graph.neighbors(nodeId, level);
        if (connections.size() <= maxConn) {
            return;
        }
        float[] nodeVec = graph.vector(nodeId);
        List<ScoredNode> scored = new ArrayList<>(connections.size());
        for (int conn : connections) {
            scored.add(new ScoredNode(conn, distance(graph.vector(conn), nodeVec, metric)));
        }
        scored.sort(Comparator.comparingDouble(s -> s.distance));
        List<Integer> selected = new ArrayList<>(maxConn);
        for (ScoredNode candidate : scored) {
            if (selected.size() >= maxConn) {
                break;
            }
            if (isDiverseAmongNodeIds(graph.vector(candidate.nodeId), candidate.distance,
                    selected, graph, metric)) {
                selected.add(candidate.nodeId);
            }
        }
        backfillFromScored(selected, scored, maxConn);
        graph.setConnections(nodeId, level, selected);
    }

    private static boolean isDiverseAmongNodeIds(float[] candidateVec, double candidateDistance,
                                                 List<Integer> selected, HnswGraph graph,
                                                 DistanceMetric metric) {
        for (int sel : selected) {
            if (distance(candidateVec, graph.vector(sel), metric) < candidateDistance) {
                return false;
            }
        }
        return true;
    }

    private static void backfillFromScored(List<Integer> selected,
                                           List<ScoredNode> scored, int maxConn) {
        for (ScoredNode candidate : scored) {
            if (selected.size() >= maxConn) {
                return;
            }
            if (!selected.contains(candidate.nodeId)) {
                selected.add(candidate.nodeId);
            }
        }
    }

    // --- Mapped graph search ---

    static List<ScoredNode> searchLayerMapped(MappedHnswGraph graph,
                                               int entryId, float[] query,
                                               int ef, int level,
                                               DistanceMetric metric) {
        LayerSearchState state = new LayerSearchState();

        double entryDist = distance(graph.vector(entryId), query, metric);
        ScoredNode entrySn = new ScoredNode(entryId, entryDist);
        state.candidates.add(entrySn);
        state.results.add(entrySn);
        state.visited.add(entryId);

        while (!state.candidates.isEmpty()) {
            ScoredNode closest = state.candidates.poll();
            if (state.results.size() >= ef && closest.distance > state.results.peek().distance) {
                break;
            }
            expandMappedNeighbors(graph, query, ef, level, metric, state, closest);
        }

        List<ScoredNode> sorted = new ArrayList<>(state.results);
        sorted.sort(Comparator.comparingDouble(s -> s.distance));
        return sorted;
    }

    private static void expandMappedNeighbors(MappedHnswGraph graph, float[] query,
                                              int ef, int level, DistanceMetric metric,
                                              LayerSearchState state, ScoredNode closest) {
        for (int neighbor : graph.neighbors(closest.nodeId, level)) {
            if (!state.visited.add(neighbor)) {
                continue;
            }
            double d = distance(graph.vector(neighbor), query, metric);
            if (d < state.results.peek().distance || state.results.size() < ef) {
                ScoredNode sn = new ScoredNode(neighbor, d);
                state.candidates.add(sn);
                state.results.add(sn);
                if (state.results.size() > ef) {
                    state.results.poll();
                }
            }
        }
    }

    static int greedyClosestMapped(MappedHnswGraph graph, int entryId,
                                    float[] query, int level,
                                    DistanceMetric metric) {
        int current = entryId;
        double currentDist = distance(graph.vector(current), query, metric);
        boolean improved = true;
        while (improved) {
            improved = false;
            for (int neighbor : graph.neighbors(current, level)) {
                double d = distance(graph.vector(neighbor), query, metric);
                if (d < currentDist) {
                    current = neighbor;
                    currentDist = d;
                    improved = true;
                }
            }
        }
        return current;
    }

    // --- Utility ---

    static double distance(float[] a, float[] b, DistanceMetric metric) {
        return switch (metric) {
            case L2 -> VectorDistanceCalculator.l2Distance(a, b);
            case COSINE -> VectorDistanceCalculator.cosineDistance(a, b);
            case INNER_PRODUCT -> VectorDistanceCalculator.innerProduct(a, b);
        };
    }

    static int randomLevel(double mL) {
        // Sample uniformly in (0, 1] so -log(u) is finite: ThreadLocalRandom
        // .nextDouble() returns [0, 1), and u == 0 would make the level
        // Integer.MAX_VALUE, triggering a multi-billion-entry allocation in
        // HnswGraph.ensureLevel (OOM/crash). Cap defensively as well.
        double u = 1.0 - ThreadLocalRandom.current().nextDouble();
        int level = (int) Math.floor(-Math.log(u) * mL);
        return Math.min(level, MAX_RANDOM_LEVEL);
    }

    static int intProp(java.util.Map<String, String> props, String key, int defaultVal) {
        String val = props.get(key);
        return val == null ? defaultVal : Integer.parseInt(val);
    }

    static int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }
}
