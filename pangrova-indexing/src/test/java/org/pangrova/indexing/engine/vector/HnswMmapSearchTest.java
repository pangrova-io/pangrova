package org.pangrova.indexing.engine.vector;

import org.pangrova.spi.index.data.vector.VectorIndexData;
import org.pangrova.spi.index.data.vector.VectorIndexData.VectorAlgorithmType;
import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.VectorSearchRequest;
import org.pangrova.spi.index.vector.VectorSearchResult;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for HNSW mmap + incremental loading.
 *
 * <p>Verifies that the memory-mapped search path produces correct results
 * and benchmarks the latency improvement for large graphs.</p>
 */
class HnswMmapSearchTest {

    private static final int DIMENSION = 32;
    private static final int TOP_K = 10;

    private final HnswEngine engine = new HnswEngine();

    @Test
    void searchReturnsSameResultsAsBuild() {
        List<float[]> vectors = generateVectors(500, DIMENSION);
        VectorIndexData indexData = buildIndex(vectors, DIMENSION, DistanceMetric.L2);

        float[] query = vectors.get(0);
        VectorSearchRequest request = new VectorSearchRequest(
                "embedding", query, TOP_K, DistanceMetric.L2);

        List<VectorSearchResult.ScoredRow> results = engine.search(
                indexData, request, Map.of("hnsw.efSearch", "64"));

        assertThat(results).isNotEmpty();
        assertThat(results.size()).isLessThanOrEqualTo(TOP_K);
        assertThat(results.get(0).distance()).isLessThan(0.01);
        assertDistanceOrdering(results);
    }

    @Test
    void searchWithCosineMetric() {
        List<float[]> vectors = generateVectors(200, DIMENSION);
        VectorIndexData indexData = buildIndex(vectors, DIMENSION, DistanceMetric.COSINE);

        float[] query = vectors.get(5);
        VectorSearchRequest request = new VectorSearchRequest(
                "embedding", query, TOP_K, DistanceMetric.COSINE);

        List<VectorSearchResult.ScoredRow> results = engine.search(
                indexData, request, Map.of("hnsw.efSearch", "64"));

        assertThat(results).isNotEmpty();
        assertThat(results.get(0).distance()).isLessThan(0.01);
        assertDistanceOrdering(results);
    }

    @Test
    void searchEmptyPayloadReturnsEmpty() {
        VectorIndexData emptyData = new VectorIndexData(
                VectorAlgorithmType.HNSW, DIMENSION, DistanceMetric.L2,
                0, new byte[0], Map.of());

        float[] query = new float[DIMENSION];
        VectorSearchRequest request = new VectorSearchRequest(
                "embedding", query, TOP_K, DistanceMetric.L2);

        List<VectorSearchResult.ScoredRow> results = engine.search(
                emptyData, request, Map.of());

        assertThat(results).isEmpty();
    }

    @Test
    void mappedGraphOpenAndClose() throws IOException {
        List<float[]> vectors = generateVectors(100, DIMENSION);
        VectorIndexData indexData = buildIndex(vectors, DIMENSION, DistanceMetric.L2);

        try (MappedHnswGraph graph = MappedHnswGraph.fromPayload(
                indexData.indexPayload())) {
            assertThat(graph.nodeCount()).isEqualTo(100);
            assertThat(graph.dimension()).isEqualTo(DIMENSION);
            assertThat(graph.entryPoint()).isGreaterThanOrEqualTo(0);
            assertThat(graph.maxLevel()).isGreaterThanOrEqualTo(0);
            assertThat(graph.isOpen()).isTrue();
        }
    }

    @Test
    void mappedGraphVectorAccess() throws IOException {
        List<float[]> vectors = generateVectors(50, DIMENSION);
        VectorIndexData indexData = buildIndex(vectors, DIMENSION, DistanceMetric.L2);

        try (MappedHnswGraph graph = MappedHnswGraph.fromPayload(
                indexData.indexPayload())) {
            float[] original = vectors.get(0);
            float[] fromGraph = graph.vector(0);
            assertThat(fromGraph).hasSize(DIMENSION);
            for (int d = 0; d < DIMENSION; d++) {
                assertThat(fromGraph[d]).isEqualTo(original[d]);
            }
        }
    }

    @Test
    void mappedGraphLevel0NeighborsEagerlyLoaded() throws IOException {
        List<float[]> vectors = generateVectors(100, DIMENSION);
        VectorIndexData indexData = buildIndex(vectors, DIMENSION, DistanceMetric.L2);

        try (MappedHnswGraph graph = MappedHnswGraph.fromPayload(
                indexData.indexPayload())) {
            List<Integer> neighbors = graph.neighbors(graph.entryPoint(), 0);
            assertThat(neighbors).isNotEmpty();
        }
    }

    @Test
    void mappedGraphDeeperLevelsLoadedOnDemand() throws IOException {
        List<float[]> vectors = generateVectors(1000, DIMENSION);
        VectorIndexData indexData = buildIndex(vectors, DIMENSION, DistanceMetric.L2);

        try (MappedHnswGraph graph = MappedHnswGraph.fromPayload(
                indexData.indexPayload())) {
            if (graph.maxLevel() > 0) {
                List<Integer> deepNeighbors = graph.neighbors(
                        graph.entryPoint(), graph.maxLevel());
                assertThat(deepNeighbors).isNotEmpty();
            }
        }
    }

    @Test
    void mappedGraphFromFileRoundTrip() throws IOException {
        List<float[]> vectors = generateVectors(100, DIMENSION);
        VectorIndexData indexData = buildIndex(vectors, DIMENSION, DistanceMetric.L2);

        Path tempFile = Files.createTempFile("hnsw-test-", ".idx");
        try {
            Files.write(tempFile, indexData.indexPayload());
            try (MappedHnswGraph graph = MappedHnswGraph.open(tempFile)) {
                assertThat(graph.nodeCount()).isEqualTo(100);
                assertThat(graph.dimension()).isEqualTo(DIMENSION);
            }
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    @Test
    void mappedGraphRejectsEmptyPayload() {
        assertThatThrownBy(() -> MappedHnswGraph.fromPayload(new byte[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty payload");
    }

    @Test
    void mappedGraphRejectsNullPayload() {
        assertThatThrownBy(() -> MappedHnswGraph.fromPayload(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty payload");
    }

    @Test
    void benchmarkMmapVsFullDeserialization() throws IOException {
        int largeNodeCount = 10_000;
        List<float[]> vectors = generateVectors(largeNodeCount, DIMENSION);
        VectorIndexData indexData = buildIndex(vectors, DIMENSION, DistanceMetric.L2);

        float[] query = generateVectors(1, DIMENSION).get(0);

        // Write index to file once (simulates production: index already on disk)
        Path indexFile = Files.createTempFile("hnsw-bench-", ".idx");
        try {
            Files.write(indexFile, indexData.indexPayload());

            // Warm up JIT and OS page cache
            try (MappedHnswGraph warmup = MappedHnswGraph.open(indexFile)) {
                for (int i = 0; i < 5; i++) {
                    searchMappedDirect(warmup, query, TOP_K, 64, DistanceMetric.L2);
                }
            }
            for (int i = 0; i < 5; i++) {
                HnswGraph g = HnswGraphCodec.deserialize(indexData.indexPayload());
                searchFullyDeserialized(g, query, TOP_K, 64, DistanceMetric.L2);
            }

            // Measure mmap-based search: open once, search many times
            int iterations = 100;
            long mmapStart = System.nanoTime();
            try (MappedHnswGraph graph = MappedHnswGraph.open(indexFile)) {
                for (int i = 0; i < iterations; i++) {
                    searchMappedDirect(graph, query, TOP_K, 64, DistanceMetric.L2);
                }
            }
            long mmapElapsed = System.nanoTime() - mmapStart;

            // Measure full deserialization: deserialize + search each time
            long deserStart = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                HnswGraph graph = HnswGraphCodec.deserialize(indexData.indexPayload());
                searchFullyDeserialized(graph, query, TOP_K, 64, DistanceMetric.L2);
            }
            long deserElapsed = System.nanoTime() - deserStart;

            double mmapAvgMs = (mmapElapsed / (double) iterations) / 1_000_000.0;
            double deserAvgMs = (deserElapsed / (double) iterations) / 1_000_000.0;
            double speedup = deserAvgMs / mmapAvgMs;

            System.out.printf(
                    "HNSW Search Benchmark (nodes=%d, dim=%d, topK=%d):%n"
                            + "  Full deserialization: %.3f ms/search%n"
                            + "  Mmap (open once):     %.3f ms/search%n"
                            + "  Speedup:              %.1fx%n",
                    largeNodeCount, DIMENSION, TOP_K, deserAvgMs, mmapAvgMs, speedup);

            // Mmap open-once avoids O(n) deserialization on each search
            assertThat(mmapAvgMs).isLessThan(deserAvgMs);
        } finally {
            Files.deleteIfExists(indexFile);
        }
    }

    // --- Benchmark helper: search on already-opened MappedHnswGraph ---

    private static List<VectorSearchResult.ScoredRow> searchMappedDirect(
            MappedHnswGraph graph, float[] query, int topK, int efSearch,
            DistanceMetric metric) {
        int current = graph.entryPoint();
        for (int level = graph.maxLevel(); level >= 1; level--) {
            current = greedyDescentMapped(graph, current, query, level, metric);
        }
        return beamSearchLevel0Mapped(graph, current, query, topK, efSearch, metric);
    }

    private static int greedyDescentMapped(MappedHnswGraph graph, int entryId,
                                           float[] query, int level,
                                           DistanceMetric metric) {
        int current = entryId;
        double currentDist = dist(graph.vector(current), query, metric);
        boolean improved = true;
        while (improved) {
            improved = false;
            for (int neighbor : graph.neighbors(current, level)) {
                double d = dist(graph.vector(neighbor), query, metric);
                if (d < currentDist) {
                    current = neighbor;
                    currentDist = d;
                    improved = true;
                }
            }
        }
        return current;
    }

    private static List<VectorSearchResult.ScoredRow> beamSearchLevel0Mapped(
            MappedHnswGraph graph, int entryId, float[] query, int topK,
            int ef, DistanceMetric metric) {
        Set<Integer> visited = new HashSet<>();
        PriorityQueue<double[]> candidates = new PriorityQueue<>(
                Comparator.comparingDouble(a -> a[1]));
        PriorityQueue<double[]> results = new PriorityQueue<>(
                Comparator.comparingDouble(a -> -a[1]));

        double entryDist = dist(graph.vector(entryId), query, metric);
        candidates.add(new double[]{entryId, entryDist});
        results.add(new double[]{entryId, entryDist});
        visited.add(entryId);

        while (!candidates.isEmpty()) {
            double[] closest = candidates.poll();
            if (results.size() >= ef && closest[1] > results.peek()[1]) {
                break;
            }
            for (int neighbor : graph.neighbors((int) closest[0], 0)) {
                if (!visited.add(neighbor)) {
                    continue;
                }
                double d = dist(graph.vector(neighbor), query, metric);
                if (d < results.peek()[1] || results.size() < ef) {
                    candidates.add(new double[]{neighbor, d});
                    results.add(new double[]{neighbor, d});
                    if (results.size() > ef) {
                        results.poll();
                    }
                }
            }
        }

        List<double[]> sorted = new ArrayList<>(results);
        sorted.sort(Comparator.comparingDouble(a -> a[1]));
        int resultSize = Math.min(topK, sorted.size());
        List<VectorSearchResult.ScoredRow> out = new ArrayList<>(resultSize);
        for (int i = 0; i < resultSize; i++) {
            out.add(new VectorSearchResult.ScoredRow(
                    (int) sorted.get(i)[0], sorted.get(i)[1]));
        }
        return out;
    }

    // --- Benchmark helper: old full-deserialization search for comparison ---

    private static List<VectorSearchResult.ScoredRow> searchFullyDeserialized(
            HnswGraph graph, float[] query, int topK, int efSearch,
            DistanceMetric metric) {
        int current = graph.entryPoint();
        for (int level = graph.maxLevel(); level >= 1; level--) {
            current = greedyDescent(graph, current, query, level, metric);
        }
        return beamSearchLevel0(graph, current, query, topK, efSearch, metric);
    }

    private static int greedyDescent(HnswGraph graph, int entryId,
                                     float[] query, int level,
                                     DistanceMetric metric) {
        int current = entryId;
        double currentDist = dist(graph.vector(current), query, metric);
        boolean improved = true;
        while (improved) {
            improved = false;
            for (int neighbor : graph.neighbors(current, level)) {
                double d = dist(graph.vector(neighbor), query, metric);
                if (d < currentDist) {
                    current = neighbor;
                    currentDist = d;
                    improved = true;
                }
            }
        }
        return current;
    }

    private static List<VectorSearchResult.ScoredRow> beamSearchLevel0(
            HnswGraph graph, int entryId, float[] query, int topK,
            int ef, DistanceMetric metric) {
        Set<Integer> visited = new HashSet<>();
        PriorityQueue<double[]> candidates = new PriorityQueue<>(
                Comparator.comparingDouble(a -> a[1]));
        PriorityQueue<double[]> results = new PriorityQueue<>(
                Comparator.comparingDouble(a -> -a[1]));

        double entryDist = dist(graph.vector(entryId), query, metric);
        candidates.add(new double[]{entryId, entryDist});
        results.add(new double[]{entryId, entryDist});
        visited.add(entryId);

        while (!candidates.isEmpty()) {
            double[] closest = candidates.poll();
            if (results.size() >= ef && closest[1] > results.peek()[1]) {
                break;
            }
            for (int neighbor : graph.neighbors((int) closest[0], 0)) {
                if (!visited.add(neighbor)) {
                    continue;
                }
                double d = dist(graph.vector(neighbor), query, metric);
                if (d < results.peek()[1] || results.size() < ef) {
                    candidates.add(new double[]{neighbor, d});
                    results.add(new double[]{neighbor, d});
                    if (results.size() > ef) {
                        results.poll();
                    }
                }
            }
        }

        List<double[]> sorted = new ArrayList<>(results);
        sorted.sort(Comparator.comparingDouble(a -> a[1]));
        int resultSize = Math.min(topK, sorted.size());
        List<VectorSearchResult.ScoredRow> out = new ArrayList<>(resultSize);
        for (int i = 0; i < resultSize; i++) {
            out.add(new VectorSearchResult.ScoredRow(
                    (int) sorted.get(i)[0], sorted.get(i)[1]));
        }
        return out;
    }

    private static double dist(float[] a, float[] b, DistanceMetric metric) {
        return switch (metric) {
            case L2 -> VectorDistanceCalculator.l2Distance(a, b);
            case COSINE -> VectorDistanceCalculator.cosineDistance(a, b);
            case INNER_PRODUCT -> VectorDistanceCalculator.innerProduct(a, b);
        };
    }

    // --- Helpers ---

    private VectorIndexData buildIndex(List<float[]> vectors, int dimension,
                                       DistanceMetric metric) {
        Map<String, String> props = Map.of(
                "algorithm", "HNSW",
                "dimension", String.valueOf(dimension),
                "metric", metric.name(),
                "hnsw.M", "16",
                "hnsw.efConstruction", "100");
        long[] rowOffsets = new long[vectors.size()];
        for (int i = 0; i < rowOffsets.length; i++) {
            rowOffsets[i] = i;
        }
        return engine.build(vectors, rowOffsets, dimension, metric, props);
    }

    private static void assertDistanceOrdering(
            List<VectorSearchResult.ScoredRow> results) {
        for (int i = 1; i < results.size(); i++) {
            assertThat(results.get(i).distance())
                    .isGreaterThanOrEqualTo(results.get(i - 1).distance());
        }
    }

    private static List<float[]> generateVectors(int count, int dim) {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        List<float[]> vectors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            float[] v = new float[dim];
            for (int d = 0; d < dim; d++) {
                v[d] = rng.nextFloat() * 2 - 1;
            }
            vectors.add(v);
        }
        return vectors;
    }
}
