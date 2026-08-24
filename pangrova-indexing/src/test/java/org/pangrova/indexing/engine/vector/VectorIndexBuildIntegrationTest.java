package org.pangrova.indexing.engine.vector;

import org.pangrova.indexing.serde.IndexSerializer;
import org.pangrova.indexing.serde.TypeSerializerRegistry;
import org.pangrova.spi.index.data.IndexData;
import org.pangrova.spi.index.data.vector.VectorIndexData;
import org.pangrova.spi.index.data.vector.VectorIndexData.VectorAlgorithmType;
import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.VectorSearchRequest;
import org.pangrova.spi.index.vector.VectorSearchResult;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test: build → serialize → deserialize → search for both HNSW and IVFPQ.
 */
class VectorIndexBuildIntegrationTest {

    private static final int DIMENSION = 16;
    private static final int VECTOR_COUNT = 200;
    private static final int TOP_K = 5;

    private final VectorStrategyProvider provider = new VectorStrategyProvider();
    private final IndexSerializer serializer = new IndexSerializer(
            TypeSerializerRegistry.createDefault());

    @Test
    void hnswBuildSerializeDeserializeSearch() {
        List<float[]> vectors = generateVectors(VECTOR_COUNT, DIMENSION);
        Map<String, String> props = Map.of(
                "algorithm", "HNSW", "dimension", String.valueOf(DIMENSION),
                "metric", "L2", "hnsw.M", "8", "hnsw.efConstruction", "50");

        VectorIndexData built = buildIndex(vectors, props);
        assertEquals(VectorAlgorithmType.HNSW, built.algorithmType());
        assertEquals(DIMENSION, built.dimension());
        assertEquals(VECTOR_COUNT, built.vectorCount());

        byte[] serialized = serializer.serialize(built);
        Optional<IndexData> deserialized = serializer.deserialize(serialized);
        assertTrue(deserialized.isPresent());
        VectorIndexData restored = assertInstanceOf(VectorIndexData.class, deserialized.get());
        assertEquals(built.algorithmType(), restored.algorithmType());
        assertEquals(built.dimension(), restored.dimension());
        assertEquals(built.vectorCount(), restored.vectorCount());
        assertEquals(built.metric(), restored.metric());

        VectorSearchRequest request = new VectorSearchRequest(
                "embedding", vectors.get(0), TOP_K, DistanceMetric.L2);
        List<VectorSearchResult.ScoredRow> results =
                provider.vectorSearch(restored, request, Map.of("hnsw.efSearch", "32"));
        assertFalse(results.isEmpty());
        assertTrue(results.size() <= TOP_K);
        assertDistanceOrdering(results);
        // Self-search: first result should be very close to 0
        assertTrue(results.get(0).distance() < 0.01,
                "Self-search distance should be near 0, got " + results.get(0).distance());
    }

    @Test
    void ivfpqBuildSerializeDeserializeSearch() {
        List<float[]> vectors = generateVectors(VECTOR_COUNT, DIMENSION);
        Map<String, String> props = Map.of(
                "algorithm", "IVFPQ", "dimension", String.valueOf(DIMENSION),
                "metric", "L2", "ivfpq.nlist", "8", "ivfpq.m", "4", "ivfpq.nbits", "8");

        VectorIndexData built = buildIndex(vectors, props);
        assertEquals(VectorAlgorithmType.IVFPQ, built.algorithmType());
        assertEquals(VECTOR_COUNT, built.vectorCount());

        byte[] serialized = serializer.serialize(built);
        Optional<IndexData> deserialized = serializer.deserialize(serialized);
        assertTrue(deserialized.isPresent());
        VectorIndexData restored = assertInstanceOf(VectorIndexData.class, deserialized.get());
        assertEquals(built.algorithmType(), restored.algorithmType());
        assertEquals(built.dimension(), restored.dimension());
        assertEquals(built.vectorCount(), restored.vectorCount());

        VectorSearchRequest request = new VectorSearchRequest(
                "embedding", vectors.get(0), TOP_K, DistanceMetric.L2);
        List<VectorSearchResult.ScoredRow> results =
                provider.vectorSearch(restored, request, Map.of("ivfpq.nprobe", "4"));
        assertFalse(results.isEmpty());
        assertTrue(results.size() <= TOP_K);
        assertDistanceOrdering(results);
    }

    @Test
    void emptyVectorsBuildAndSearch() {
        Map<String, String> props = Map.of(
                "algorithm", "HNSW", "dimension", String.valueOf(DIMENSION), "metric", "L2");
        Iterator<List<Comparable<?>>> empty = List.<List<Comparable<?>>>of().iterator();
        IndexData built = provider.build(empty, props);
        assertInstanceOf(VectorIndexData.class, built);

        byte[] serialized = serializer.serialize(built);
        Optional<IndexData> deserialized = serializer.deserialize(serialized);
        assertTrue(deserialized.isPresent());
    }

    @SuppressWarnings("unchecked")
    private VectorIndexData buildIndex(List<float[]> vectors, Map<String, String> props) {
        List<List<Comparable<?>>> rows = new ArrayList<>(vectors.size());
        for (float[] v : vectors) {
            List<Float> floatList = new ArrayList<>(v.length);
            for (float f : v) {
                floatList.add(f);
            }
            // VectorStrategyProvider.toFloatArray() accepts List<?> of Number
            rows.add(List.of((Comparable<?>) new ComparableFloatList(floatList)));
        }
        IndexData data = provider.build(rows.iterator(), props);
        return assertInstanceOf(VectorIndexData.class, data);
    }

    private static void assertDistanceOrdering(List<VectorSearchResult.ScoredRow> results) {
        for (int i = 1; i < results.size(); i++) {
            assertTrue(results.get(i).distance() >= results.get(i - 1).distance(),
                    "Results not sorted by distance at index " + i);
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

    /**
     * A List<Float> that also implements Comparable, allowing it to pass through
     * the Iterator<List<Comparable<?>>> interface. VectorStrategyProvider.toFloatArray()
     * checks {@code value instanceof List<?>} and iterates elements as Number.
     */
    private static final class ComparableFloatList extends ArrayList<Float>
            implements Comparable<ComparableFloatList> {

        ComparableFloatList(List<Float> values) {
            super(values);
        }

        @Override
        public int compareTo(ComparableFloatList other) {
            return Integer.compare(this.size(), other.size());
        }
    }
}
