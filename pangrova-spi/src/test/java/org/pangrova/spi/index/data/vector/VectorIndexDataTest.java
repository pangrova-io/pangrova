package org.pangrova.spi.index.data.vector;

import org.pangrova.spi.index.IndexType;
import org.pangrova.spi.index.data.IndexData;
import org.pangrova.spi.index.data.vector.VectorIndexData.VectorAlgorithmType;
import org.pangrova.spi.index.vector.DistanceMetric;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link VectorIndexData} record and {@link VectorAlgorithmType} enum.
 */
class VectorIndexDataTest {

    private static VectorIndexData sample() {
        return new VectorIndexData(
                VectorAlgorithmType.HNSW, 128, DistanceMetric.L2,
                1000, new byte[]{1, 2, 3}, Map.of("M", "16"));
    }

    // ── IndexType correctness ───────────────────────────────────────

    @Test
    void indexTypeReturnsVector() {
        assertEquals(IndexType.VECTOR, sample().indexType());
    }

    @Test
    void implementsIndexData() {
        assertInstanceOf(IndexData.class, sample());
    }

    // ── estimatedSizeInBytes ────────────────────────────────────────

    @Test
    void estimatedSizeIncludesPayloadAndParams() {
        byte[] payload = new byte[1024];
        var data = new VectorIndexData(
                VectorAlgorithmType.HNSW, 64, DistanceMetric.COSINE,
                500, payload, Map.of("a", "1", "b", "2"));
        assertEquals(1024 + 2 * 64L, data.estimatedSizeInBytes());
    }

    @Test
    void estimatedSizeEmptyParams() {
        byte[] payload = new byte[256];
        var data = new VectorIndexData(
                VectorAlgorithmType.IVFPQ, 32, DistanceMetric.INNER_PRODUCT,
                100, payload, Map.of());
        assertEquals(256L, data.estimatedSizeInBytes());
    }

    // ── Validation ──────────────────────────────────────────────────

    @Test
    void rejectsDimensionLessThanOne() {
        var ex = assertThrows(IllegalArgumentException.class, () ->
                new VectorIndexData(VectorAlgorithmType.HNSW, 0, DistanceMetric.L2,
                        10, new byte[1], Map.of()));
        assertEquals("dimension must be positive", ex.getMessage());
    }

    @Test
    void rejectsNegativeDimension() {
        assertThrows(IllegalArgumentException.class, () ->
                new VectorIndexData(VectorAlgorithmType.HNSW, -5, DistanceMetric.L2,
                        10, new byte[1], Map.of()));
    }

    @Test
    void rejectsNegativeVectorCount() {
        var ex = assertThrows(IllegalArgumentException.class, () ->
                new VectorIndexData(VectorAlgorithmType.HNSW, 128, DistanceMetric.L2,
                        -1, new byte[1], Map.of()));
        assertEquals("vectorCount must be non-negative", ex.getMessage());
    }

    @Test
    void rejectsNullPayload() {
        var ex = assertThrows(IllegalArgumentException.class, () ->
                new VectorIndexData(VectorAlgorithmType.HNSW, 128, DistanceMetric.L2,
                        10, null, Map.of()));
        assertEquals("indexPayload must not be null", ex.getMessage());
    }

    @Test
    void rejectsNullBuildParams() {
        assertThrows(NullPointerException.class, () ->
                new VectorIndexData(VectorAlgorithmType.HNSW, 128, DistanceMetric.L2,
                        10, new byte[1], null));
    }

    @Test
    void acceptsZeroVectorCount() {
        var data = new VectorIndexData(
                VectorAlgorithmType.HNSW, 128, DistanceMetric.L2,
                0, new byte[1], Map.of());
        assertEquals(0, data.vectorCount());
    }

    @Test
    void acceptsDimensionOne() {
        var data = new VectorIndexData(
                VectorAlgorithmType.IVFPQ, 1, DistanceMetric.COSINE,
                5, new byte[1], Map.of());
        assertEquals(1, data.dimension());
    }

    // ── Defensive copy — buildParams ────────────────────────────────

    @Test
    void buildParamsDefensivelyCopied() {
        var mutable = new HashMap<String, String>();
        mutable.put("M", "16");
        var data = new VectorIndexData(
                VectorAlgorithmType.HNSW, 128, DistanceMetric.L2,
                100, new byte[1], mutable);
        mutable.put("extra", "value");
        assertEquals(1, data.buildParams().size());
    }

    @Test
    void buildParamsIsUnmodifiable() {
        var data = sample();
        assertThrows(UnsupportedOperationException.class, () ->
                data.buildParams().put("new", "value"));
    }

    // ── indexPayload NOT defensively copied ──────────────────────────

    @Test
    void indexPayloadNotDefensivelyCopied() {
        byte[] payload = {1, 2, 3};
        var data = new VectorIndexData(
                VectorAlgorithmType.HNSW, 128, DistanceMetric.L2,
                10, payload, Map.of());
        // Mutation of original array IS visible — this is the documented exception
        payload[0] = 99;
        assertEquals(99, data.indexPayload()[0]);
    }

    @Test
    void indexPayloadAccessorReturnsSameArray() {
        var data = sample();
        assertSame(data.indexPayload(), data.indexPayload());
    }

    // ── VectorAlgorithmType enum ────────────────────────────────────

    @Test
    void algorithmTypeHasTwoValues() {
        assertEquals(3, VectorAlgorithmType.values().length);
    }

    @Test
    void algorithmTypeValueOf() {
        assertEquals(VectorAlgorithmType.HNSW, VectorAlgorithmType.valueOf("HNSW"));
        assertEquals(VectorAlgorithmType.IVFPQ, VectorAlgorithmType.valueOf("IVFPQ"));
    }

    // ── Record field accessors ──────────────────────────────────────

    @Test
    void fieldAccessorsReturnConstructorValues() {
        var data = new VectorIndexData(
                VectorAlgorithmType.IVFPQ, 768, DistanceMetric.INNER_PRODUCT,
                5000, new byte[]{42}, Map.of("nlist", "256"));
        assertEquals(VectorAlgorithmType.IVFPQ, data.algorithmType());
        assertEquals(768, data.dimension());
        assertEquals(DistanceMetric.INNER_PRODUCT, data.metric());
        assertEquals(5000, data.vectorCount());
        assertArrayEquals(new byte[]{42}, data.indexPayload());
        assertEquals(Map.of("nlist", "256"), data.buildParams());
    }
}
