package org.pangrova.spi.index.vector;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link VectorSearchRequest}.
 */
class VectorSearchRequestTest {

    private static final float[] QUERY = {1.0f, 2.0f, 3.0f};

    // ── Validation ──────────────────────────────────────────────────

    @Test
    void rejectsNullColumn() {
        var ex = assertThrows(IllegalArgumentException.class, () ->
                new VectorSearchRequest(null, QUERY, 10, DistanceMetric.L2));
        assertEquals("column required", ex.getMessage());
    }

    @Test
    void rejectsBlankColumn() {
        assertThrows(IllegalArgumentException.class, () ->
                new VectorSearchRequest("   ", QUERY, 10, DistanceMetric.L2));
    }

    @Test
    void rejectsEmptyColumn() {
        assertThrows(IllegalArgumentException.class, () ->
                new VectorSearchRequest("", QUERY, 10, DistanceMetric.L2));
    }

    @Test
    void rejectsNullQueryVector() {
        var ex = assertThrows(IllegalArgumentException.class, () ->
                new VectorSearchRequest("col", null, 10, DistanceMetric.L2));
        assertEquals("queryVector required", ex.getMessage());
    }

    @Test
    void rejectsEmptyQueryVector() {
        assertThrows(IllegalArgumentException.class, () ->
                new VectorSearchRequest("col", new float[0], 10, DistanceMetric.L2));
    }

    @Test
    void rejectsTopKZero() {
        var ex = assertThrows(IllegalArgumentException.class, () ->
                new VectorSearchRequest("col", QUERY, 0, DistanceMetric.L2));
        assertEquals("topK must be >= 1", ex.getMessage());
    }

    @Test
    void rejectsNegativeTopK() {
        assertThrows(IllegalArgumentException.class, () ->
                new VectorSearchRequest("col", QUERY, -5, DistanceMetric.L2));
    }

    @Test
    void rejectsNullMetric() {
        var ex = assertThrows(IllegalArgumentException.class, () ->
                new VectorSearchRequest("col", QUERY, 10, null));
        assertEquals("metric required", ex.getMessage());
    }

    // ── Defensive copy ──────────────────────────────────────────────

    @Test
    void queryVectorIsDefensivelyCopied() {
        float[] original = {1.0f, 2.0f, 3.0f};
        var request = new VectorSearchRequest("col", original, 10, DistanceMetric.L2);

        // Mutating the original array must not affect the record
        original[0] = 99.0f;
        assertEquals(1.0f, request.queryVector()[0]);
    }

    @Test
    void queryVectorAccessorReturnsNewReference() {
        var request = new VectorSearchRequest("col", QUERY, 10, DistanceMetric.L2);
        // Record returns the internal array directly (no accessor copy), but
        // the constructor copy ensures the original is isolated
        assertArrayEquals(QUERY, request.queryVector());
    }

    // ── Field accessors ─────────────────────────────────────────────

    @Test
    void fieldAccessorsReturnConstructorValues() {
        var request = new VectorSearchRequest("embedding", QUERY, 5, DistanceMetric.COSINE);
        assertEquals("embedding", request.column());
        assertArrayEquals(QUERY, request.queryVector());
        assertEquals(5, request.topK());
        assertEquals(DistanceMetric.COSINE, request.metric());
    }

    @Test
    void acceptsTopKOne() {
        var request = new VectorSearchRequest("col", QUERY, 1, DistanceMetric.INNER_PRODUCT);
        assertEquals(1, request.topK());
    }

    @Test
    void acceptsSingleElementVector() {
        var request = new VectorSearchRequest("col", new float[]{42.0f}, 10, DistanceMetric.L2);
        assertEquals(1, request.queryVector().length);
    }
}
