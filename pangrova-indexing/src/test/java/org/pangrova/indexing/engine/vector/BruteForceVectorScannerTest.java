package org.pangrova.indexing.engine.vector;

import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.VectorSearchRequest;
import org.pangrova.spi.index.vector.VectorSearchResult.ScoredRow;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link BruteForceVectorScanner}.
 *
 * <p>Per F-CHAIN-INDEX-03, verifies that brute-force linear scan correctly
 * identifies nearest neighbors when no ANN index is available.</p>
 */
class BruteForceVectorScannerTest {

    private static final String FILE_PATH = "/data/table/part-00001.parquet";

    // ── Basic top-K selection ──────────────────────────────────────────────

    @Test
    void scanReturnsTopKNearest_L2() {
        // 4 vectors in 2D; query is [0, 0]; closest are [1,0] (d=1) and [0,2] (d=2)
        List<List<Comparable<?>>> rows = List.of(
                row(new float[]{1.0f, 0.0f}),   // row 0, distance = 1.0
                row(new float[]{3.0f, 4.0f}),   // row 1, distance = 5.0
                row(new float[]{0.0f, 2.0f}),   // row 2, distance = 2.0
                row(new float[]{10.0f, 10.0f})   // row 3, distance = 14.14
        );

        VectorSearchRequest request = new VectorSearchRequest(
                "embedding", new float[]{0.0f, 0.0f}, 2, DistanceMetric.L2);

        List<ScoredRow> result = BruteForceVectorScanner.scan(
                rows.iterator(), FILE_PATH, request);

        assertEquals(2, result.size());
        // Sorted by distance ascending
        assertEquals(0, result.get(0).rowOffset());  // [1,0] at row 0
        assertEquals(2, result.get(1).rowOffset());  // [0,2] at row 2
        assertTrue(result.get(0).distance() <= result.get(1).distance());
    }

    @Test
    void scanReturnsTopKNearest_cosine() {
        // Cosine distance: 1 - cos(angle)
        // query = [1, 0]; [1,0] is identical direction (d=0), [0,1] is orthogonal (d=1)
        List<List<Comparable<?>>> rows = List.of(
                row(new float[]{0.0f, 1.0f}),    // row 0, cosine distance = 1.0
                row(new float[]{1.0f, 0.0f}),    // row 1, cosine distance = 0.0
                row(new float[]{1.0f, 1.0f})     // row 2, cosine distance ~0.29
        );

        VectorSearchRequest request = new VectorSearchRequest(
                "embedding", new float[]{1.0f, 0.0f}, 2, DistanceMetric.COSINE);

        List<ScoredRow> result = BruteForceVectorScanner.scan(
                rows.iterator(), FILE_PATH, request);

        assertEquals(2, result.size());
        assertEquals(1, result.get(0).rowOffset());  // [1,0] at row 1, distance = 0
        assertEquals(2, result.get(1).rowOffset());  // [1,1] at row 2
    }

    @Test
    void scanReturnsTopKNearest_innerProduct() {
        // Inner product distance = -dot(a, b)
        // query = [1, 1]; [3,3] has dot=6 (d=-6), [1,0] has dot=1 (d=-1)
        List<List<Comparable<?>>> rows = List.of(
                row(new float[]{1.0f, 0.0f}),    // row 0, -dot = -1
                row(new float[]{3.0f, 3.0f}),    // row 1, -dot = -6
                row(new float[]{2.0f, 2.0f})     // row 2, -dot = -4
        );

        VectorSearchRequest request = new VectorSearchRequest(
                "embedding", new float[]{1.0f, 1.0f}, 2, DistanceMetric.INNER_PRODUCT);

        List<ScoredRow> result = BruteForceVectorScanner.scan(
                rows.iterator(), FILE_PATH, request);

        assertEquals(2, result.size());
        // Lower = more similar; -6 < -4
        assertEquals(1, result.get(0).rowOffset());  // [3,3] -> -6
        assertEquals(2, result.get(1).rowOffset());  // [2,2] -> -4
    }

    // ── Null / empty row handling ─────────────────────────────────────────

    @Test
    void scanSkipsNullVectorRows() {
        List<List<Comparable<?>>> rows = new ArrayList<>();
        rows.add(row(new float[]{1.0f, 0.0f}));   // row 0
        rows.add(nullRow());                        // row 1 - null vector
        rows.add(row(new float[]{0.0f, 1.0f}));   // row 2
        rows.add(emptyRow());                       // row 3 - empty row

        VectorSearchRequest request = new VectorSearchRequest(
                "embedding", new float[]{0.0f, 0.0f}, 10, DistanceMetric.L2);

        List<ScoredRow> result = BruteForceVectorScanner.scan(
                rows.iterator(), FILE_PATH, request);

        // Only 2 non-null rows should be returned
        assertEquals(2, result.size());
        assertEquals(0, result.get(0).rowOffset());
        assertEquals(2, result.get(1).rowOffset());
    }

    @Test
    void scanPreservesRowOffsetAcrossNulls() {
        // row 0: null, row 1: null, row 2: [1,0], row 3: null, row 4: [0,1]
        List<List<Comparable<?>>> rows = new ArrayList<>();
        rows.add(nullRow());
        rows.add(nullRow());
        rows.add(row(new float[]{1.0f, 0.0f}));
        rows.add(nullRow());
        rows.add(row(new float[]{0.0f, 1.0f}));

        VectorSearchRequest request = new VectorSearchRequest(
                "embedding", new float[]{0.0f, 0.0f}, 10, DistanceMetric.L2);

        List<ScoredRow> result = BruteForceVectorScanner.scan(
                rows.iterator(), FILE_PATH, request);

        assertEquals(2, result.size());
        assertEquals(2, result.get(0).rowOffset());  // file row 2, not build index 0
        assertEquals(4, result.get(1).rowOffset());  // file row 4, not build index 1
    }

    // ── Edge cases ────────────────────────────────────────────────────────

    @Test
    void scanEmptyIteratorReturnsEmptyResult() {
        VectorSearchRequest request = new VectorSearchRequest(
                "embedding", new float[]{0.0f, 0.0f}, 5, DistanceMetric.L2);

        List<ScoredRow> result = BruteForceVectorScanner.scan(
                Collections.emptyIterator(), FILE_PATH, request);

        assertTrue(result.isEmpty());
    }

    @Test
    void scanFewerThanTopKReturnsAllAvailable() {
        List<List<Comparable<?>>> rows = List.of(
                row(new float[]{1.0f, 0.0f}),
                row(new float[]{2.0f, 0.0f})
        );

        VectorSearchRequest request = new VectorSearchRequest(
                "embedding", new float[]{0.0f, 0.0f}, 10, DistanceMetric.L2);

        List<ScoredRow> result = BruteForceVectorScanner.scan(
                rows.iterator(), FILE_PATH, request);

        assertEquals(2, result.size());
    }

    @Test
    void scanTopKEqualsOneReturnsNearest() {
        List<List<Comparable<?>>> rows = List.of(
                row(new float[]{5.0f, 0.0f}),     // row 0, distance = 5
                row(new float[]{1.0f, 0.0f}),     // row 1, distance = 1
                row(new float[]{3.0f, 0.0f})      // row 2, distance = 3
        );

        VectorSearchRequest request = new VectorSearchRequest(
                "embedding", new float[]{0.0f, 0.0f}, 1, DistanceMetric.L2);

        List<ScoredRow> result = BruteForceVectorScanner.scan(
                rows.iterator(), FILE_PATH, request);

        assertEquals(1, result.size());
        assertEquals(1, result.get(0).rowOffset());
    }

    @Test
    void scanStampsFileIdOnResults() {
        List<List<Comparable<?>>> rows = List.of(
                row(new float[]{1.0f, 0.0f})
        );

        VectorSearchRequest request = new VectorSearchRequest(
                "embedding", new float[]{0.0f, 0.0f}, 5, DistanceMetric.L2);

        List<ScoredRow> result = BruteForceVectorScanner.scan(
                rows.iterator(), FILE_PATH, request);

        assertEquals(1, result.size());
        // fileId() removed from ScoredRow -- verify row offset instead
        assertEquals(0L, result.get(0).rowOffset());
    }

    @Test
    void scanResultsSortedByDistanceAscending() {
        List<List<Comparable<?>>> rows = List.of(
                row(new float[]{5.0f, 0.0f}),
                row(new float[]{1.0f, 0.0f}),
                row(new float[]{3.0f, 0.0f}),
                row(new float[]{2.0f, 0.0f}),
                row(new float[]{4.0f, 0.0f})
        );

        VectorSearchRequest request = new VectorSearchRequest(
                "embedding", new float[]{0.0f, 0.0f}, 5, DistanceMetric.L2);

        List<ScoredRow> result = BruteForceVectorScanner.scan(
                rows.iterator(), FILE_PATH, request);

        for (int i = 1; i < result.size(); i++) {
            assertTrue(result.get(i - 1).distance() <= result.get(i).distance(),
                    "Results must be sorted by distance ascending");
        }
    }

    @Test
    void scanSkipsDimensionMismatchRows() {
        // query is 2D; one row has 3D vector -- should be skipped
        List<List<Comparable<?>>> rows = new ArrayList<>();
        rows.add(row(new float[]{1.0f, 0.0f}));           // row 0, 2D - OK
        rows.add(row(new float[]{1.0f, 0.0f, 0.0f}));    // row 1, 3D - skip
        rows.add(row(new float[]{0.0f, 1.0f}));           // row 2, 2D - OK

        VectorSearchRequest request = new VectorSearchRequest(
                "embedding", new float[]{0.0f, 0.0f}, 10, DistanceMetric.L2);

        List<ScoredRow> result = BruteForceVectorScanner.scan(
                rows.iterator(), FILE_PATH, request);

        assertEquals(2, result.size());
        assertEquals(0, result.get(0).rowOffset());
        assertEquals(2, result.get(1).rowOffset());
    }

    // ── Distance computation correctness ──────────────────────────────────

    @Test
    void computeDistanceL2() {
        double d = BruteForceVectorScanner.computeDistance(
                new float[]{0, 0}, new float[]{3, 4}, DistanceMetric.L2);
        assertEquals(5.0, d, 0.0001);
    }

    @Test
    void computeDistanceCosine() {
        // Same direction -> 0
        double d = BruteForceVectorScanner.computeDistance(
                new float[]{1, 0}, new float[]{2, 0}, DistanceMetric.COSINE);
        assertEquals(0.0, d, 0.0001);
    }

    @Test
    void computeDistanceInnerProduct() {
        // -dot([1,2], [3,4]) = -(3+8) = -11
        double d = BruteForceVectorScanner.computeDistance(
                new float[]{1, 2}, new float[]{3, 4}, DistanceMetric.INNER_PRODUCT);
        assertEquals(-11.0, d, 0.0001);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    /**
     * Creates a row with a vector stored as {@code List<Float>}, which is the
     * realistic format from {@link org.pangrova.spi.connector.index.ColumnValueReader}
     * (Parquet/ORC readers yield boxed Number lists, not primitive float arrays).
     * Uses raw types because {@code List<Float>} does not implement
     * {@code Comparable<?>}, matching how real column readers populate rows.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static List<Comparable<?>> row(float[] vector) {
        List<Float> boxed = new ArrayList<>(vector.length);
        for (float v : vector) boxed.add(v);
        List raw = new ArrayList();
        raw.add(boxed);
        return raw;
    }

    private static List<Comparable<?>> nullRow() {
        List<Comparable<?>> r = new ArrayList<>();
        r.add(null);
        return r;
    }

    private static List<Comparable<?>> emptyRow() {
        return List.of();
    }
}
