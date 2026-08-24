package org.pangrova.spi.index.vector;

import org.pangrova.spi.index.vector.VectorSearchResult.ScoredRow;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link VectorSearchResult} and {@link ScoredRow}.
 */
class VectorSearchResultTest {

    // ── VectorSearchResult ──────────────────────────────────────────

    @Test
    void neighborsDefensivelyCopied() {
        var mutable = new ArrayList<>(List.of(
                new ScoredRow(0, 1.0),
                new ScoredRow(1, 2.0)));
        var result = new VectorSearchResult("/data/file1.parquet", mutable);

        mutable.add(new ScoredRow(2, 3.0));
        assertEquals(2, result.neighbors().size());
    }

    @Test
    void neighborsIsUnmodifiable() {
        var result = new VectorSearchResult("/data/file1.parquet",
                List.of(new ScoredRow(0, 1.0)));
        assertThrows(UnsupportedOperationException.class, () ->
                result.neighbors().add(new ScoredRow(1, 2.0)));
    }

    @Test
    void emptyNeighborsAllowed() {
        var result = new VectorSearchResult("/data/file1.parquet", List.of());
        assertTrue(result.neighbors().isEmpty());
    }

    @Test
    void fieldAccessorsReturnConstructorValues() {
        var neighbors = List.of(new ScoredRow(100, 0.5));
        var result = new VectorSearchResult("/data/file.parquet", neighbors);
        assertEquals("/data/file.parquet", result.dataFilePath());
        assertEquals(1, result.neighbors().size());
        assertEquals(100, result.neighbors().get(0).rowOffset());
        assertEquals(0.5, result.neighbors().get(0).distance());
    }

    @Test
    void rejectsNullNeighborsList() {
        assertThrows(NullPointerException.class, () ->
                new VectorSearchResult("/data/file.parquet", null));
    }

    // ── ScoredRow ───────────────────────────────────────────────────

    @Test
    void compareToOrdersByDistanceAscending() {
        var closer = new ScoredRow(0, 1.0);
        var farther = new ScoredRow(1, 2.0);
        assertTrue(closer.compareTo(farther) < 0);
        assertTrue(farther.compareTo(closer) > 0);
    }

    @Test
    void compareToEqualDistance() {
        var a = new ScoredRow(0, 1.5);
        var b = new ScoredRow(1, 1.5);
        assertEquals(0, a.compareTo(b));
    }

    @Test
    void compareToHandlesZeroDistance() {
        var zero = new ScoredRow(0, 0.0);
        var positive = new ScoredRow(1, 0.001);
        assertTrue(zero.compareTo(positive) < 0);
    }

    @Test
    void compareToUsesDoubleCompareSemantics() {
        // Double.compare handles NaN consistently
        var nan = new ScoredRow(0, Double.NaN);
        var normal = new ScoredRow(1, 1.0);
        // NaN is considered greater than any non-NaN value by Double.compare
        assertTrue(nan.compareTo(normal) > 0);
    }

    @Test
    void scoredRowFieldAccessors() {
        var row = new ScoredRow(42L, 3.14);
        assertEquals(42L, row.rowOffset());
        assertEquals(3.14, row.distance());
    }

    @Test
    void sortingProducesDistanceAscendingOrder() {
        var rows = new ArrayList<>(List.of(
                new ScoredRow(2, 3.0),
                new ScoredRow(0, 1.0),
                new ScoredRow(1, 2.0)));
        rows.sort(null); // uses natural ordering (Comparable)
        assertEquals(1.0, rows.get(0).distance());
        assertEquals(2.0, rows.get(1).distance());
        assertEquals(3.0, rows.get(2).distance());
    }
}
