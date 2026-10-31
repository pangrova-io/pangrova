package org.pangrova.indexing.api.statistics;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.pangrova.connector.spi.index.statistics.FileColumnStatistics;

/**
 * Tests for {@link DynamicFilterSummary} (Task 165: build-side dynamic filtering).
 */
class DynamicFilterSummaryTest {

    @Test
    void canPrune_disjointRanges() {
        var summary = new DynamicFilterSummary.ColumnRangeSummary(
                "id", (Comparable<?>) 100, (Comparable<?>) 200, new byte[0], 50);
        var probeStats = new FileColumnStatistics("id",
                (Comparable<?>) 300, (Comparable<?>) 400, 0L, 100L, 50L, 1024L);
        assertTrue(summary.canPrune(probeStats));
    }

    @Test
    void cannotPrune_overlappingRanges() {
        var summary = new DynamicFilterSummary.ColumnRangeSummary(
                "id", (Comparable<?>) 100, (Comparable<?>) 200, new byte[0], 50);
        var probeStats = new FileColumnStatistics("id",
                (Comparable<?>) 150, (Comparable<?>) 250, 0L, 100L, 50L, 1024L);
        assertFalse(summary.canPrune(probeStats));
    }

    @Test
    void canPrune_probeBelowBuild() {
        var summary = new DynamicFilterSummary.ColumnRangeSummary(
                "id", (Comparable<?>) 100, (Comparable<?>) 200, new byte[0], 50);
        var probeStats = new FileColumnStatistics("id",
                (Comparable<?>) 10, (Comparable<?>) 50, 0L, 100L, 50L, 1024L);
        assertTrue(summary.canPrune(probeStats));
    }

    @Test
    void cannotPrune_nullBuildRange() {
        var summary = new DynamicFilterSummary.ColumnRangeSummary(
                "id", null, null, new byte[0], 0);
        var probeStats = new FileColumnStatistics("id",
                (Comparable<?>) 300, (Comparable<?>) 400, 0L, 100L, 50L, 1024L);
        assertFalse(summary.canPrune(probeStats));
    }

    @Test
    void rejectsNullColumnName() {
        assertThrows(IllegalArgumentException.class,
                () -> new DynamicFilterSummary.ColumnRangeSummary(
                        null, (Comparable<?>) 1, (Comparable<?>) 10, new byte[0], 5));
    }

    @Test
    void summaryRecordImmutable() {
        var summaries = List.of(
                new DynamicFilterSummary.ColumnRangeSummary(
                        "a", (Comparable<?>) 1, (Comparable<?>) 10, new byte[0], 5));
        var ds = new DynamicFilterSummary("q-1", "join-1", summaries);
        assertThrows(UnsupportedOperationException.class,
                () -> ds.columnSummaries().clear());
    }
}
