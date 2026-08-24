package org.pangrova.coordinator.ai.vector;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for GlobalTopKMerger (Task 195).
 */
class GlobalTopKMergerTest {

    @Test
    void mergeSingleWorkerResult() {
        List<GlobalTopKMerger.ScoredRow> worker1 = List.of(
                new GlobalTopKMerger.ScoredRow("w1", 1, 0.1, "file1.parquet"),
                new GlobalTopKMerger.ScoredRow("w1", 2, 0.3, "file1.parquet"),
                new GlobalTopKMerger.ScoredRow("w1", 3, 0.5, "file1.parquet"));

        var result = GlobalTopKMerger.merge(List.of(worker1), 2);

        assertEquals(2, result.rows().size());
        assertEquals(0.1, result.rows().get(0).distance(), 1e-9);
        assertEquals(0.3, result.rows().get(1).distance(), 1e-9);
        assertEquals(3, result.totalScanned());
        assertEquals(1, result.workerCount());
    }

    @Test
    void mergeMultipleWorkerResults() {
        List<GlobalTopKMerger.ScoredRow> worker1 = List.of(
                new GlobalTopKMerger.ScoredRow("w1", 1, 0.1, "file1.parquet"),
                new GlobalTopKMerger.ScoredRow("w1", 2, 0.5, "file1.parquet"));
        List<GlobalTopKMerger.ScoredRow> worker2 = List.of(
                new GlobalTopKMerger.ScoredRow("w2", 3, 0.2, "file2.parquet"),
                new GlobalTopKMerger.ScoredRow("w2", 4, 0.4, "file2.parquet"));

        var result = GlobalTopKMerger.merge(List.of(worker1, worker2), 3);

        assertEquals(3, result.rows().size());
        // Should be sorted by distance: 0.1, 0.2, 0.4
        assertEquals(0.1, result.rows().get(0).distance(), 1e-9);
        assertEquals(0.2, result.rows().get(1).distance(), 1e-9);
        assertEquals(0.4, result.rows().get(2).distance(), 1e-9);
        assertEquals(4, result.totalScanned());
        assertEquals(2, result.workerCount());
    }

    @Test
    void mergeEmptyResults() {
        var result = GlobalTopKMerger.merge(List.of(), 5);

        assertTrue(result.rows().isEmpty());
        assertEquals(0, result.totalScanned());
    }

    @Test
    void topKGreaterThanTotal() {
        List<GlobalTopKMerger.ScoredRow> worker1 = List.of(
                new GlobalTopKMerger.ScoredRow("w1", 1, 0.1, "file.parquet"));

        var result = GlobalTopKMerger.merge(List.of(worker1), 100);

        assertEquals(1, result.rows().size());
    }

    @Test
    void invalidTopKThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> GlobalTopKMerger.merge(List.of(), 0));
    }
}
