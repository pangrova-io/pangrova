package org.pangrova.coordinator.ai.embedding;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PersistentEmbeddingQueueCoordinator (Task 194).
 */
class PersistentEmbeddingQueueCoordinatorTest {

    @Test
    void enqueueAndDrain() {
        var workStore = new InMemoryEmbeddingWorkStore();
        var coordinator = new PersistentEmbeddingQueueCoordinator(workStore);

        coordinator.enqueue("catalog.schema.table", "row-1", "model-1",
                "embedding_col", "hello world");

        assertEquals(1, coordinator.queueDepth("catalog.schema.table"));
        assertEquals(1, coordinator.totalEnqueued());

        // Verify persisted in work store
        assertEquals(1, workStore.countUnfinished("catalog.schema.table"));

        List<PendingEmbeddingRow> drained = coordinator.drain("catalog.schema.table", 10);
        assertEquals(1, drained.size());
        assertEquals("row-1", drained.get(0).rowId());
        assertEquals(1, coordinator.totalDrained());
    }

    @Test
    void markCompleted() {
        var workStore = new InMemoryEmbeddingWorkStore();
        var coordinator = new PersistentEmbeddingQueueCoordinator(workStore);

        coordinator.enqueue("t1", "r1", "m1", "col", "text");
        coordinator.drain("t1", 10);
        coordinator.markCompleted("t1", "r1");

        assertEquals(0, workStore.countUnfinished("t1"));
    }

    @Test
    void recoverFromWorkStore() {
        var workStore = new InMemoryEmbeddingWorkStore();

        // Simulate items already in store (from previous run)
        workStore.save(new PendingEmbeddingRow(
                "t1", "r1", "m1", "col", "text1", java.time.Instant.now()));
        workStore.save(new PendingEmbeddingRow(
                "t1", "r2", "m1", "col", "text2", java.time.Instant.now()));

        var coordinator = new PersistentEmbeddingQueueCoordinator(workStore);
        int recovered = coordinator.recover();

        assertEquals(2, recovered);
        assertEquals(2, coordinator.queueDepth("t1"));
    }

    @Test
    void heapLimitTriggersBackpressure() {
        var workStore = new InMemoryEmbeddingWorkStore();
        // Very low heap limit to trigger backpressure quickly
        var coordinator = new PersistentEmbeddingQueueCoordinator(
                workStore, 100_000, 1024);

        // Each enqueue adds ~512 bytes estimated overhead
        coordinator.enqueue("t1", "r1", "m1", "col", "text");
        coordinator.enqueue("t1", "r2", "m1", "col", "text");

        // Third enqueue should hit heap limit
        assertThrows(EmbeddingQueueFullException.class, () ->
                coordinator.enqueue("t1", "r3", "m1", "col", "text"));
        assertTrue(coordinator.backpressureActivations() > 0);
    }

    @Test
    void backpressureMetricsTracked() {
        var workStore = new InMemoryEmbeddingWorkStore();
        var coordinator = new PersistentEmbeddingQueueCoordinator(workStore);

        coordinator.enqueue("t1", "r1", "m1", "col", "text");

        assertEquals(1, coordinator.totalEnqueued());
        assertEquals(0, coordinator.totalDrained());
        assertEquals(0, coordinator.backpressureActivations());
        assertTrue(coordinator.estimatedHeapBytes() > 0);
        assertTrue(coordinator.maxHeapBytes() > 0);
    }
}
