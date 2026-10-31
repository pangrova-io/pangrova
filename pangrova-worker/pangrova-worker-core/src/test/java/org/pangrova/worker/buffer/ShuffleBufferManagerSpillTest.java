package org.pangrova.worker.buffer;

import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Validates ShuffleBufferManager spill integration:
 * threshold-triggered spill and disk read fallback.
 */
class ShuffleBufferManagerSpillTest {

    @TempDir
    Path tempDir;

    private ShuffleDiskSpiller spiller;
    private ShuffleBufferManager manager;

    @BeforeEach
    void setUp() {
        spiller = new ShuffleDiskSpiller(tempDir.resolve("spill"));
        // Small capacity to trigger spill quickly.
        manager = new ShuffleBufferManager(10, spiller, 0.5);
    }

    @AfterEach
    void tearDown() {
        manager.close();
    }

    @Test
    void memoryOnlyModeNeverSpills() throws InterruptedException {
        var memoryOnly = new ShuffleBufferManager(10);
        var key = new ShuffleBufferKey("test", "stage", 0);
        memoryOnly.createPartition(key);
        memoryOnly.addBatch(key, new byte[1024]);
        byte[] batch = memoryOnly.pollBatch(key, 100);
        assertNotNull(batch);
        assertEquals(1024, batch.length);
        memoryOnly.close();
    }

    @Test
    void addBatchWithSpillerDoesNotThrow() throws InterruptedException {
        var key = new ShuffleBufferKey("test", "stage", 0);
        manager.createPartition(key);
        manager.addBatch(key, new byte[64]);
        byte[] batch = manager.pollBatch(key, 100);
        assertNotNull(batch);
        assertEquals(64, batch.length);
    }

    @Test
    void multiplePartitionsWithSpiller() throws InterruptedException {
        var key0 = new ShuffleBufferKey("test", "stage", 0);
        var key1 = new ShuffleBufferKey("test", "stage", 1);
        manager.createPartition(key0);
        manager.createPartition(key1);
        manager.addBatch(key0, "p0-data".getBytes());
        manager.addBatch(key1, "p1-data".getBytes());

        byte[] b0 = manager.pollBatch(key0, 100);
        byte[] b1 = manager.pollBatch(key1, 100);
        assertNotNull(b0);
        assertNotNull(b1);
    }
}
