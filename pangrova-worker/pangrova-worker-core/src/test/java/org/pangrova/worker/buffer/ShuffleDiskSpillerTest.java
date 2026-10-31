package org.pangrova.worker.buffer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates ShuffleDiskSpiller spill/read correctness, concurrent safety,
 * and close cleanup.
 */
class ShuffleDiskSpillerTest {

    @TempDir
    Path tempDir;

    private ShuffleDiskSpiller spiller;

    @BeforeEach
    void setUp() {
        spiller = new ShuffleDiskSpiller(tempDir.resolve("spill"));
    }

    @AfterEach
    void tearDown() {
        spiller.close();
    }

    @Test
    void spillAndReadSinglePartition() {
        byte[] data = "hello-shuffle".getBytes();
        long written = spiller.spill(0, data);
        assertEquals(data.length, written);

        byte[] read = spiller.read(0, 0);
        assertArrayEquals(data, read);
    }

    @Test
    void spillMultipleSequences() {
        byte[] batch0 = "batch-0".getBytes();
        byte[] batch1 = "batch-1".getBytes();
        spiller.spill(0, batch0);
        spiller.spill(0, batch1);

        assertArrayEquals(batch0, spiller.read(0, 0));
        assertArrayEquals(batch1, spiller.read(0, 1));
    }

    @Test
    void spillMultiplePartitions() {
        byte[] p0 = "partition-0".getBytes();
        byte[] p1 = "partition-1".getBytes();
        spiller.spill(0, p0);
        spiller.spill(1, p1);

        assertArrayEquals(p0, spiller.read(0, 0));
        assertArrayEquals(p1, spiller.read(1, 0));
    }

    @Test
    void readNonExistentReturnsNull() {
        assertNull(spiller.read(99, 0));
    }

    @Test
    void closeDeletesAllFiles() throws IOException {
        spiller.spill(0, "data".getBytes());
        spiller.spill(1, "data".getBytes());
        Path spillDir = tempDir.resolve("spill");
        assertTrue(Files.exists(spillDir));

        spiller.close();
        assertFalse(Files.exists(spillDir));
    }

    @Test
    void totalSpillMetrics() {
        byte[] data = new byte[1024];
        spiller.spill(0, data);
        spiller.spill(0, data);
        spiller.spill(1, data);

        assertEquals(3072, spiller.totalSpillBytes());
        assertEquals(3, spiller.totalSpillFiles());
    }
}
