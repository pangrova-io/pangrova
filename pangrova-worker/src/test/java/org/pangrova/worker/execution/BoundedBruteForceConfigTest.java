package org.pangrova.worker.execution;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for BoundedBruteForceConfig (Task 195).
 */
class BoundedBruteForceConfigTest {

    @Test
    void defaultConfig() {
        var config = BoundedBruteForceConfig.DEFAULT;
        assertEquals(100_000, config.maxRowsPerFile());
        assertEquals(50, config.maxFilesTotal());
        assertEquals(30_000, config.timeoutMs());
    }

    @Test
    void customConfig() {
        var config = new BoundedBruteForceConfig(50_000, 10, 15_000);
        assertEquals(50_000, config.maxRowsPerFile());
        assertEquals(10, config.maxFilesTotal());
        assertEquals(15_000, config.timeoutMs());
    }

    @Test
    void unlimitedConfig() {
        var config = new BoundedBruteForceConfig(0, 0, 0);
        assertEquals(0, config.maxRowsPerFile());
        assertEquals(0, config.maxFilesTotal());
        assertEquals(0, config.timeoutMs());
    }

    @Test
    void negativeValuesThrow() {
        assertThrows(IllegalArgumentException.class,
                () -> new BoundedBruteForceConfig(-1, 10, 1000));
        assertThrows(IllegalArgumentException.class,
                () -> new BoundedBruteForceConfig(100, -1, 1000));
        assertThrows(IllegalArgumentException.class,
                () -> new BoundedBruteForceConfig(100, 10, -1));
    }
}
