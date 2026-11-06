package org.pangrova.planner.core.adaptive;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates AdaptiveQueryConfig record mapping from configuration values.
 * Simulates YAML configuration parsing → record construction.
 * Covers REQ-4: AQE configuration validation.
 */
class AdaptiveQueryConfigMappingTest {

    @Test
    void fullConfigMapsAllFields() {
        // Simulates: aqe.enabled=true, targetPartitionBytes=128MB,
        // skewThresholdRatio=3.0, skewMinBytes=64MB, minPartitions=4
        AdaptiveQueryConfig config = new AdaptiveQueryConfig(
                true,
                128L * 1024 * 1024,
                3.0,
                64L * 1024 * 1024,
                4);
        assertTrue(config.enabled());
        assertEquals(128L * 1024 * 1024, config.targetPartitionBytes());
        assertEquals(3.0, config.skewPartitionThresholdRatio());
        assertEquals(64L * 1024 * 1024, config.skewPartitionMinBytes());
        assertEquals(4, config.minPartitionsForCoalesce());
    }

    @Test
    void enabledOnlyUsesDefaults() {
        // Simulates: aqe.enabled=true (no other fields specified)
        AdaptiveQueryConfig config = new AdaptiveQueryConfig(true);
        assertTrue(config.enabled());
        assertEquals(256L * 1024 * 1024, config.targetPartitionBytes());
        assertEquals(5.0, config.skewPartitionThresholdRatio());
        assertEquals(256L * 1024 * 1024, config.skewPartitionMinBytes());
        assertEquals(2, config.minPartitionsForCoalesce());
    }

    @Test
    void disabledConstantHasCorrectDefaults() {
        AdaptiveQueryConfig config = AdaptiveQueryConfig.DISABLED;
        assertFalse(config.enabled());
        assertEquals(256L * 1024 * 1024, config.targetPartitionBytes());
        assertEquals(5.0, config.skewPartitionThresholdRatio());
    }

    @Test
    void invalidTargetPartitionBytesRejected() {
        assertThrows(IllegalArgumentException.class, () ->
                new AdaptiveQueryConfig(true, 0, 5.0, 256L * 1024 * 1024, 2));
    }

    @Test
    void invalidSkewThresholdRatioRejected() {
        assertThrows(IllegalArgumentException.class, () ->
                new AdaptiveQueryConfig(true, 256L * 1024 * 1024, 0.0, 256L * 1024 * 1024, 2));
    }

    @Test
    void invalidMinPartitionsRejected() {
        assertThrows(IllegalArgumentException.class, () ->
                new AdaptiveQueryConfig(true, 256L * 1024 * 1024, 5.0, 256L * 1024 * 1024, 0));
    }
}
