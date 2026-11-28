package org.pangrova.coordinator.query.execution;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import org.pangrova.coordinator.query.execution.dispatch.dag.PipelineConfig;

/**
 * Validates PipelineConfig YAML configuration mapping and behavioral contracts.
 * Covers REQ-28: Phase 2 configuration validation.
 */
class PipelineConfigMappingTest {

    @Test
    @DisplayName("DISABLED constant: pipeline off, unlimited stages")
    void disabledConstantHasExpectedDefaults() {
        assertFalse(PipelineConfig.DISABLED.enabled());
        assertEquals(0, PipelineConfig.DISABLED.maxConcurrentStages());
    }

    @Test
    @DisplayName("ENABLED constant: pipeline on, unlimited stages")
    void enabledConstantHasExpectedDefaults() {
        assertTrue(PipelineConfig.ENABLED.enabled());
        assertEquals(0, PipelineConfig.ENABLED.maxConcurrentStages());
    }

    @Test
    @DisplayName("Custom config: all fields mapped correctly")
    void customConfigMapsAllFields() {
        PipelineConfig config = new PipelineConfig(true, 8);
        assertTrue(config.enabled());
        assertEquals(8, config.maxConcurrentStages());
    }

    @Test
    @DisplayName("maxConcurrentStages=0 means unlimited")
    void zeroConcurrentStagesMeansUnlimited() {
        PipelineConfig config = new PipelineConfig(true, 0);
        assertEquals(0, config.maxConcurrentStages());
    }

    @Test
    @DisplayName("Negative maxConcurrentStages rejected")
    void negativeConcurrentStagesRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new PipelineConfig(true, -1));
    }
}
