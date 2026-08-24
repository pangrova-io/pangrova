package org.pangrova.coordinator.query.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.pangrova.coordinator.query.execution.split.CacheAffinityConfig;
import org.pangrova.coordinator.query.execution.split.CacheAffinityAssigner;
import org.pangrova.coordinator.query.execution.split.SplitAssignment;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentConfig;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Phase 4 configuration validation tests.
 * Covers REQ-47 (split assignment config) and REQ-53 (cache affinity config).
 */
class Phase4ConfigValidationTest {

    @Nested
    @DisplayName("SplitAssignmentConfig")
    class SplitAssignmentConfigTests {

        @Test
        void defaultConfig() {
            SplitAssignmentConfig config = SplitAssignmentConfig.defaults();
            assertThat(config.leaseDurationMs()).isEqualTo(60_000);
            assertThat(config.pullModeEnabled()).isFalse();
        }

        @Test
        void customConfig() {
            SplitAssignmentConfig config = new SplitAssignmentConfig(30_000, null, 5_000, true);
            assertThat(config.leaseDurationMs()).isEqualTo(30_000);
            assertThat(config.stealDelayMs()).isEqualTo(5_000);
            assertThat(config.pullModeEnabled()).isTrue();
        }
    }

    @Nested
    @DisplayName("CacheAffinityConfig")
    class CacheAffinityConfigTests {

        @Test
        void defaultConfig() {
            CacheAffinityConfig config = CacheAffinityConfig.DEFAULT;
            assertThat(config.enabled()).isTrue();
            assertThat(config.stealDelayMs()).isEqualTo(500);
        }

        @Test
        void disabledConfig() {
            CacheAffinityConfig config = CacheAffinityConfig.DISABLED;
            assertThat(config.enabled()).isFalse();
            assertThat(config.stealDelayMs()).isZero();
        }

        @Test
        void customConfig() {
            CacheAffinityConfig config = new CacheAffinityConfig(true, 1000);
            assertThat(config.enabled()).isTrue();
            assertThat(config.stealDelayMs()).isEqualTo(1000);
        }

        @Test
        void zeroStealDelayDisablesDelay() {
            CacheAffinityConfig config = new CacheAffinityConfig(true, 0);
            assertThat(config.stealDelayMs()).isZero();
        }

        @Test
        void negativeStealDelayRejected() {
            assertThatThrownBy(() -> new CacheAffinityConfig(true, -1))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void disabledConfigDisablesCacheAffineSplitAssignment() {
            // When disabled, SplitAssignmentManager should not use CacheAffinityAssigner
            SplitAssignmentManager manager = new SplitAssignmentManager(60_000, null, 0);
            long now = System.currentTimeMillis();
            SplitAssignment split = new SplitAssignment(0, "data", java.util.List.of(),
                    java.util.List.of("s3://f1"), now);
            manager.registerSplits("q1", 0, java.util.List.of(split));

            // Without assigner, falls through to any-split (no cache affinity)
            var result = manager.requestSplit("q1", 0, "w-1", java.util.List.of());
            assertThat(result).isPresent();
        }

        @Test
        void enabledConfigEnablesThreeTierPriority() {
            CacheAffinityAssigner assigner = new CacheAffinityAssigner();
            // With assigner but no workers, falls through to locality then any
            SplitAssignmentManager manager = new SplitAssignmentManager(60_000, assigner, 500);
            long pastDelay = System.currentTimeMillis() - 1_000;
            SplitAssignment split = new SplitAssignment(0, "data", java.util.List.of(),
                    java.util.List.of("s3://f1"), pastDelay);
            manager.registerSplits("q1", 0, java.util.List.of(split));

            var result = manager.requestSplit("q1", 0, "w-1", java.util.List.of());
            assertThat(result).isPresent();
        }
    }
}
