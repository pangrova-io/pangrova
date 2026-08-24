/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.testing.e2e.ai;

import org.pangrova.planner.adaptive.AdaptiveQueryConfig;
import org.pangrova.planner.runtimefilter.RuntimeFilter;
import org.pangrova.planner.runtimefilter.RuntimeFilterRegistry;
import org.pangrova.planner.stats.CardinalityFeedbackStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalDouble;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the Adaptive Query Execution (AQE) chain: configuration defaults,
 * cardinality feedback recording, and runtime filter registry operations.
 */
@DisplayName("Adaptive Query Chain E2E")
class AdaptiveQueryChainE2ETest {

    @Nested
    @DisplayName("AQE Configuration")
    class AqeConfiguration {

        @Test
        @DisplayName("ENABLED preset has enabled=true")
        void enabledPresetIsEnabled() {
            assertTrue(AdaptiveQueryConfig.ENABLED.enabled(),
                    "ENABLED preset should have enabled=true");
        }

        @Test
        @DisplayName("DISABLED preset has enabled=false")
        void disabledPresetIsDisabled() {
            assertFalse(AdaptiveQueryConfig.DISABLED.enabled(),
                    "DISABLED preset should have enabled=false");
        }

        @Test
        @DisplayName("Default target partition size is 256 MB")
        void defaultTargetPartitionBytes() {
            long expected = 256L * 1024 * 1024;
            assertEquals(expected, AdaptiveQueryConfig.ENABLED.targetPartitionBytes(),
                    "Default target partition bytes should be 256 MB");
        }

        @Test
        @DisplayName("Default skew threshold ratio is 5.0")
        void defaultSkewThresholdRatio() {
            assertEquals(5.0, AdaptiveQueryConfig.ENABLED.skewPartitionThresholdRatio(),
                    "Default skew threshold ratio should be 5.0");
        }

        @Test
        @DisplayName("Default broadcast threshold is 10 MB")
        void defaultBroadcastThreshold() {
            long expected = 10L * 1024 * 1024;
            assertEquals(expected, AdaptiveQueryConfig.ENABLED.broadcastThresholdBytes(),
                    "Default broadcast threshold should be 10 MB");
        }

        @Test
        @DisplayName("Default min partitions for coalesce is 2")
        void defaultMinPartitionsForCoalesce() {
            assertEquals(2, AdaptiveQueryConfig.ENABLED.minPartitionsForCoalesce(),
                    "Default min partitions for coalesce should be 2");
        }

        @Test
        @DisplayName("Custom config preserves all values")
        void customConfigPreservesValues() {
            AdaptiveQueryConfig config = new AdaptiveQueryConfig(
                    true, 512L * 1024 * 1024, 3.0, 128L * 1024 * 1024, 4, 20L * 1024 * 1024);
            assertTrue(config.enabled());
            assertEquals(512L * 1024 * 1024, config.targetPartitionBytes());
            assertEquals(3.0, config.skewPartitionThresholdRatio());
            assertEquals(128L * 1024 * 1024, config.skewPartitionMinBytes());
            assertEquals(4, config.minPartitionsForCoalesce());
            assertEquals(20L * 1024 * 1024, config.broadcastThresholdBytes());
        }

        @Test
        @DisplayName("Invalid target partition bytes rejected")
        void invalidTargetPartitionBytesRejected() {
            assertThrows(IllegalArgumentException.class,
                    () -> new AdaptiveQueryConfig(true, 0, 5.0, 256L * 1024 * 1024, 2, 10L * 1024 * 1024),
                    "Zero target partition bytes should be rejected");
        }
    }

    @Nested
    @DisplayName("Cardinality Feedback Store")
    class CardinalityFeedbackStoreTests {

        @Test
        @DisplayName("Records and returns corrected estimates")
        void recordsAndReturnsCorrectedEstimates() {
            CardinalityFeedbackStore store = new CardinalityFeedbackStore();

            store.record("scan_orders", 1000, 5000);
            OptionalDouble factor = store.correctionFactor("scan_orders");

            assertTrue(factor.isPresent(), "Correction factor should be present after recording");
            assertEquals(5.0, factor.getAsDouble(), 0.001,
                    "First observation should produce exact ratio as factor");
        }

        @Test
        @DisplayName("correctedEstimate applies the learned factor")
        void correctedEstimateAppliesFactor() {
            CardinalityFeedbackStore store = new CardinalityFeedbackStore();

            store.record("scan_orders", 1000, 5000);
            double corrected = store.correctedEstimate("scan_orders", 2000);

            assertEquals(10000.0, corrected, 0.001,
                    "Corrected estimate should be freshEstimate * factor");
        }

        @Test
        @DisplayName("Unknown shape returns unchanged estimate")
        void unknownShapeReturnsUnchangedEstimate() {
            CardinalityFeedbackStore store = new CardinalityFeedbackStore();

            double corrected = store.correctedEstimate("unknown_shape", 2000);
            assertEquals(2000.0, corrected, 0.001,
                    "Unknown shape should return the fresh estimate unchanged");
        }

        @Test
        @DisplayName("Multiple observations converge via exponential smoothing")
        void multipleObservationsSmooth() {
            CardinalityFeedbackStore store = new CardinalityFeedbackStore();

            store.record("join_shape", 100, 300);
            store.record("join_shape", 100, 300);

            OptionalDouble factor = store.correctionFactor("join_shape");
            assertTrue(factor.isPresent());
            // After 2 observations with same ratio (3.0):
            // First: factor = 3.0
            // Second: factor = 0.3 * 3.0 + 0.7 * 3.0 = 3.0
            assertEquals(3.0, factor.getAsDouble(), 0.001);
        }

        @Test
        @DisplayName("Store size tracks number of distinct shapes")
        void storeSizeTracksShapes() {
            CardinalityFeedbackStore store = new CardinalityFeedbackStore();

            assertEquals(0, store.size());
            store.record("shape_a", 100, 200);
            assertEquals(1, store.size());
            store.record("shape_b", 100, 500);
            assertEquals(2, store.size());
        }

        @Test
        @DisplayName("Clear removes all entries")
        void clearRemovesAllEntries() {
            CardinalityFeedbackStore store = new CardinalityFeedbackStore();

            store.record("shape_a", 100, 200);
            store.record("shape_b", 100, 500);
            store.clear();

            assertEquals(0, store.size());
            assertFalse(store.correctionFactor("shape_a").isPresent());
        }
    }

    @Nested
    @DisplayName("Runtime Filter Registry")
    class RuntimeFilterRegistryTests {

        @Test
        @DisplayName("Registry can be instantiated")
        void registryCanBeInstantiated() {
            RuntimeFilterRegistry registry = new RuntimeFilterRegistry();
            assertNotNull(registry, "RuntimeFilterRegistry should be instantiable");
        }

        @Test
        @DisplayName("Register and retrieve a min-max filter")
        void registerAndRetrieveMinMaxFilter() {
            RuntimeFilterRegistry registry = new RuntimeFilterRegistry();

            RuntimeFilter filter = RuntimeFilter.minMax("order_id", 1, 1000);
            registry.register("stage-1", filter);

            Optional<RuntimeFilter> retrieved = registry.getFilter("stage-1");
            assertTrue(retrieved.isPresent(), "Filter should be retrievable after registration");
            assertEquals("order_id", retrieved.get().column());
            assertEquals(RuntimeFilter.FilterType.MIN_MAX, retrieved.get().type());
        }

        @Test
        @DisplayName("Missing filter returns empty Optional")
        void missingFilterReturnsEmpty() {
            RuntimeFilterRegistry registry = new RuntimeFilterRegistry();

            Optional<RuntimeFilter> result = registry.getFilter("nonexistent-stage");
            assertFalse(result.isPresent(),
                    "Non-existent stage should return empty Optional");
        }

        @Test
        @DisplayName("registeredStageIds returns all registered stages")
        void registeredStageIdsReturnsAllStages() {
            RuntimeFilterRegistry registry = new RuntimeFilterRegistry();

            registry.register("stage-1", RuntimeFilter.minMax("col_a", 1, 100));
            registry.register("stage-2", RuntimeFilter.minMax("col_b", 10, 200));

            assertEquals(2, registry.registeredStageIds().size());
            assertTrue(registry.registeredStageIds().containsAll(
                    java.util.List.of("stage-1", "stage-2")));
        }

        @Test
        @DisplayName("Clear removes all registered filters")
        void clearRemovesAllFilters() {
            RuntimeFilterRegistry registry = new RuntimeFilterRegistry();

            registry.register("stage-1", RuntimeFilter.minMax("col_a", 1, 100));
            registry.clear();

            assertTrue(registry.registeredStageIds().isEmpty());
        }
    }
}
