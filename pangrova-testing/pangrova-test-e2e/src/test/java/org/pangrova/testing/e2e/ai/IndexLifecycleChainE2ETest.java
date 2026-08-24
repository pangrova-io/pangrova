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

import org.pangrova.coordinator.ai.index.lifecycle.VectorIndexHealthAssessor;
import org.pangrova.coordinator.ai.index.lifecycle.VectorIndexHealthAssessor.HealthAssessment;
import org.pangrova.coordinator.ai.index.lifecycle.VectorIndexHealthAssessor.HealthLevel;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;
import org.pangrova.spi.index.IndexType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the index lifecycle chain: index descriptor state transitions
 * and vector index health assessment.
 */
@DisplayName("Index Lifecycle Chain E2E")
class IndexLifecycleChainE2ETest {

    private static IndexDescriptor createDescriptor(IndexState state) {
        return new IndexDescriptor(
                "idx-001", "vec_embedding_idx",
                "lakehouse", "public", "documents",
                List.of("embedding"),
                IndexType.VECTOR,
                Map.of("algorithm", "HNSW", "dimension", "768", "metric", "COSINE"),
                state, null, 0, null, null,
                Instant.now(), Instant.now(), "default");
    }

    @Nested
    @DisplayName("Index State Transitions")
    class IndexStateTransitions {

        @Test
        @DisplayName("BUILDING -> READY is allowed")
        void buildingToReady() {
            IndexDescriptor desc = createDescriptor(IndexState.BUILDING);
            IndexDescriptor updated = desc.withState(IndexState.READY, Instant.now());
            assertEquals(IndexState.READY, updated.state());
        }

        @Test
        @DisplayName("BUILDING -> STALE is allowed")
        void buildingToStale() {
            IndexDescriptor desc = createDescriptor(IndexState.BUILDING);
            IndexDescriptor updated = desc.withState(IndexState.STALE, "build timeout", Instant.now());
            assertEquals(IndexState.STALE, updated.state());
        }

        @Test
        @DisplayName("BUILDING -> DROPPED is allowed")
        void buildingToDropped() {
            IndexDescriptor desc = createDescriptor(IndexState.BUILDING);
            IndexDescriptor updated = desc.withState(IndexState.DROPPED, Instant.now());
            assertEquals(IndexState.DROPPED, updated.state());
        }

        @Test
        @DisplayName("BUILDING -> BUILDING is allowed (restart)")
        void buildingToBuilding() {
            IndexDescriptor desc = createDescriptor(IndexState.BUILDING);
            IndexDescriptor updated = desc.withState(IndexState.BUILDING, Instant.now());
            assertEquals(IndexState.BUILDING, updated.state());
        }

        @Test
        @DisplayName("READY -> BUILDING is allowed (rebuild)")
        void readyToBuilding() {
            IndexDescriptor desc = createDescriptor(IndexState.READY);
            IndexDescriptor updated = desc.withState(IndexState.BUILDING, Instant.now());
            assertEquals(IndexState.BUILDING, updated.state());
        }

        @Test
        @DisplayName("READY -> STALE is allowed")
        void readyToStale() {
            IndexDescriptor desc = createDescriptor(IndexState.READY);
            IndexDescriptor updated = desc.withState(IndexState.STALE, "data changed", Instant.now());
            assertEquals(IndexState.STALE, updated.state());
        }

        @Test
        @DisplayName("READY -> DROPPED is allowed")
        void readyToDropped() {
            IndexDescriptor desc = createDescriptor(IndexState.READY);
            IndexDescriptor updated = desc.withState(IndexState.DROPPED, Instant.now());
            assertEquals(IndexState.DROPPED, updated.state());
        }

        @Test
        @DisplayName("STALE -> BUILDING is allowed (rebuild)")
        void staleToBuilding() {
            IndexDescriptor desc = createDescriptor(IndexState.BUILDING)
                    .withState(IndexState.STALE, "outdated", Instant.now());
            IndexDescriptor updated = desc.withState(IndexState.BUILDING, Instant.now());
            assertEquals(IndexState.BUILDING, updated.state());
        }

        @Test
        @DisplayName("STALE -> DROPPED is allowed")
        void staleToDropped() {
            IndexDescriptor desc = createDescriptor(IndexState.BUILDING)
                    .withState(IndexState.STALE, "outdated", Instant.now());
            IndexDescriptor updated = desc.withState(IndexState.DROPPED, Instant.now());
            assertEquals(IndexState.DROPPED, updated.state());
        }

        @Test
        @DisplayName("DROPPED -> any state is rejected")
        void droppedToAnyIsRejected() {
            IndexDescriptor desc = createDescriptor(IndexState.BUILDING)
                    .withState(IndexState.DROPPED, Instant.now());

            assertThrows(IllegalStateException.class,
                    () -> desc.withState(IndexState.BUILDING, Instant.now()),
                    "DROPPED -> BUILDING should be rejected");
            assertThrows(IllegalStateException.class,
                    () -> desc.withState(IndexState.READY, Instant.now()),
                    "DROPPED -> READY should be rejected");
            assertThrows(IllegalStateException.class,
                    () -> desc.withState(IndexState.STALE, Instant.now()),
                    "DROPPED -> STALE should be rejected");
        }

        @Test
        @DisplayName("STALE -> READY is rejected")
        void staleToReadyIsRejected() {
            IndexDescriptor desc = createDescriptor(IndexState.BUILDING)
                    .withState(IndexState.STALE, "outdated", Instant.now());

            assertThrows(IllegalStateException.class,
                    () -> desc.withState(IndexState.READY, Instant.now()),
                    "STALE -> READY should be rejected (must rebuild first)");
        }

        @Test
        @DisplayName("withGeneration updates generation counter")
        void withGenerationUpdatesCounter() {
            IndexDescriptor desc = createDescriptor(IndexState.READY);
            IndexDescriptor updated = desc.withGeneration(5, Instant.now());
            assertEquals(5, updated.generation());
        }

        @Test
        @DisplayName("withBuildSnapshotId updates snapshot reference")
        void withBuildSnapshotIdUpdatesSnapshot() {
            IndexDescriptor desc = createDescriptor(IndexState.READY);
            IndexDescriptor updated = desc.withBuildSnapshotId("snap-123", Instant.now());
            assertEquals("snap-123", updated.buildSnapshotId());
        }

        @Test
        @DisplayName("Stale reason is cleared on transition to non-STALE state")
        void staleReasonClearedOnNonStaleTransition() {
            IndexDescriptor stale = createDescriptor(IndexState.BUILDING)
                    .withState(IndexState.STALE, "data drift", Instant.now());
            assertEquals("data drift", stale.staleReason());

            IndexDescriptor rebuilding = stale.withState(IndexState.BUILDING, Instant.now());
            assertFalse(rebuilding.staleReason() != null && !rebuilding.staleReason().isBlank(),
                    "Stale reason should be cleared when transitioning away from STALE");
        }
    }

    @Nested
    @DisplayName("Vector Index Health Assessment")
    class VectorIndexHealthAssessmentTests {

        @Test
        @DisplayName("Healthy index with zero staleness")
        void healthyIndexZeroStaleness() {
            VectorIndexHealthAssessor assessor = new VectorIndexHealthAssessor();
            IndexDescriptor desc = createDescriptor(IndexState.READY);

            HealthAssessment assessment = assessor.assess(desc, 100, 100);
            assertNotNull(assessment);
            assertEquals(HealthLevel.HEALTHY, assessment.level());
            assertEquals(0.0, assessment.stalenessRatio(), 0.001);
            assertFalse(assessment.rebuildRecommended());
        }

        @Test
        @DisplayName("Warning level when staleness exceeds warning threshold")
        void warningLevelWhenStalenessBelowCritical() {
            VectorIndexHealthAssessor assessor = new VectorIndexHealthAssessor(0.10, 0.30);
            IndexDescriptor desc = createDescriptor(IndexState.READY);

            // 20% staleness: above 10% warning, below 30% critical
            HealthAssessment assessment = assessor.assess(desc, 100, 80);
            assertEquals(HealthLevel.WARNING, assessment.level());
            assertFalse(assessment.rebuildRecommended());
        }

        @Test
        @DisplayName("Critical level when staleness exceeds critical threshold")
        void criticalLevelWhenStalenessExceedsCritical() {
            VectorIndexHealthAssessor assessor = new VectorIndexHealthAssessor(0.10, 0.30);
            IndexDescriptor desc = createDescriptor(IndexState.READY);

            // 50% staleness: above 30% critical
            HealthAssessment assessment = assessor.assess(desc, 100, 50);
            assertEquals(HealthLevel.CRITICAL, assessment.level());
            assertTrue(assessment.rebuildRecommended());
        }

        @Test
        @DisplayName("BUILDING index is always assessed as healthy")
        void buildingIndexAlwaysHealthy() {
            VectorIndexHealthAssessor assessor = new VectorIndexHealthAssessor();
            IndexDescriptor desc = createDescriptor(IndexState.BUILDING);

            HealthAssessment assessment = assessor.assess(desc, 100, 0);
            assertEquals(HealthLevel.HEALTHY, assessment.level());
        }

        @Test
        @DisplayName("Empty table is always healthy")
        void emptyTableAlwaysHealthy() {
            VectorIndexHealthAssessor assessor = new VectorIndexHealthAssessor();
            IndexDescriptor desc = createDescriptor(IndexState.READY);

            HealthAssessment assessment = assessor.assess(desc, 0, 0);
            assertEquals(HealthLevel.HEALTHY, assessment.level());
        }

        @Test
        @DisplayName("Non-vector index type is rejected")
        void nonVectorIndexTypeRejected() {
            VectorIndexHealthAssessor assessor = new VectorIndexHealthAssessor();
            IndexDescriptor bloomIndex = new IndexDescriptor(
                    "idx-002", "bloom_idx",
                    "lakehouse", "public", "orders",
                    List.of("order_id"),
                    IndexType.BLOOM_FILTER,
                    Map.of(),
                    IndexState.READY, null, 0, null, null,
                    Instant.now(), Instant.now(), "default");

            assertThrows(IllegalArgumentException.class,
                    () -> assessor.assess(bloomIndex, 100, 100),
                    "Non-vector index should be rejected");
        }

        @Test
        @DisplayName("applyHealthTransition transitions READY to STALE on CRITICAL")
        void applyHealthTransitionOnCritical() {
            VectorIndexHealthAssessor assessor = new VectorIndexHealthAssessor(0.10, 0.30);
            IndexDescriptor desc = createDescriptor(IndexState.READY);

            HealthAssessment critical = assessor.assess(desc, 100, 50);
            assertEquals(HealthLevel.CRITICAL, critical.level());

            IndexDescriptor transitioned = assessor.applyHealthTransition(desc, critical);
            assertEquals(IndexState.STALE, transitioned.state());
            assertNotNull(transitioned.staleReason());
        }

        @Test
        @DisplayName("applyHealthTransition does not transition on WARNING")
        void applyHealthTransitionNoChangeOnWarning() {
            VectorIndexHealthAssessor assessor = new VectorIndexHealthAssessor(0.10, 0.30);
            IndexDescriptor desc = createDescriptor(IndexState.READY);

            HealthAssessment warning = assessor.assess(desc, 100, 80);
            assertEquals(HealthLevel.WARNING, warning.level());

            IndexDescriptor result = assessor.applyHealthTransition(desc, warning);
            assertEquals(IndexState.READY, result.state(),
                    "WARNING should not trigger state transition");
        }

        @Test
        @DisplayName("filterUnhealthy returns only WARNING and CRITICAL assessments")
        void filterUnhealthyReturnsOnlyUnhealthy() {
            VectorIndexHealthAssessor assessor = new VectorIndexHealthAssessor(0.10, 0.30);

            List<HealthAssessment> all = List.of(
                    new HealthAssessment("idx-1", HealthLevel.HEALTHY, 0.05, "ok", false),
                    new HealthAssessment("idx-2", HealthLevel.WARNING, 0.20, "warning", false),
                    new HealthAssessment("idx-3", HealthLevel.CRITICAL, 0.50, "critical", true));

            List<HealthAssessment> unhealthy = assessor.filterUnhealthy(all);
            assertEquals(2, unhealthy.size());
            assertTrue(unhealthy.stream().noneMatch(a -> a.level() == HealthLevel.HEALTHY));
        }

        @Test
        @DisplayName("Default thresholds are warning=15%, critical=30%")
        void defaultThresholds() {
            VectorIndexHealthAssessor assessor = new VectorIndexHealthAssessor();
            IndexDescriptor desc = createDescriptor(IndexState.READY);

            // 10% staleness should be healthy (below 15% default warning)
            HealthAssessment healthy = assessor.assess(desc, 100, 90);
            assertEquals(HealthLevel.HEALTHY, healthy.level());

            // 20% staleness should be warning (above 15%, below 30%)
            HealthAssessment warning = assessor.assess(desc, 100, 80);
            assertEquals(HealthLevel.WARNING, warning.level());

            // 40% staleness should be critical (above 30%)
            HealthAssessment critical = assessor.assess(desc, 100, 60);
            assertEquals(HealthLevel.CRITICAL, critical.level());
        }
    }
}
