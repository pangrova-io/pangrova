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
package org.pangrova.coordinator.indexing.ai.lifecycle;

import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.lifecycle.IndexState;
import org.pangrova.indexing.api.descriptor.IndexType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class VectorIndexHealthAssessorTest {

    private final VectorIndexHealthAssessor assessor = new VectorIndexHealthAssessor(0.15, 0.30);

    @Test
    void healthy_whenStalenessUnderWarningThreshold() {
        IndexDescriptor d = descriptor(IndexState.READY, Instant.now());
        VectorIndexHealthAssessor.HealthAssessment result = assessor.assess(d, 100, 90);
        assertEquals(VectorIndexHealthAssessor.HealthLevel.HEALTHY, result.level());
        assertFalse(result.rebuildRecommended());
    }

    @Test
    void warning_whenStalenessExceedsWarningThreshold() {
        IndexDescriptor d = descriptor(IndexState.READY, Instant.now());
        // 20% unindexed > 15% warning threshold
        VectorIndexHealthAssessor.HealthAssessment result = assessor.assess(d, 100, 80);
        assertEquals(VectorIndexHealthAssessor.HealthLevel.WARNING, result.level());
        assertFalse(result.rebuildRecommended());
    }

    @Test
    void critical_whenStalenessExceedsCriticalThreshold() {
        IndexDescriptor d = descriptor(IndexState.READY, Instant.now());
        // 40% unindexed > 30% critical threshold
        VectorIndexHealthAssessor.HealthAssessment result = assessor.assess(d, 100, 60);
        assertEquals(VectorIndexHealthAssessor.HealthLevel.CRITICAL, result.level());
        assertTrue(result.rebuildRecommended());
    }

    @Test
    void healthy_whenBuildingWithinTimeLimit() {
        Instant recentStart = Instant.now().minusSeconds(60 * 30); // 30 min ago
        IndexDescriptor d = descriptor(IndexState.BUILDING, recentStart);
        VectorIndexHealthAssessor.HealthAssessment result = assessor.assess(d, 100, 0);
        assertEquals(VectorIndexHealthAssessor.HealthLevel.HEALTHY, result.level());
        assertFalse(result.rebuildRecommended());
    }

    @Test
    void critical_whenBuildingStuckBeyondTwoHours() {
        Instant stuckStart = Instant.now().minusSeconds(60 * 60 * 3); // 3 hours ago
        IndexDescriptor d = descriptor(IndexState.BUILDING, stuckStart);
        VectorIndexHealthAssessor.HealthAssessment result = assessor.assess(d, 100, 0);
        assertEquals(VectorIndexHealthAssessor.HealthLevel.CRITICAL, result.level());
        assertTrue(result.rebuildRecommended());
        assertEquals(1.0, result.stalenessRatio(), 1e-9);
    }

    @Test
    void healthy_whenDropped() {
        IndexDescriptor d = descriptor(IndexState.DROPPED, Instant.now());
        VectorIndexHealthAssessor.HealthAssessment result = assessor.assess(d, 100, 0);
        assertEquals(VectorIndexHealthAssessor.HealthLevel.HEALTHY, result.level());
        assertFalse(result.rebuildRecommended());
    }

    @Test
    void healthy_whenTableHasNoData() {
        IndexDescriptor d = descriptor(IndexState.READY, Instant.now());
        VectorIndexHealthAssessor.HealthAssessment result = assessor.assess(d, 0, 0);
        assertEquals(VectorIndexHealthAssessor.HealthLevel.HEALTHY, result.level());
    }

    @Test
    void throwsForNonVectorIndex() {
        IndexDescriptor d = new IndexDescriptor(
                "idx-1", "idx-1", "cat", "schema", "tbl",
                List.of("col"), IndexType.BLOOM_FILTER, Map.of(),
                IndexState.READY, null, 1, null, null,
                Instant.now(), Instant.now(), "default");
        assertThrows(IllegalArgumentException.class, () -> assessor.assess(d, 100, 50));
    }

    @Test
    void stalenessRatioComputedCorrectly() {
        IndexDescriptor d = descriptor(IndexState.READY, Instant.now());
        VectorIndexHealthAssessor.HealthAssessment result = assessor.assess(d, 10, 7);
        assertEquals(0.3, result.stalenessRatio(), 1e-9);
    }

    @Test
    void constructorRejectsInvalidThresholds() {
        assertThrows(IllegalArgumentException.class,
                () -> new VectorIndexHealthAssessor(-0.1, 0.5));
        assertThrows(IllegalArgumentException.class,
                () -> new VectorIndexHealthAssessor(0.5, 0.3)); // critical < warning
    }

    private static IndexDescriptor descriptor(IndexState state, Instant updatedAt) {
        Instant now = Instant.now();
        return new IndexDescriptor(
                "idx-test", "test-index", "cat", "schema", "tbl",
                List.of("vec_col"), IndexType.VECTOR, Map.of(),
                state, null, 1, null, null,
                now, updatedAt, "default");
    }
}
