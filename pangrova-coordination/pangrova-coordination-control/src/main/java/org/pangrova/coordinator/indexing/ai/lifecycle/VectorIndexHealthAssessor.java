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
import org.pangrova.indexing.descriptor.IndexDescriptorTransitions;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Assesses the health of vector indexes by comparing the build snapshot against
 * the current table snapshot to estimate data staleness.
 *
 * <p>Staleness is expressed as the ratio of un-indexed data files to total data
 * files. When the ratio exceeds a configurable threshold, the assessor marks
 * the index as {@link IndexState#STALE} with a descriptive reason.</p>
 *
 * <p>Health levels:
 * <ul>
 *   <li>{@link HealthLevel#HEALTHY} — staleness &le; warningThreshold</li>
 *   <li>{@link HealthLevel#WARNING} — staleness &gt; warningThreshold</li>
 *   <li>{@link HealthLevel#CRITICAL} — staleness &gt; criticalThreshold; auto-rebuild recommended</li>
 * </ul></p>
 */
@ThreadSafe
public final class VectorIndexHealthAssessor {

    private static final Logger LOG = LoggerFactory.getLogger(VectorIndexHealthAssessor.class);
    private static final Duration MAX_BUILD_DURATION = Duration.ofHours(2);
    private static final double DEFAULT_WARNING_THRESHOLD = 0.15;
    private static final double DEFAULT_CRITICAL_THRESHOLD = 0.30;
    private static final long SECONDS_PER_MINUTE = 60;

    private final double warningThreshold;
    private final double criticalThreshold;

    /** Creates an assessor with explicit staleness thresholds in the [0, 1] range. */
    public VectorIndexHealthAssessor(double warningThreshold, double criticalThreshold) {
        if (warningThreshold < 0 || warningThreshold > 1.0) {
            throw new IllegalArgumentException("warningThreshold must be in [0, 1]: " + warningThreshold);
        }
        if (criticalThreshold < warningThreshold || criticalThreshold > 1.0) {
            throw new IllegalArgumentException(
                    "criticalThreshold must be in [warningThreshold, 1]: " + criticalThreshold);
        }
        this.warningThreshold = warningThreshold;
        this.criticalThreshold = criticalThreshold;
    }

    public VectorIndexHealthAssessor() {
        this(DEFAULT_WARNING_THRESHOLD, DEFAULT_CRITICAL_THRESHOLD);
    }

    /**
     * Assesses the health of a vector index.
     *
     * @param descriptor      the index descriptor
     * @param totalDataFiles  total data files in the table
     * @param indexedDataFiles number of data files covered by the current index build
     * @return health assessment result
     */
    public HealthAssessment assess(IndexDescriptor descriptor, long totalDataFiles, long indexedDataFiles) {
        if (descriptor.indexType() != IndexType.VECTOR) {
            throw new IllegalArgumentException("Not a vector index: " + descriptor.indexType());
        }
        if (descriptor.state() == IndexState.DROPPED) {
            return new HealthAssessment(descriptor.indexId(), HealthLevel.HEALTHY, 0.0,
                    "Index is dropped", false);
        }
        if (descriptor.state() == IndexState.BUILDING) {
            Duration buildAge = Duration.between(descriptor.updatedAt(), Instant.now());
            if (buildAge.compareTo(MAX_BUILD_DURATION) > 0) {
                String msg = String.format("Index build has been stuck for %.0f minutes (limit %d min)",
                        (double) buildAge.toSeconds() / SECONDS_PER_MINUTE, MAX_BUILD_DURATION.toMinutes());
                LOG.warn("event=vector_index_build_stuck indexId={} buildAgeMin={}",
                        descriptor.indexId(), buildAge.toMinutes());
                return new HealthAssessment(descriptor.indexId(), HealthLevel.CRITICAL, 1.0, msg, true);
            }
            return new HealthAssessment(descriptor.indexId(), HealthLevel.HEALTHY, 0.0,
                    "Index is currently building", false);
        }
        if (totalDataFiles == 0) {
            return new HealthAssessment(descriptor.indexId(), HealthLevel.HEALTHY, 0.0,
                    "Table has no data files", false);
        }

        long unindexedFiles = totalDataFiles - Math.min(indexedDataFiles, totalDataFiles);
        double stalenessRatio = (double) unindexedFiles / totalDataFiles;

        HealthLevel level;
        boolean rebuildRecommended;
        if (stalenessRatio > criticalThreshold) {
            level = HealthLevel.CRITICAL;
            rebuildRecommended = true;
        } else if (stalenessRatio > warningThreshold) {
            level = HealthLevel.WARNING;
            rebuildRecommended = false;
        } else {
            level = HealthLevel.HEALTHY;
            rebuildRecommended = false;
        }

        String message = String.format(
                "staleness=%.1f%% unindexed=%d/%d threshold=%.0f%%/%.0f%%",
                stalenessRatio * 100, unindexedFiles, totalDataFiles,
                warningThreshold * 100, criticalThreshold * 100);

        LOG.info("event=vector_index_health_assessed indexId={} level={} {}",
                descriptor.indexId(), level, message);

        return new HealthAssessment(descriptor.indexId(), level, stalenessRatio,
                message, rebuildRecommended);
    }

    /**
     * Batch-assesses all vector indexes and returns those that need attention.
     *
     * @param assessments pre-computed assessments
     * @return assessments with level WARNING or CRITICAL
     */
    public List<HealthAssessment> filterUnhealthy(List<HealthAssessment> assessments) {
        return assessments.stream()
                .filter(a -> a.level() != HealthLevel.HEALTHY)
                .toList();
    }

    /**
     * Returns the descriptor transitioned to STALE if the assessment is CRITICAL
     * and the descriptor is currently READY.
     *
     * @param descriptor the current descriptor
     * @param assessment the health assessment
     * @return updated descriptor if transitioned, original otherwise
     */
    public IndexDescriptor applyHealthTransition(IndexDescriptor descriptor, HealthAssessment assessment) {
        if (assessment.level() == HealthLevel.CRITICAL
                && descriptor.state() == IndexState.READY) {
            String reason = String.format("Auto-staled: %.1f%% data unindexed (threshold %.0f%%)",
                    assessment.stalenessRatio() * 100, criticalThreshold * 100);
            LOG.warn("event=vector_index_auto_staled indexId={} reason={}", descriptor.indexId(), reason);
            return IndexDescriptorTransitions.transition(descriptor, IndexState.STALE, reason, Instant.now());
        }
        return descriptor;
    }

    public enum HealthLevel {
        HEALTHY,
        WARNING,
        CRITICAL
    }

    public record HealthAssessment(
            String indexId,
            HealthLevel level,
            double stalenessRatio,
            String message,
            boolean rebuildRecommended) {

        public HealthAssessment {
            if (indexId == null || indexId.isBlank()) {
                throw new IllegalArgumentException("indexId required");
            }
            if (level == null) {
                throw new IllegalArgumentException("level required");
            }
        }
    }
}
