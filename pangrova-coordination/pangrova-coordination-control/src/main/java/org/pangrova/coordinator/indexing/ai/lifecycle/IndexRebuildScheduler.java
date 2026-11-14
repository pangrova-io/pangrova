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

import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.persistence.coordination.api.lease.DistributedLease;
import org.pangrova.coordination.api.lease.LeaseEntry;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 * Schedules vector index rebuild operations based on health assessments.
 *
 * <p>Uses a {@link DistributedLease} to prevent concurrent rebuilds of the
 * same index across coordinators, following the same pattern as
 * {@link org.pangrova.coordinator.clustering.ClusteringScheduler}.</p>
 *
 * <p>Every published ANN generation is immutable:
 * <ul>
 *   <li>HNSW, IVF_PQ, and DISK_ANN rebuild into a new complete generation.</li>
 *   <li>Publication atomically swaps the active generation after validation.</li>
 * </ul></p>
 */
@ThreadSafe
public final class IndexRebuildScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(IndexRebuildScheduler.class);
    private static final Duration DEFAULT_LEASE_DURATION = Duration.ofMinutes(60);
    static final String LEASE_KEY_PREFIX = "index_rebuild:";

    private final ExecutorService executor;
    private final DistributedLease distributedLease;
    private final String coordinatorId;
    private final VectorIndexHealthAssessor healthAssessor;
    private final BiFunction<IndexDescriptor, RebuildAction, CompletableFuture<Void>> rebuildExecutor;
    private final AtomicInteger activeRebuilds = new AtomicInteger(0);
    private final int maxConcurrentRebuilds;

    /** Wires the scheduler with lease, health assessment, and rebuild execution dependencies. */
    public IndexRebuildScheduler(ExecutorService executor,
                                 DistributedLease distributedLease,
                                 String coordinatorId,
                                 VectorIndexHealthAssessor healthAssessor,
                                 BiFunction<IndexDescriptor, RebuildAction, CompletableFuture<Void>> rebuildExecutor,
                                 int maxConcurrentRebuilds) {
        this.executor = executor;
        this.distributedLease = distributedLease;
        if (coordinatorId == null || coordinatorId.isBlank()) {
            throw new IllegalArgumentException("coordinatorId must not be null or blank");
        }
        this.coordinatorId = coordinatorId;
        this.healthAssessor = healthAssessor;
        this.rebuildExecutor = rebuildExecutor;
        this.maxConcurrentRebuilds = maxConcurrentRebuilds;
    }

    public IndexRebuildScheduler(ExecutorService executor,
                                 DistributedLease distributedLease,
                                 String coordinatorId,
                                 VectorIndexHealthAssessor healthAssessor,
                                 BiFunction<IndexDescriptor, RebuildAction, CompletableFuture<Void>> rebuildExecutor) {
        this(executor, distributedLease, coordinatorId, healthAssessor, rebuildExecutor, 2);
    }

    /**
     * Schedules a rebuild for the given index if the health assessment warrants it.
     *
     * @param descriptor the stale index descriptor
     * @param assessment the health assessment that triggered the rebuild
     * @return future completing with the rebuild result
     * @throws IllegalStateException if max concurrent rebuilds reached or lease unavailable
     */
    public CompletableFuture<RebuildResult> scheduleRebuild(
            IndexDescriptor descriptor,
            VectorIndexHealthAssessor.HealthAssessment assessment) {
        if (descriptor.indexType() != IndexType.VECTOR) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Not a vector index: " + descriptor.indexType()));
        }
        if (!assessment.rebuildRecommended()) {
            return CompletableFuture.completedFuture(
                    new RebuildResult(descriptor.indexId(), RebuildAction.SKIPPED,
                            "Rebuild not recommended by health assessment", 0));
        }
        if (activeRebuilds.get() >= maxConcurrentRebuilds) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Max concurrent rebuilds reached: " + maxConcurrentRebuilds));
        }

        return CompletableFuture.supplyAsync(
                () -> executeWithLease(descriptor, assessment), executor);
    }

    /**
     * Evaluates a list of health assessments and schedules rebuilds for all
     * CRITICAL indexes, up to the concurrency limit.
     *
     * @param descriptors descriptors paired with their assessments
     * @return number of rebuilds scheduled
     */
    public int scheduleFromAssessments(List<IndexWithAssessment> descriptors) {
        int scheduled = 0;
        for (IndexWithAssessment entry : descriptors) {
            if (entry.assessment().rebuildRecommended()
                    && activeRebuilds.get() < maxConcurrentRebuilds) {
                scheduleRebuild(entry.descriptor(), entry.assessment());
                scheduled++;
            }
        }
        LOG.info("event=rebuild_batch_scheduled count={} total={}", scheduled, descriptors.size());
        return scheduled;
    }

    /**
     * Returns the number of currently active rebuild operations.
     */
    public int activeRebuildCount() {
        return activeRebuilds.get();
    }

    private RebuildResult executeWithLease(IndexDescriptor descriptor,
                                           VectorIndexHealthAssessor.HealthAssessment assessment) {
        String leaseKey = LEASE_KEY_PREFIX + descriptor.indexId();
        LeaseEntry heldLease = null;
        activeRebuilds.incrementAndGet();
        long startMs = System.currentTimeMillis();
        try {
            if (distributedLease != null) {
                heldLease = distributedLease.tryAcquire(
                        leaseKey, coordinatorId, DEFAULT_LEASE_DURATION).orElse(null);
                if (heldLease == null) {
                    LOG.warn("event=rebuild_lease_denied indexId={} coordinatorId={}",
                            descriptor.indexId(), coordinatorId);
                    return new RebuildResult(descriptor.indexId(), RebuildAction.SKIPPED,
                            "Could not acquire rebuild lease; another coordinator may be rebuilding", 0);
                }
                LOG.info("event=rebuild_lease_acquired indexId={} coordinatorId={}",
                        descriptor.indexId(), coordinatorId);
            }
            RebuildAction action = determineAction(descriptor);
            LOG.info("event=rebuild_started indexId={} action={} algorithm={} staleness={}%",
                    descriptor.indexId(), action,
                    descriptor.properties().getOrDefault("algorithm", "HNSW"),
                    String.format("%.1f", assessment.stalenessRatio() * 100));

            rebuildExecutor.apply(descriptor, action).join();

            long durationMs = System.currentTimeMillis() - startMs;
            LOG.info("event=rebuild_completed indexId={} action={} durationMs={}",
                    descriptor.indexId(), action, durationMs);
            return new RebuildResult(descriptor.indexId(), action,
                    "Rebuild " + action.name().toLowerCase() + " completed", durationMs);
        } catch (Exception e) {
            long durationMs = System.currentTimeMillis() - startMs;
            LOG.error("event=rebuild_failed indexId={} durationMs={}",
                    descriptor.indexId(), durationMs, e);
            throw new RuntimeException("Index rebuild failed for " + descriptor.indexId(), e);
        } finally {
            activeRebuilds.decrementAndGet();
            if (distributedLease != null && heldLease != null) {
                distributedLease.release(
                        leaseKey, coordinatorId, heldLease.epoch());
                LOG.info("event=rebuild_lease_released indexId={} coordinatorId={}",
                        descriptor.indexId(), coordinatorId);
            }
        }
    }

    private RebuildAction determineAction(IndexDescriptor descriptor) {
        String algorithmStr = descriptor.properties().getOrDefault("algorithm", "HNSW");
        CapabilityRef capability =
                BuiltInCapabilityCatalog.vectorRetrievalPlanRef(algorithmStr);
        String capabilityId = capability.getCapabilityId();
        if (BuiltInCapabilityCatalog.HNSW_RETRIEVAL_ID.equals(capabilityId)) {
            return RebuildAction.FULL_REBUILD;
        }
        if (BuiltInCapabilityCatalog.IVF_PQ_RETRIEVAL_ID.equals(capabilityId)) {
            return RebuildAction.FULL_REBUILD;
        }
        if (BuiltInCapabilityCatalog.DISKANN_RETRIEVAL_ID.equals(capabilityId)) {
            return RebuildAction.FULL_REBUILD;
        }
        throw new IllegalArgumentException(
                "Unsupported Native vector retrieval capability: " + capabilityId);
    }

    public enum RebuildAction {
        FULL_REBUILD,
        SKIPPED
    }

    public record RebuildResult(
            String indexId,
            RebuildAction action,
            String message,
            long durationMs) {

        public RebuildResult {
            if (indexId == null || indexId.isBlank()) {
                throw new IllegalArgumentException("indexId required");
            }
            if (action == null) {
                throw new IllegalArgumentException("action required");
            }
        }
    }

    public record IndexWithAssessment(
            IndexDescriptor descriptor,
            VectorIndexHealthAssessor.HealthAssessment assessment) {

        public IndexWithAssessment {
            if (descriptor == null) {
                throw new IllegalArgumentException("descriptor required");
            }
            if (assessment == null) {
                throw new IllegalArgumentException("assessment required");
            }
        }
    }
}
