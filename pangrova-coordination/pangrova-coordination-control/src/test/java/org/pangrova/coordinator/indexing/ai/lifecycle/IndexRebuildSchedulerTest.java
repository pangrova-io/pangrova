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

import org.pangrova.coordinator.enums.VectorIndexHealthLevel;
import org.pangrova.coordinator.enums.IndexRebuildAction;
import org.pangrova.persistence.coordination.api.lease.DistributedLease;
import org.pangrova.coordination.api.lease.LeaseEntry;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class IndexRebuildSchedulerTest {

    private static final VectorIndexHealthAssessor HEALTH_ASSESSOR =
            new VectorIndexHealthAssessor(0.15, 0.30);

    @Test
    void constructorRejectsNullCoordinatorId() {
        assertThrows(IllegalArgumentException.class,
                () -> scheduler(null));
    }

    @Test
    void constructorRejectsBlankCoordinatorId() {
        assertThrows(IllegalArgumentException.class,
                () -> scheduler("  "));
    }

    @Test
    void constructorAcceptsValidCoordinatorId() {
        assertDoesNotThrow(() -> scheduler("coord-1"));
    }

    @Test
    void scheduleRebuild_skippedWhenRebuildNotRecommended() throws Exception {
        IndexRebuildScheduler scheduler = scheduler("coord-1");
        IndexDescriptor d = vectorDescriptor(IndexState.READY);
        // 20% staleness — above warning (15%) but below critical (30%)
        VectorIndexHealthAssessor.HealthAssessment assessment = HEALTH_ASSESSOR.assess(d, 100, 80);
        assertFalse(assessment.rebuildRecommended());

        IndexRebuildScheduler.RebuildResult result =
                scheduler.scheduleRebuild(d, assessment).get();
        assertEquals(IndexRebuildAction.SKIPPED, result.action());
    }

    @Test
    void scheduleRebuild_failsForNonVectorIndex() {
        IndexRebuildScheduler scheduler = scheduler("coord-1");
        IndexDescriptor d = bloomDescriptor();
        VectorIndexHealthAssessor.HealthAssessment assessment =
                new VectorIndexHealthAssessor.HealthAssessment(
                        "idx-bloom", VectorIndexHealthLevel.CRITICAL,
                        0.9, "too stale", true);

        CompletableFuture<IndexRebuildScheduler.RebuildResult> future =
                scheduler.scheduleRebuild(d, assessment);
        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(IllegalArgumentException.class, ex.getCause());
    }

    @Test
    void scheduleRebuild_executesRebuildWhenCritical() throws Exception {
        CompletableFuture<Void> rebuildCalled = new CompletableFuture<>();
        IndexRebuildScheduler scheduler = new IndexRebuildScheduler(
                Executors.newSingleThreadExecutor(),
                null, "coord-1", HEALTH_ASSESSOR,
                (desc, action) -> {
                    rebuildCalled.complete(null);
                    return CompletableFuture.completedFuture(null);
                });

        IndexDescriptor d = vectorDescriptor(IndexState.READY);
        VectorIndexHealthAssessor.HealthAssessment assessment = HEALTH_ASSESSOR.assess(d, 100, 60);
        assertTrue(assessment.rebuildRecommended());

        IndexRebuildScheduler.RebuildResult result =
                scheduler.scheduleRebuild(d, assessment).get();
        assertEquals(IndexRebuildAction.FULL_REBUILD, result.action());
        assertTrue(rebuildCalled.isDone());
    }

    @Test
    void scheduleFromAssessments_returnsCountScheduled() {
        IndexRebuildScheduler scheduler = scheduler("coord-1");
        IndexDescriptor d1 = vectorDescriptor(IndexState.READY);
        IndexDescriptor d2 = vectorDescriptor(IndexState.READY);

        VectorIndexHealthAssessor.HealthAssessment critical = HEALTH_ASSESSOR.assess(d1, 100, 60);
        VectorIndexHealthAssessor.HealthAssessment warning = HEALTH_ASSESSOR.assess(d2, 100, 80);

        List<IndexRebuildScheduler.IndexWithAssessment> batch = List.of(
                new IndexRebuildScheduler.IndexWithAssessment(d1, critical),
                new IndexRebuildScheduler.IndexWithAssessment(d2, warning));

        int scheduled = scheduler.scheduleFromAssessments(batch);
        assertEquals(1, scheduled, "Only critical (rebuildRecommended=true) should be scheduled");
    }

    @Test
    void distributedLeaseUsed_whenLeaseAcquired() throws Exception {
        AlwaysGrantedLease lease = new AlwaysGrantedLease();
        IndexRebuildScheduler scheduler = new IndexRebuildScheduler(
                Executors.newSingleThreadExecutor(),
                lease, "coord-1", HEALTH_ASSESSOR,
                (desc, action) -> CompletableFuture.completedFuture(null));

        IndexDescriptor d = vectorDescriptor(IndexState.READY);
        VectorIndexHealthAssessor.HealthAssessment critical = HEALTH_ASSESSOR.assess(d, 100, 60);
        scheduler.scheduleRebuild(d, critical).get();

        assertTrue(lease.acquired, "Lease must be acquired before rebuild");
        assertTrue(lease.released, "Lease must be released after rebuild");
    }

    @Test
    void distributedLeaseBlocks_whenDenied() throws Exception {
        DenyingLease lease = new DenyingLease();
        IndexRebuildScheduler scheduler = new IndexRebuildScheduler(
                Executors.newSingleThreadExecutor(),
                lease, "coord-1", HEALTH_ASSESSOR,
                (desc, action) -> CompletableFuture.completedFuture(null));

        IndexDescriptor d = vectorDescriptor(IndexState.READY);
        VectorIndexHealthAssessor.HealthAssessment critical = HEALTH_ASSESSOR.assess(d, 100, 60);
        IndexRebuildScheduler.RebuildResult result =
                scheduler.scheduleRebuild(d, critical).get();

        assertEquals(IndexRebuildAction.SKIPPED, result.action());
    }

    @Test
    void leaseKeyPrefixMatchesConstant() {
        assertEquals("index_rebuild:", IndexRebuildScheduler.LEASE_KEY_PREFIX);
    }

    // ---- helpers ----

    private static IndexRebuildScheduler scheduler(String coordinatorId) {
        return new IndexRebuildScheduler(
                Executors.newSingleThreadExecutor(),
                null,
                coordinatorId,
                HEALTH_ASSESSOR,
                (desc, action) -> CompletableFuture.completedFuture(null));
    }

    private static IndexDescriptor vectorDescriptor(IndexState state) {
        Instant now = Instant.now();
        return new IndexDescriptor(
                "idx-vec-1", "test-vec-index", "cat", "schema", "orders",
                List.of("embedding"), IndexType.VECTOR, Map.of("algorithm", "HNSW"),
                state, null, 1, null, null,
                now, now, "default");
    }

    private static IndexDescriptor bloomDescriptor() {
        Instant now = Instant.now();
        return new IndexDescriptor(
                "idx-bloom", "test-bloom", "cat", "schema", "orders",
                List.of("order_id"), IndexType.BLOOM_FILTER, Map.of(),
                IndexState.READY, null, 1, null, null,
                now, now, "default");
    }

    private static final class AlwaysGrantedLease implements DistributedLease {
        boolean acquired;
        boolean released;

        @Override
        public Optional<LeaseEntry> tryAcquire(
                String leaseKey, String holderId, Duration duration) {
            acquired = true;
            return Optional.of(new LeaseEntry(
                    leaseKey, holderId, 1, Instant.now(), Instant.now().plus(duration)));
        }

        @Override
        public boolean tryRenew(
                String leaseKey, String holderId, long epoch, Duration duration) {
            return true;
        }

        @Override
        public boolean release(String leaseKey, String holderId, long epoch) {
            released = true;
            return true;
        }

        @Override
        public Optional<LeaseEntry> currentHolder(String leaseKey) {
            return Optional.empty();
        }

    }

    private static final class DenyingLease implements DistributedLease {
        @Override
        public Optional<LeaseEntry> tryAcquire(
                String leaseKey, String holderId, Duration duration) {
            return Optional.empty();
        }

        @Override
        public boolean tryRenew(
                String leaseKey, String holderId, long epoch, Duration duration) {
            return false;
        }

        @Override
        public boolean release(String leaseKey, String holderId, long epoch) {
            return false;
        }

        @Override
        public Optional<LeaseEntry> currentHolder(String leaseKey) {
            return Optional.empty();
        }

    }
}
