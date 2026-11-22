package org.pangrova.coordinator.scheduler.placement;

import org.pangrova.scheduler.api.resource.SpillBudget;


import org.pangrova.scheduler.api.exception.NoAvailableWorkerException;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.scheduler.api.enums.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.strategies.placement.LoadBalancedStrategy;
import org.pangrova.common.enums.MemoryPressure;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.coordination.api.heartbeat.CacheCatalogSummary;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for the fit-filter bypass in {@link DefaultWorkerSelector}: select/selectExcluding
 * previously passed a {@code 0L} estimate, which made {@code TaskScheduler.isMemoryAcceptable}
 * fall through to the pressure-only heuristic and admit workers that cannot hold even a minimal
 * task. The selector now passes the {@code MIN_TASK_MEMORY_BYTES} (64MB) floor so under-provisioned
 * workers are excluded.
 */
class DefaultWorkerSelectorMemoryFitTest {

    private static final long MIN_TASK_MEMORY_BYTES = 64L * 1024 * 1024;

    @Test
    void selectExcludesWorkerBelowMinTaskMemoryFloor() {
        WorkerManager wm = buildWorkerManager();
        // Available memory below the 64MB floor: must be excluded even though pressure is NONE.
        wm.registerWorker(worker("tiny", MIN_TASK_MEMORY_BYTES - 1));
        DefaultWorkerSelector selector = newSelector(wm);

        assertThatThrownBy(() -> selector.select(null))
                .isInstanceOf(NoAvailableWorkerException.class);
    }

    @Test
    void selectAdmitsWorkerAtOrAboveFloor() {
        WorkerManager wm = buildWorkerManager();
        wm.registerWorker(worker("ok", MIN_TASK_MEMORY_BYTES));
        DefaultWorkerSelector selector = newSelector(wm);

        WorkerInfo chosen = selector.select(null);

        assertThat(chosen.workerId()).isEqualTo("ok");
    }

    @Test
    void selectExcludingAppliesMemoryFloorToo() {
        WorkerManager wm = buildWorkerManager();
        wm.registerWorker(worker("tiny", MIN_TASK_MEMORY_BYTES - 1));
        DefaultWorkerSelector selector = newSelector(wm);

        assertThatThrownBy(() -> selector.selectExcluding(null, "some-other-worker"))
                .isInstanceOf(NoAvailableWorkerException.class);
    }

    @Test
    void registeredWorkerWithoutResourceHeartbeatDoesNotInflateStageCapacity() {
        WorkerManager wm = buildWorkerManager();
        wm.registerWorker(worker("ready", 2L * 1024 * 1024 * 1024));
        wm.registerWorker(worker("registered-only", 0));
        DefaultWorkerSelector selector = newSelector(wm);

        assertThat(wm.getActiveWorkers()).hasSize(2);
        assertThat(selector.activeWorkerCount()).isEqualTo(1);
        var grant = selector.computeResourceLimit(1, 1, wm.getWorker("ready"));
        assertThat(grant.getNativeMemoryBytes() + grant.getShuffleBufferBytes())
                .isEqualTo(2L * 1024 * 1024 * 1024);
        assertThat(grant.getShuffleBufferBytes()).isEqualTo(32L * 1024 * 1024);
    }

    @Test
    void resourceGrantFitsTheExactSelectedWorker() {
        WorkerManager wm = buildWorkerManager();
        wm.registerWorker(worker("large", 2L * 1024 * 1024 * 1024));
        wm.registerWorker(worker("small", 512L * 1024 * 1024));
        DefaultWorkerSelector selector = newSelector(wm);

        var small = selector.computeResourceLimit(1, 1, wm.getWorker("small"));
        var large = selector.computeResourceLimit(1, 1, wm.getWorker("large"));
        assertThat(small.getNativeMemoryBytes() + small.getShuffleBufferBytes())
                .isEqualTo(512L * 1024 * 1024);
        assertThat(large.getNativeMemoryBytes() + large.getShuffleBufferBytes())
                .isEqualTo(2L * 1024 * 1024 * 1024);
    }

    @Test
    void noResourceHeartbeatFailsStageGrantClosed() {
        WorkerManager wm = buildWorkerManager();
        wm.registerWorker(worker("registered-only", 0));
        DefaultWorkerSelector selector = newSelector(wm);

        assertThat(selector.activeWorkerCount()).isZero();
        assertThatThrownBy(() -> selector.computeResourceLimit(
                1, 1, wm.getWorker("registered-only")))
                .isInstanceOf(NoAvailableWorkerException.class);
    }

    @Test
    void selectedWorkerIsRevalidatedAfterItsAvailableMemoryFalls() {
        WorkerManager wm = buildWorkerManager();
        wm.registerWorker(worker("selected", 2L * 1024 * 1024 * 1024));
        WorkerInfo staleSelection = wm.getWorker("selected");
        DefaultWorkerSelector selector = newSelector(wm);
        wm.onHeartbeat(new org.pangrova.coordination.api.membership.WorkerRegistrationIdentity("selected", wm.getWorker("selected").workerExecutionIdentity().admissionDomainId(), wm.getWorker("selected").workerExecutionIdentity().incarnationId()), new WorkerManager.HeartbeatData(
                MemoryPressure.CRITICAL, 1.0, 1, 0));

        assertThatThrownBy(() -> selector.computeResourceLimit(
                1, 1, staleSelection))
                .isInstanceOf(NoAvailableWorkerException.class);
    }

    @Test
    void cacheAffinityCannotBypassMemoryAdmission() {
        WorkerManager wm = buildWorkerManager();
        String path = "/data/cached.parquet";
        wm.registerWorker(worker(
                "tiny",
                MIN_TASK_MEMORY_BYTES - 1,
                new CacheCatalogSummary(
                        List.of(path), 1, 1, 1, 0)));
        wm.registerWorker(worker("fit", MIN_TASK_MEMORY_BYTES, null));
        CacheAffinityAssigner affinity = new CacheAffinityAssigner();
        affinity.updateTopology(wm.getActiveWorkers());
        DefaultWorkerSelector selector = new DefaultWorkerSelector(
                wm,
                new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.of("default")),
                affinity,
                new SpillBudget(0L));

        WorkerInfo selected = selector.selectWithCacheAffinity(
                null, List.of(path));

        assertThat(selected.workerId()).isEqualTo("fit");
    }

    @Test
    void exclusionNeverFallsBackToTheOnlyOrOnlyFittingWorker() {
        WorkerManager wm = buildWorkerManager();
        wm.registerWorker(worker("excluded", MIN_TASK_MEMORY_BYTES));
        var selector = newSelector(wm);
        assertThatThrownBy(() -> selector.selectExcluding(null, "excluded"))
                .isInstanceOf(NoAvailableWorkerException.class);
        wm.registerWorker(worker("too-small", MIN_TASK_MEMORY_BYTES - 1));
        assertThatThrownBy(() -> selector.selectExcluding(null, "excluded"))
                .isInstanceOf(NoAvailableWorkerException.class);
        wm.registerWorker(worker("alternative", MIN_TASK_MEMORY_BYTES));
        assertThat(selector.selectExcluding(null, "excluded").workerId()).isEqualTo("alternative");
    }

    @Test
    void defaultSelectorCannotIgnoreExclusionOrExactBindings() {
        var candidate = worker("fixed", MIN_TASK_MEMORY_BYTES);
        WorkerSelector selector = new WorkerSelector() {
            @Override public WorkerInfo select(String tenantId) { return candidate; }
            @Override public org.pangrova.protocol.v2.StageResourceGrant computeResourceLimit(
                    int parallelism, int concurrentStages, WorkerInfo selectedWorker) {
                throw new UnsupportedOperationException("Not used by selection contract test");
            }
        };
        assertThatThrownBy(() -> selector.selectExcluding(null, "fixed"))
                .isInstanceOf(NoAvailableWorkerException.class);
        assertThat(selector.selectExcluding(null, "different")).isSameAs(candidate);
        var unsupported = org.pangrova.protocol.v2.OperatorBinding.newBuilder()
                .setCapability(org.pangrova.protocol.v2.CapabilityRef.newBuilder()
                        .setCapabilityId("unavailable.test.capability").setSemanticVersion("1")
                        .setSignatureSha256(com.google.protobuf.ByteString.copyFrom(new byte[32])))
                .build();
        assertThatThrownBy(() -> selector.selectForBindings(null, List.of(), List.of(unsupported)))
                .isInstanceOf(NoAvailableWorkerException.class);
        assertThat(selector.selectForBindings(null, List.of(), List.of())).isSameAs(candidate);
    }

    @Test
    void selectsCdcWorkerFromMixedTopologyRecoveredFromMembership() {
        var registry = new InMemoryWorkerRegistry();
        var first = new WorkerManager(20_000, 60_000, registry);
        first.registerWorker(worker("general", 2 * MIN_TASK_MEMORY_BYTES));
        first.registerWorker(worker("cdc-small", MIN_TASK_MEMORY_BYTES - 1, null, true));
        first.registerWorker(worker("cdc-ready", MIN_TASK_MEMORY_BYTES, null, true));
        var recovered = new WorkerManager(20_000, 60_000, registry);
        recovered.loadFromRegistry();

        var selector = new DefaultWorkerSelector(recovered,
                new TaskScheduler(new LoadBalancedStrategy(), tenant -> java.util.Optional.empty()),
                null, new SpillBudget(0L));
        assertThat(selector.selectForCdc("tenant").workerId()).isEqualTo("cdc-ready");
    }

    @Test
    void rejectsCdcWhenOnlyGeneralWorkersAreEligible() {
        var manager = buildWorkerManager();
        manager.registerWorker(worker("general", 2 * MIN_TASK_MEMORY_BYTES));
        manager.registerWorker(worker("cdc-small", MIN_TASK_MEMORY_BYTES - 1, null, true));

        assertThatThrownBy(() -> newSelector(manager).selectForCdc("tenant"))
                .isInstanceOf(NoAvailableWorkerException.class)
                .hasMessageContaining("CDC backend");
    }

    private static DefaultWorkerSelector newSelector(WorkerManager wm) {
        return new DefaultWorkerSelector(
                wm,
                new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.of("default")),
                null,
                new SpillBudget(0L));
    }

    private static WorkerManager buildWorkerManager() {
        return new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
    }

    private static WorkerInfo worker(String id, long availableMemoryBytes) {
        return worker(id, availableMemoryBytes, null);
    }

    private static WorkerInfo worker(
            String id,
            long availableMemoryBytes,
            CacheCatalogSummary cacheSummary) {
        return worker(id, availableMemoryBytes, cacheSummary, false);
    }

    private static WorkerInfo worker(String id, long availableMemoryBytes,
                                     CacheCatalogSummary cacheSummary, boolean cdcIngestSupported) {
        return new WorkerInfo(
                id, "host-" + id, 8080, 9090, 9091,
                HealthState.ACTIVE, MemoryPressure.NONE, 0.1, 0,
                availableMemoryBytes, Instant.now(), 1.0,
                "default", 2, null, List.of(), cacheSummary,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(), 0,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(id), null,
                cdcIngestSupported);
    }
}
