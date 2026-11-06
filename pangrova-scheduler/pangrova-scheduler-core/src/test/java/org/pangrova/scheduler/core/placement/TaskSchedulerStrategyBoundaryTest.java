package org.pangrova.scheduler.core.placement;

import org.junit.jupiter.api.Test;
import org.pangrova.common.value.MemoryPressure;
import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.scheduler.api.placement.PlacementWorker;
import java.util.List;
import java.util.Optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Contract tests deliberately supply alternate ranking decisions, without a Provider dependency. */
class TaskSchedulerStrategyBoundaryTest {
    @Test
    void tenantIdentityIsResolvedIndependentlyOfPoolIdentity() {
        var scheduler = new TaskScheduler(request -> Optional.of(request.candidates().get(0).workerId()),
                tenant -> {
                    assertThat(tenant).isEqualTo("tenant-a");
                    return Optional.of("pool-b");
                });
        var assigned = worker("assigned", MemoryPressure.NONE, 100, "pool-b", List.of());
        var misleading = worker("wrong", MemoryPressure.NONE, 100, "tenant-a", List.of());
        assertThat(scheduler.selectWorkerForTenant(1, List.of(misleading, assigned), "tenant-a"))
                .containsSame(assigned);
        assertThat(scheduler.selectWorkerForTenant(1, List.of(misleading), "tenant-a")).isEmpty();
    }

    @Test
    void explicitlySharedTenantCannotConsumeDedicatedPool() {
        var scheduler = new TaskScheduler(request -> Optional.of(request.candidates().get(0).workerId()),
                tenant -> Optional.empty());
        var shared = worker("shared", MemoryPressure.NONE, 100, null, List.of());
        var dedicated = worker("dedicated", MemoryPressure.NONE, 100, "private-pool", List.of());
        assertThat(scheduler.selectWorkerForTenant(1, List.of(dedicated, shared), "tenant-a"))
                .containsSame(shared);
        assertThat(scheduler.selectWorkerForTenant(1, List.of(dedicated), "tenant-a")).isEmpty();
    }

    @Test
    void tenantAuthorityFailureCannotFallBackToSharedWorkers() {
        var failure = new IllegalStateException("tenant authority unavailable");
        var scheduler = new TaskScheduler(request -> { throw new AssertionError("must not rank"); },
                tenant -> { throw failure; });
        assertThatThrownBy(() -> scheduler.selectWorkerForTenant(1,
                List.of(worker("shared", MemoryPressure.NONE, 100, null, List.of())), "tenant-a"))
                .isSameAs(failure);
    }

    @Test
    void alternateStrategyRunsAfterEligibilityFiltering() {
        var scheduler = new TaskScheduler(request -> {
            assertThat(request.candidates()).extracting(c -> c.workerId()).containsExactly("eligible");
            return Optional.of("eligible");
        }, ignoredTenantForPlacement -> java.util.Optional.empty());
        var eligible = worker("eligible", MemoryPressure.NONE, 100, "pool", List.of());
        var workers = List.of(worker("critical", MemoryPressure.CRITICAL, 100, "pool", List.of()),
                worker("small", MemoryPressure.NONE, 1, "pool", List.of()),
                worker("other", MemoryPressure.NONE, 100, "other", List.of()), eligible);
        assertThat(scheduler.selectWorker(10, workers, "pool")).containsSame(eligible);
    }

    @Test
    void alternateRankingCanSelectAnotherEligibleWorkerWithoutChangingCore() {
        var scheduler = new TaskScheduler(request -> Optional.of(
                request.candidates().get(request.candidates().size() - 1).workerId()), ignoredTenantForPlacement -> java.util.Optional.empty());
        var first = worker("a", MemoryPressure.NONE, 100, "pool", List.of());
        var last = worker("b", MemoryPressure.NONE, 100, "pool", List.of());
        assertThat(scheduler.selectWorker(1, List.of(first, last))).containsSame(last);
    }

    @Test
    void providerCannotReturnExcludedIdentity() {
        var scheduler = new TaskScheduler(request -> Optional.of("excluded"), ignoredTenantForPlacement -> java.util.Optional.empty());
        assertThatThrownBy(() -> scheduler.selectWorker(1,
                List.of(worker("eligible", MemoryPressure.NONE, 100, "pool", List.of()))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("outside the eligible request");
    }

    @Test
    void failuresAreNotHiddenByFallback() {
        var failure = new IllegalStateException("ranking failed");
        var scheduler = new TaskScheduler(request -> { throw failure; }, ignoredTenantForPlacement -> java.util.Optional.empty());
        assertThatThrownBy(() -> scheduler.selectWorkerWithLocality(1,
                List.of(worker("eligible", MemoryPressure.NONE, 100, "pool", List.of("file"))),
                "pool", List.of("file"))).isSameAs(failure);
    }

    @Test
    void localityNarrowsCandidateSetBeforeCallingStrategy() {
        var scheduler = new TaskScheduler(request -> {
            assertThat(request.candidates()).extracting(c -> c.workerId()).containsExactly("local");
            return Optional.of("local");
        }, ignoredTenantForPlacement -> java.util.Optional.empty());
        var local = worker("local", MemoryPressure.NONE, 100, "pool", List.of("file"));
        assertThat(scheduler.selectWorkerWithLocality(1,
                List.of(worker("remote", MemoryPressure.NONE, 100, "pool", List.of()), local),
                "pool", List.of("file"))).containsSame(local);
    }

    @Test
    void noEligibleWorkerDoesNotInvokeStrategy() {
        var scheduler = new TaskScheduler(request -> { throw new AssertionError("must not rank an empty set"); }, ignoredTenantForPlacement -> java.util.Optional.empty());
        assertThat(scheduler.selectWorker(1, List.of())).isEmpty();
        assertThat(scheduler.selectWorker(1,
                List.of(worker("w", MemoryPressure.NONE, 100, "other", List.of())), "missing")).isEmpty();
    }

    private static Worker worker(String id, MemoryPressure pressure, long memory, String pool, List<String> locality) {
        return new Worker(id, "host", 8080, HealthState.ACTIVE, pressure, .1, 0, memory, pool, null, locality);
    }

    private record Worker(String workerId, String host, int httpPort, HealthState healthState,
                          MemoryPressure memoryPressure, double cpuUtilization, int runningTaskCount,
                          long availableMemoryBytes, String resourcePoolId, String rackId,
                          List<String> dataLocality) implements PlacementWorker {}
}
