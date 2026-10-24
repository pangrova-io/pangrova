package org.pangrova.coordinator.property.dispatch;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig;
import org.pangrova.coordinator.query.execution.dispatch.dag.DefaultWorkerSelector;
import org.pangrova.coordinator.query.execution.dispatch.dag.SpillBudget;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 3: Spill configuration lossless propagation.
 *
 * <p>For any SpillBudget (arbitrary maxSpillBytes and spillDirectory values),
 * the byte quota propagates unchanged through DefaultWorkerSelector.computeResourceLimit
 * into the canonical StageResourceGrant. Spill paths remain node-local configuration.</p>
 *
 * <p><b>Validates: Requirements 3.2, 3.3</b></p>
 */
class SpillBudgetPropagationPropertyTest {

    @Property(tries = 100)
    void spillBudgetValuesPreservedInResourceLimit(
            @ForAll("spillBudgets") SpillBudget budget,
            @ForAll("parallelisms") int parallelism,
            @ForAll("concurrentStages") int concurrentStages) {

        WorkerManager wm = buildWorkerManager();
        registerWorker(wm);
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        SplitAssignmentManager sam = new SplitAssignmentManager();
        DefaultWorkerSelector selector = new DefaultWorkerSelector(wm, scheduler, sam, budget);

        StageResourceGrant payload = selector.computeResourceLimit(
                parallelism, concurrentStages, wm.getWorker("worker-1"));

        assertThat(payload.getSpillBytes())
                .as("maxSpillBytes must propagate unchanged")
                .isEqualTo(budget.maxSpillBytes());
    }

    @Property(tries = 100)
    void configSpillBudgetPreservedThroughBootstrapConfig(
            @ForAll("spillBudgets") SpillBudget budget) {

        var spillConfig = new CoordinatorBootstrapConfig.SpillConfig(
                budget.maxSpillBytes(), budget.spillDirectory());
        var queryConfig = new CoordinatorBootstrapConfig.QueryConfig(
                300, 10, 0, 600, 60, spillConfig);
        var config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig("test-coord", 8080, 30_000),
                CoordinatorBootstrapConfig.FeatureFlags.allDisabled(),
                queryConfig,
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null);

        SpillBudget result = config.spillBudget();

        assertThat(result.maxSpillBytes())
                .as("maxSpillBytes must survive config round-trip")
                .isEqualTo(budget.maxSpillBytes());
        assertThat(result.spillDirectory())
                .as("spillDirectory must survive config round-trip")
                .isEqualTo(budget.spillDirectory());
    }

    @Provide
    Arbitrary<SpillBudget> spillBudgets() {
        Arbitrary<Long> bytes = Arbitraries.longs().between(0, 100L * 1024 * 1024 * 1024);
        Arbitrary<String> dirs = Arbitraries.of(
                "", "/tmp/spill", "/data/spill/pangrova", "/mnt/nvme/spill");
        return Combinators.combine(bytes, dirs).as(SpillBudget::new);
    }

    @Provide
    Arbitrary<Integer> parallelisms() {
        return Arbitraries.integers().between(1, 64);
    }

    @Provide
    Arbitrary<Integer> concurrentStages() {
        return Arbitraries.integers().between(1, 16);
    }

    private static WorkerManager buildWorkerManager() {
        return new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
    }

    private static void registerWorker(WorkerManager wm) {
        wm.registerWorker(new WorkerInfo(
                "worker-1", "host-1", 8080, 9090, 9091,
                HealthState.ACTIVE, MemoryPressure.NONE, 0.1, 0,
                4L * 1024 * 1024 * 1024, Instant.now(), 1.0,
                "default", 2, null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("worker-1")));
    }
}
