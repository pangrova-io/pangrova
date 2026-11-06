package org.pangrova.server.coordinator.property.dispatch;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.value.MemoryPressure;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.config.YamlConfigLoader;
import org.pangrova.coordinator.scheduler.placement.DefaultWorkerSelector;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.scheduler.api.resource.SpillBudget;
import org.pangrova.scheduler.builtin.placement.LoadBalancedStrategy;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrapConfig;
import org.pangrova.server.coordinator.bootstrap.PangrovaCoordinatorMain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Configuration through Server assembly and placement to the canonical per-task grant. */
class SpillBudgetPropagationPropertyTest {

    @Property(tries = 100)
    void spillBudgetValuesPreservedInResourceLimit(
            @ForAll("spillBudgets") SpillBudget budget,
            @ForAll("parallelisms") int parallelism,
            @ForAll("concurrentStages") int concurrentStages) {

        WorkerManager wm = buildWorkerManager();
        registerWorker(wm);
        TaskScheduler scheduler = new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty());
        DefaultWorkerSelector selector = new DefaultWorkerSelector(wm, scheduler, null, budget);

        StageResourceGrant payload = selector.computeResourceLimit(
                parallelism, concurrentStages, wm.getWorker("worker-1"));

        assertThat(payload.getSpillBytes())
                .as("maxSpillBytes must propagate unchanged")
                .isEqualTo(budget.maxSpillBytes());
    }

    @Property(tries = 100)
    void configuredBudgetReachesTheActualTaskGrant(@ForAll("spillBudgets") SpillBudget budget) {
        var config = PangrovaCoordinatorMain.buildBootstrapConfig(PangrovaConfiguration.fromYaml(
                Map.of("coordinator.task.spill.max-bytes", Long.toString(budget.maxSpillBytes()))));
        assertThat(grant(config).getSpillBytes()).isEqualTo(budget.maxSpillBytes());
    }

    @Test
    void realYamlAndWorkerLocalPathDoNotChangeTheTaskQuota(@TempDir Path directory) throws Exception {
        Path yaml = directory.resolve("standalone.yml");
        Files.writeString(yaml, """
                coordinator:
                  task:
                    spill:
                      max-bytes: 5368709120
                worker:
                  spill:
                    directory: /worker-owned/local-spill
                """);
        var config = PangrovaCoordinatorMain.buildBootstrapConfig(
                PangrovaConfiguration.fromYaml(YamlConfigLoader.load(yaml)));
        assertThat(grant(config).getSpillBytes()).isEqualTo(5L * 1024 * 1024 * 1024);
    }

    @Test
    void defaultsAndExplicitZeroDisableTaskSpilling() {
        for (var values : List.of(Map.<String, String>of(),
                Map.of("coordinator.task.spill.max-bytes", "0"),
                Map.of("worker.spill.directory", "/worker-owned/local-spill"))) {
            var config = PangrovaCoordinatorMain.buildBootstrapConfig(PangrovaConfiguration.fromYaml(values));
            assertThat(grant(config).getSpillBytes()).isZero();
        }
    }

    @Test
    void negativeOverflowAndMalformedQuotasFailBeforeStartup() {
        for (String value : List.of("-1", "-9223372036854775808", "9223372036854775808", "1.5", "unlimited")) {
            assertThatThrownBy(() -> PangrovaCoordinatorMain.buildBootstrapConfig(
                    PangrovaConfiguration.fromYaml(Map.of("coordinator.task.spill.max-bytes", value))))
                    .as("invalid task spill quota: %s", value).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> new SpillBudget(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fullSignedByteRangeIsPreservedWithoutNarrowing() {
        var config = PangrovaCoordinatorMain.buildBootstrapConfig(PangrovaConfiguration.fromYaml(
                Map.of("coordinator.task.spill.max-bytes", Long.toString(Long.MAX_VALUE))));
        assertThat(grant(config).getSpillBytes()).isEqualTo(Long.MAX_VALUE);
    }

    private static StageResourceGrant grant(CoordinatorBootstrapConfig config) {
        WorkerManager workers = buildWorkerManager();
        registerWorker(workers);
        var scheduler = new TaskScheduler(new LoadBalancedStrategy(), ignored -> java.util.Optional.empty());
        var selector = new DefaultWorkerSelector(workers, scheduler, null, config.spillBudget());
        return selector.computeResourceLimit(2, 1, workers.getWorker("worker-1"));
    }

    @Provide
    Arbitrary<SpillBudget> spillBudgets() {
        return Arbitraries.longs().between(0, 100L * 1024 * 1024 * 1024).map(SpillBudget::new);
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
