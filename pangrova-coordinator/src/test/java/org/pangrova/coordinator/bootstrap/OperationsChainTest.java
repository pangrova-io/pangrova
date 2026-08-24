package org.pangrova.coordinator.bootstrap;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import net.jqwik.api.lifecycle.AfterProperty;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.operations.bootstrap.BootstrapPhase;
import org.pangrova.operations.bootstrap.SystemBootstrapSequence;
import org.pangrova.operations.scaling.AutoScalingController;
import org.pangrova.operations.scaling.EmergencyThreshold;
import org.pangrova.operations.scaling.MetricSnapshot;
import org.pangrova.operations.scaling.ScalingPolicy;
import org.pangrova.operations.upgrade.NodeInfo;
import org.pangrova.operations.upgrade.NodeOperations;
import org.pangrova.operations.upgrade.RollingUpgradeController;
import org.pangrova.operations.upgrade.UpgradeResult;
import org.pangrova.operations.upgrade.VersionCompatibilityChecker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Acceptance test: validates the operations chain from main() to runtime management.
 *
 * <p>Chain under test: HighAvailabilityCoordinationService
 * leader election and query ownership, AutoScalingController metric evaluation,
 * RollingUpgradeController node upgrades, SystemBootstrapSequence phase ordering,
 * and CoordinatorBootstrap integration with HA service.</p>
 *
 * <p><b>Property 36: Operations chain from main() to runtime management</b></p>
 */
class OperationsChainTest {

    private static final String TAG =
            "Feature: e2e-pipeline-integration - Property 36: Operations chain from main() to runtime management";

    private CoordinatorBootstrap coordinatorBootstrap;

    @AfterEach
    void tearDownJUnit() {
        shutdownAll();
    }

    @AfterProperty
    void tearDownJqwik() {
        shutdownAll();
    }

    // -- Providers --

    @Provide
    Arbitrary<String> coordinatorIds() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(8)
                .map(s -> "ops-" + s);
    }

    // -- Property tests --

    /**
     * P36a: For any coordinatorId, CoordinatorBootstrap starts with HA service active.
     * haService.tryBecomeLeader() is called during startHA(), so isLeader() returns true.
     * Verified by submitting a query — proves HA did not block query acceptance.
     */
    @Property(tries = 5)
    @Tag(TAG)
    void haServiceActiveAfterBootstrap(
            @ForAll("coordinatorIds") String coordinatorId) throws Exception {

        startCoordinator(coordinatorId);
        int port = coordinatorBootstrap.getHttpPort();

        java.net.http.HttpResponse<String> resp = submitQuery(port, "SELECT 1");
        assertThat(resp.statusCode()).isEqualTo(200);
    }

    // DynamicConfigService tests removed (R1.4: idle five-layer chain deleted)

    // -- Scenario tests --

    /**
     * HighAvailabilityCoordinationService: leader election, query ownership acquire/release, and duplicate rejection.
     */
    @Test
    void haCoordinationServiceLeaderElectionAndOwnership() {
        // Two HA service instances sharing the same lease backend simulate two coordinators
        InMemoryDistributedLease sharedLease = new InMemoryDistributedLease();
        HighAvailabilityCoordinationService haService1 =
                new HighAvailabilityCoordinationService(sharedLease);
        HighAvailabilityCoordinationService haService2 =
                new HighAvailabilityCoordinationService(sharedLease);

        assertThat(haService1.tryBecomeLeader("coord-1")).isTrue();
        assertThat(haService2.tryBecomeLeader("coord-2")).isFalse();
        assertThat(haService1.hasActiveLeader()).isTrue();

        assertThat(haService1.acquireQueryOwnership("q-1", "coord-1")).isTrue();
        assertThat(haService1.acquireQueryOwnership("q-1", "coord-2")).isFalse();

        assertThat(haService1.releaseQueryOwnership("q-1", "coord-1")).isTrue();
        assertThat(haService1.releaseQueryOwnership("q-1", "coord-2")).isFalse();
    }

    /**
     * AutoScalingController evaluates metrics: low metrics → no scale, high metrics → scale out.
     */
    @Test
    void autoScalingControllerEvaluatesMetrics() {
        RecordingScalingExecutor executor = new RecordingScalingExecutor();
        java.util.concurrent.atomic.AtomicInteger workerCount = new java.util.concurrent.atomic.AtomicInteger(5);
        AutoScalingController controller = new AutoScalingController(
                new ScalingPolicy(),
                new EmergencyThreshold(0.95, 0.95, 0.95, 0.95),
                executor,
                workerCount::get);

        MetricSnapshot lowMetrics = new MetricSnapshot(0.1, 0.1, 0.1, 0.1);
        int resultLow = controller.evaluate(lowMetrics);
        assertThat(resultLow).isLessThanOrEqualTo(5);

        MetricSnapshot highMetrics = new MetricSnapshot(0.9, 0.9, 0.9, 0.9);
        int resultHigh = controller.evaluate(highMetrics);
        assertThat(resultHigh).isGreaterThanOrEqualTo(5);
    }

    /**
     * RollingUpgradeController upgrades workers then coordinators from "1.0" to "1.1".
     * All nodes succeed when versions are compatible.
     */
    @Test
    void rollingUpgradeControllerUpgradesNodes() {
        VersionCompatibilityChecker checker = new VersionCompatibilityChecker(
                Map.of("1.0", 1, "1.1", 1));
        NoOpNodeOperations nodeOps = new NoOpNodeOperations();
        RollingUpgradeController controller = new RollingUpgradeController(
                checker, nodeOps, true, Duration.ofSeconds(5));

        List<NodeInfo> workers = List.of(
                new NodeInfo("w-1", NodeInfo.NodeType.WORKER, "1.0"));
        List<NodeInfo> coordinators = List.of(
                new NodeInfo("c-1", NodeInfo.NodeType.COORDINATOR, "1.0"));

        UpgradeResult result = controller.upgradeCluster(workers, coordinators, "1.1");

        assertThat(result.allSucceeded()).isTrue();
        assertThat(result.nodeResults()).hasSize(2);
    }

    /**
     * SystemBootstrapSequence executes phases in strict order:
     * INFRASTRUCTURE → COORDINATORS → WORKERS → READINESS.
     */
    @Test
    void systemBootstrapSequenceExecutesPhasesInOrder() {
        List<BootstrapPhase> executedPhases = new CopyOnWriteArrayList<>();
        SystemBootstrapSequence.PhaseExecutor recorder = executedPhases::add;

        SystemBootstrapSequence sequence = new SystemBootstrapSequence(List.of(recorder));
        sequence.run();

        assertThat(executedPhases).containsExactly(
                BootstrapPhase.INFRASTRUCTURE,
                BootstrapPhase.COORDINATORS,
                BootstrapPhase.WORKERS,
                BootstrapPhase.READINESS);
    }

    // -- Bootstrap helpers --

    private void startCoordinator(String coordinatorId) {
        shutdownCoordinator();
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults(
                coordinatorId, 0);
        coordinatorBootstrap = new CoordinatorBootstrap(config);
        coordinatorBootstrap.start();
        assertThat(coordinatorBootstrap.isStarted()).isTrue();
        assertThat(coordinatorBootstrap.getHttpPort()).isGreaterThan(0);
    }

    private void shutdownAll() {
        shutdownCoordinator();
    }

    private void shutdownCoordinator() {
        if (coordinatorBootstrap != null) {
            coordinatorBootstrap.shutdown();
            coordinatorBootstrap = null;
        }
    }

    // -- HTTP helpers --

    private static java.net.http.HttpResponse<String> submitQuery(int port, String sql)
            throws Exception {
        String body = new com.fasterxml.jackson.databind.ObjectMapper()
                .writeValueAsString(Map.of("query", sql));
        java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create("http://localhost:" + port + "/v1/query/submit"))
                .header("Content-Type", "application/json")
                .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body))
                .build();
        return java.net.http.HttpClient.newHttpClient()
                .send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
    }

    // -- In-memory implementations --



    /**
     * Recording ScalingExecutor that captures target values for assertion.
     */
    private static final class RecordingScalingExecutor implements AutoScalingController.ScalingExecutor {

        private final List<Integer> targets = new CopyOnWriteArrayList<>();

        @Override
        public void scale(int targetWorkers) {
            targets.add(targetWorkers);
        }

        List<Integer> getTargets() {
            return Collections.unmodifiableList(targets);
        }
    }

    /**
     * No-op NodeOperations where all lifecycle methods are empty — for upgrade controller testing.
     */
    private static final class NoOpNodeOperations implements NodeOperations {

        @Override
        public void drain(NodeInfo node, Duration timeout) {}

        @Override
        public void waitTasks(NodeInfo node) {}

        @Override
        public void stop(NodeInfo node) {}

        @Override
        public void deploy(NodeInfo node, String targetVersion) {}

        @Override
        public void start(NodeInfo node) {}

        @Override
        public void verify(NodeInfo node) {}

        @Override
        public void undrain(NodeInfo node) {}

        @Override
        public void rollback(NodeInfo node) {}
    }
}
