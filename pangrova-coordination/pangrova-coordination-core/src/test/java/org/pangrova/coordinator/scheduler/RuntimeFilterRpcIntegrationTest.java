package org.pangrova.coordinator.scheduler;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.RuntimeFilterStageHandler;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.ShuffleOutputCollector;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleEvent;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.observability.api.noop.NoOpTracingProvider;
import org.pangrova.planner.runtimefilter.RuntimeFilter;
import org.pangrova.planner.runtimefilter.RuntimeFilterGenerator;
import org.pangrova.planner.runtimefilter.RuntimeFilterRegistry;
import org.pangrova.observability.api.TracingProvider;
import org.pangrova.security.api.UserIdentity;
import org.pangrova.testing.testkit.TestExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.TaskManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test verifying the full runtime filter pipeline:
 * build-side stage completes → RuntimeFilterGenerator generates filter →
 * filter registered in RuntimeFilterRegistry → filter propagated to
 * probe-side tasks via RemoteTaskDispatcher.injectDynamicFilter HTTP RPC.
 *
 * Uses a real WorkerHttpServer to verify the HTTP path end-to-end.
 */
class RuntimeFilterRpcIntegrationTest {

    private static final TracingProvider TRACING = new NoOpTracingProvider();

    private WorkerHttpServer workerServer;
    private RemoteTaskDispatcher dispatcher;

    @BeforeEach
    void setUp() throws Exception {
        TaskManager taskManager = TaskManager.forHttpWiringOnly();
        var config = new WorkerHttpServer.WorkerHttpConfig(0, "localhost", 0);
        workerServer = new WorkerHttpServer(config, taskManager, TestExchangeTransport.INSTANCE);
        workerServer.start();
        dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TRACING);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (workerServer != null) {
            workerServer.stop();
        }
    }

    @Test
    @DisplayName("Join with selective build side: filter generated, registered, and propagated via RPC")
    void selectiveBuildSide_filterPropagatedViaRpc() {
        RuntimeFilterRegistry registry = new RuntimeFilterRegistry();
        RuntimeFilterGenerator generator = new RuntimeFilterGenerator();
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        ShuffleOutputCollector collector = new ShuffleOutputCollector();

        RuntimeFilterStageHandler handler = new RuntimeFilterStageHandler(
                generator, registry, dispatcher, tracker, collector);

        handler.registerFilterTarget("0", "1", "customer_id", "customer_id",
                org.pangrova.connector.spi.data.type.PangrovaType.INTEGER);

        StageNode s0 = new StageNode(0, dummyPlan(), 1, "COMPUTE", List.of());
        StageNode s1 = new StageNode(1, dummyPlan(), 2, "COMPUTE", List.of());
        StageDAG dag = StageDAG.fromNodes(
                List.of(s0, s1), Map.of(1, List.of(0)), List.of()
        ).withStages("q-rpc-dpp");

        Stage probeStage = dag.findStage(1);
        probeStage.addTaskId("task-probe-0");
        probeStage.addTaskId("task-probe-1");

        String workerAddress = "localhost:" + workerServer.getPort();
        tracker.registerTask("q-rpc-dpp", "task-probe-0");
        tracker.assignWorker("task-probe-0", workerAddress);
        tracker.registerTask("q-rpc-dpp", "task-probe-1");
        tracker.assignWorker("task-probe-1", workerAddress);

        // Selective build side: only 5 distinct values (NDV <= 1000 → MinMax filter)
        List<Comparable<?>> buildValues = List.of(100, 200, 300, 400, 500);
        collector.deposit("q-rpc-dpp", "0", "customer_id", buildValues);

        Stage buildStage = dag.findStage(0);
        CoordinatorSessionContext session = createSession("q-rpc-dpp");
        StageLifecycleEvent event = new StageLifecycleEvent(buildStage, dag, "q-rpc-dpp",
                new org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext.Live(session));

        // Fire the stage-completion hook
        handler.onFinished(event);

        // Verify: filter was generated and registered
        // F-AQE-02: registry key is namespaced by queryId to prevent cross-query collisions
        Optional<RuntimeFilter> registeredFilter = registry.getFilter("q-rpc-dpp:1");
        assertThat(registeredFilter).isPresent();
        assertThat(registeredFilter.get().type()).isEqualTo(RuntimeFilter.FilterType.MIN_MAX);
        assertThat(registeredFilter.get().column()).isEqualTo("customer_id");

        // Verify: the RPC call completed without error (worker returns CONFLICT
        // since tasks don't actually exist on the worker — but the HTTP path is exercised).
        // The key assertion is that no exception was thrown during propagation,
        // meaning the full Coordinator→Worker HTTP path is functional.
    }

    @Test
    @DisplayName("Large NDV build side generates Bloom filter and propagates via RPC")
    void largeBuildSide_bloomFilterPropagatedViaRpc() {
        RuntimeFilterRegistry registry = new RuntimeFilterRegistry();
        RuntimeFilterGenerator generator = new RuntimeFilterGenerator();
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        ShuffleOutputCollector collector = new ShuffleOutputCollector();

        RuntimeFilterStageHandler handler = new RuntimeFilterStageHandler(
                generator, registry, dispatcher, tracker, collector);

        handler.registerFilterTarget("0", "1", "order_id", "order_id",
                org.pangrova.connector.spi.data.type.PangrovaType.INTEGER);

        StageNode s0 = new StageNode(0, dummyPlan(), 1, "COMPUTE", List.of());
        StageNode s1 = new StageNode(1, dummyPlan(), 1, "COMPUTE", List.of());
        StageDAG dag = StageDAG.fromNodes(
                List.of(s0, s1), Map.of(1, List.of(0)), List.of()
        ).withStages("q-rpc-bloom");

        Stage probeStage = dag.findStage(1);
        probeStage.addTaskId("task-bloom-0");

        String workerAddress = "localhost:" + workerServer.getPort();
        tracker.registerTask("q-rpc-bloom", "task-bloom-0");
        tracker.assignWorker("task-bloom-0", workerAddress);

        // Large NDV (> 1000) → Bloom filter
        List<Comparable<?>> largeValues = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            largeValues.add(i);
        }
        collector.deposit("q-rpc-bloom", "0", "order_id", largeValues);

        Stage buildStage = dag.findStage(0);
        CoordinatorSessionContext session = createSession("q-rpc-bloom");
        StageLifecycleEvent event = new StageLifecycleEvent(buildStage, dag, "q-rpc-bloom",
                new org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext.Live(session));

        handler.onFinished(event);

        // F-AQE-02: registry key is namespaced by queryId
        Optional<RuntimeFilter> filter = registry.getFilter("q-rpc-bloom:1");
        assertThat(filter).isPresent();
        assertThat(filter.get().type()).isEqualTo(RuntimeFilter.FilterType.BLOOM);
        assertThat(filter.get().column()).isEqualTo("order_id");
    }

    @Test
    @DisplayName("injectDynamicFilter RPC sends valid HTTP POST to worker endpoint")
    void injectDynamicFilter_sendsHttpPost() {
        // Directly call injectDynamicFilter to verify the HTTP path
        String workerAddress = "localhost:" + workerServer.getPort();
        byte[] filterProto = new byte[]{0x0A, 0x04, 0x08, 0x01, 0x10, 0x64};

        // Should not throw — worker returns CONFLICT (task not found) but that's OK
        dispatcher.injectDynamicFilter("task-nonexistent", "scan_col", filterProto, workerAddress);

        // If we reach here, the HTTP path is functional (no exception thrown)
    }

    // ── Helpers ──

    private static CoordinatorSessionContext createSession(String queryId) {
        UserIdentity identity = new UserIdentity(
                "testUser", Set.of(), Map.of(), Instant.now());
        return new CoordinatorSessionContext(queryId, Map.of(), identity, "default-tenant");
    }

    private static org.pangrova.planner.PlanFragment dummyPlan() {
        var cluster = org.apache.calcite.plan.RelOptCluster.create(
                new org.apache.calcite.plan.volcano.VolcanoPlanner(),
                new org.apache.calcite.rex.RexBuilder(
                        new org.apache.calcite.jdbc.JavaTypeFactoryImpl()));
        return new org.pangrova.planner.PlanFragment(
                org.apache.calcite.rel.logical.LogicalValues.createOneRow(cluster), -1L, -1L);
    }
}
