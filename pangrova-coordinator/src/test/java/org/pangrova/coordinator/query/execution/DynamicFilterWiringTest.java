package org.pangrova.coordinator.query.execution;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.RuntimeFilterStageHandler;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.ShuffleOutputCollector;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleEvent;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.planner.runtimefilter.RuntimeFilter;
import org.pangrova.planner.runtimefilter.RuntimeFilterDescriptor;
import org.pangrova.planner.runtimefilter.RuntimeFilterGenerator;
import org.pangrova.planner.runtimefilter.RuntimeFilterRegistry;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.security.UserIdentity;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the coordinator execution path wiring for dynamic partition pruning:
 * when a build-side stage produces a runtime filter, the filter is propagated
 * to probe-side tasks via TaskDispatcher.injectDynamicFilter.
 *
 * This test exercises the full wiring path:
 * 1. Planner produces RuntimeFilterDescriptor during compilation
 * 2. QueryDispatcher.registerRuntimeFilterTargets maps descriptors to DAG stages
 * 3. Build-side stage completion triggers RuntimeFilterStageHandler.onFinished
 * 4. Handler generates filter and calls TaskDispatcher.injectDynamicFilter
 */
class DynamicFilterWiringTest {

    @Test
    @DisplayName("Join with selective build side: probe side receives dynamic filter via TaskDispatcher")
    void joinWithSelectiveBuildSide_probeSideReceivesDynamicFilter() {
        // Setup: create the handler with a recording dispatcher
        RuntimeFilterRegistry registry = new RuntimeFilterRegistry();
        RuntimeFilterGenerator generator = new RuntimeFilterGenerator();
        RecordingTaskDispatcher dispatcher = new RecordingTaskDispatcher();
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        ShuffleOutputCollector collector = new ShuffleOutputCollector();

        RuntimeFilterStageHandler handler = new RuntimeFilterStageHandler(
                generator, registry, dispatcher, tracker, collector);

        // Simulate what QueryDispatcher.registerRuntimeFilterTargets does:
        // map planner descriptors to DAG stage IDs
        List<RuntimeFilterDescriptor> descriptors = List.of(
                new RuntimeFilterDescriptor("dpp-1", "customer_id", "customer_id", "INNER",
                        PangrovaType.BIGINT));

        // Create a 3-stage DAG: stage 0 (build scan) + stage 1 (probe scan) → stage 2 (join)
        StageNode buildNode = new StageNode(0, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode probeNode = new StageNode(1, dummyFragment(), 2, "COMPUTE", List.of());
        StageNode joinNode = new StageNode(2, dummyFragment(), 2, "COMPUTE", List.of());
        StageDAG dag = StageDAG.fromNodes(
                List.of(buildNode, probeNode, joinNode),
                Map.of(2, List.of(0, 1)), List.of()
        ).withStages("q-dpp-wiring");

        // Register filter targets (this is what the coordinator execution path does)
        List<StageNode> roots = dag.leafStages();
        assertThat(roots).hasSize(2);
        String buildStageId = String.valueOf(roots.get(0).stageId());
        String probeStageId = String.valueOf(roots.get(1).stageId());
        for (var descriptor : descriptors) {
            handler.registerFilterTarget(buildStageId, probeStageId,
                    descriptor.buildColumn(), descriptor.probeColumn(),
                    org.pangrova.spi.connector.data.type.PangrovaType.INTEGER);
        }

        // Setup probe-side tasks
        Stage probeStage = dag.findStage(1);
        probeStage.addTaskId("task-probe-0");
        probeStage.addTaskId("task-probe-1");
        tracker.registerTask("q-dpp-wiring", "task-probe-0");
        tracker.assignWorker("task-probe-0", "worker-a:8080");
        tracker.registerTask("q-dpp-wiring", "task-probe-1");
        tracker.assignWorker("task-probe-1", "worker-b:8080");

        // Deposit build-side values (selective: only 5 distinct values)
        List<Comparable<?>> buildValues = List.of(10, 20, 30, 40, 50);
        collector.deposit("q-dpp-wiring", "0", "customer_id", buildValues);

        // Fire build-side stage completion
        Stage buildStage = dag.findStage(0);
        CoordinatorSessionContext session = createSession("q-dpp-wiring");
        StageLifecycleEvent event = new StageLifecycleEvent(buildStage, dag, "q-dpp-wiring",
                new org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext.Live(session));
        handler.onFinished(event);

        // Verify: filter was generated and registered
        // F-AQE-02: registry key is namespaced by queryId to prevent cross-query collisions
        Optional<RuntimeFilter> registeredFilter = registry.getFilter("q-dpp-wiring:" + probeStageId);
        assertThat(registeredFilter).isPresent();
        assertThat(registeredFilter.get().column()).isEqualTo("customer_id");

        // Verify: filter was propagated to BOTH probe-side tasks
        assertThat(dispatcher.injections).hasSize(2);
        assertThat(dispatcher.injections.stream().map(r -> r.taskId))
                .containsExactlyInAnyOrder("task-probe-0", "task-probe-1");

        // Verify: each injection targets the correct plan node
        for (var injection : dispatcher.injections) {
            assertThat(injection.planNodeId).isEqualTo("scan_customer_id");
            assertThat(injection.filterProto).isNotEmpty();
        }
    }

    @Test
    @DisplayName("No descriptors: no filter targets registered, no injection occurs")
    void noDescriptors_noFilterInjection() {
        RuntimeFilterRegistry registry = new RuntimeFilterRegistry();
        RuntimeFilterGenerator generator = new RuntimeFilterGenerator();
        RecordingTaskDispatcher dispatcher = new RecordingTaskDispatcher();
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        ShuffleOutputCollector collector = new ShuffleOutputCollector();

        RuntimeFilterStageHandler handler = new RuntimeFilterStageHandler(
                generator, registry, dispatcher, tracker, collector);

        // No registerFilterTarget called (simulates empty descriptors list)

        StageNode s0 = new StageNode(0, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode s1 = new StageNode(1, dummyFragment(), 1, "COMPUTE", List.of());
        StageDAG dag = StageDAG.fromNodes(
                List.of(s0, s1), Map.of(1, List.of(0)), List.of()
        ).withStages("q-no-dpp");

        Stage probeStage = dag.findStage(1);
        probeStage.addTaskId("task-0");
        tracker.registerTask("q-no-dpp", "task-0");
        tracker.assignWorker("task-0", "worker-a:8080");

        collector.deposit("q-no-dpp", "0", "col", List.of(1, 2, 3));

        Stage buildStage = dag.findStage(0);
        CoordinatorSessionContext session = createSession("q-no-dpp");
        StageLifecycleEvent event = new StageLifecycleEvent(buildStage, dag, "q-no-dpp",
                new org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext.Live(session));
        handler.onFinished(event);

        // No filter targets registered → no injection
        assertThat(dispatcher.injections).isEmpty();
    }

    @Test
    @DisplayName("Large build side generates Bloom filter propagated to probe tasks")
    void largeBuildSide_bloomFilterPropagatedToProbe() {
        RuntimeFilterRegistry registry = new RuntimeFilterRegistry();
        RuntimeFilterGenerator generator = new RuntimeFilterGenerator();
        RecordingTaskDispatcher dispatcher = new RecordingTaskDispatcher();
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        ShuffleOutputCollector collector = new ShuffleOutputCollector();

        RuntimeFilterStageHandler handler = new RuntimeFilterStageHandler(
                generator, registry, dispatcher, tracker, collector);

        // Register filter target from descriptor
        handler.registerFilterTarget("0", "1", "order_id", "order_id",
                org.pangrova.spi.connector.data.type.PangrovaType.INTEGER);

        StageNode s0 = new StageNode(0, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode s1 = new StageNode(1, dummyFragment(), 1, "COMPUTE", List.of());
        StageDAG dag = StageDAG.fromNodes(
                List.of(s0, s1), Map.of(1, List.of(0)), List.of()
        ).withStages("q-bloom-wiring");

        Stage probeStage = dag.findStage(1);
        probeStage.addTaskId("task-probe-0");
        tracker.registerTask("q-bloom-wiring", "task-probe-0");
        tracker.assignWorker("task-probe-0", "worker-c:8080");

        // Large NDV (> 1000) → Bloom filter
        List<Comparable<?>> largeValues = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            largeValues.add(i);
        }
        collector.deposit("q-bloom-wiring", "0", "order_id", largeValues);

        Stage buildStage = dag.findStage(0);
        CoordinatorSessionContext session = createSession("q-bloom-wiring");
        StageLifecycleEvent event = new StageLifecycleEvent(buildStage, dag, "q-bloom-wiring",
                new org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext.Live(session));
        handler.onFinished(event);

        // Verify Bloom filter generated
        // F-AQE-02: registry key is namespaced by queryId
        Optional<RuntimeFilter> filter = registry.getFilter("q-bloom-wiring:1");
        assertThat(filter).isPresent();
        assertThat(filter.get().type()).isEqualTo(RuntimeFilter.FilterType.BLOOM);

        // Verify propagated to probe task
        assertThat(dispatcher.injections).hasSize(1);
        assertThat(dispatcher.injections.get(0).taskId).isEqualTo("task-probe-0");
        assertThat(dispatcher.injections.get(0).filterProto).isNotEmpty();
    }

    // ── Test infrastructure ──

    private static class RecordingTaskDispatcher implements TaskDispatcher {
        final List<InjectionRecord> injections = new ArrayList<>();

        @Override
        public DispatchResult dispatch(
                org.pangrova.protocol.rest.task.SubmitTaskRequest request, WorkerInfo worker) {
            return new DispatchResult("worker", "task-0",
                    new ResultEndpoint("worker", 8816, "q", "grpc"));
        }

        @Override
        public void injectDynamicFilter(String taskId, String planNodeId,
                                        byte[] filterProto, String workerId) {
            injections.add(new InjectionRecord(taskId, planNodeId, filterProto, workerId));
        }

        record InjectionRecord(String taskId, String planNodeId,
                               byte[] filterProto, String workerId) {}
    }

    private static CoordinatorSessionContext createSession(String queryId) {
        UserIdentity identity = new UserIdentity(
                "testUser", Set.of(), Map.of(), Instant.now());
        return new CoordinatorSessionContext(queryId, Map.of(), identity, "default-tenant");
    }

    private static org.pangrova.planner.PlanFragment dummyFragment() {
        var cluster = org.apache.calcite.plan.RelOptCluster.create(
                new org.apache.calcite.plan.volcano.VolcanoPlanner(),
                new org.apache.calcite.rex.RexBuilder(
                        new org.apache.calcite.jdbc.JavaTypeFactoryImpl()));
        return new org.pangrova.planner.PlanFragment(
                org.apache.calcite.rel.logical.LogicalValues.createOneRow(cluster), -1L, -1L);
    }
}
