package org.pangrova.coordinator.query.execution;

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
import org.pangrova.planner.runtimefilter.RuntimeFilterGenerator;
import org.pangrova.planner.runtimefilter.RuntimeFilterRegistry;
import org.pangrova.client.core.protocol.ResultEndpoint;
import org.pangrova.connector.spi.data.type.PangrovaType;
import org.pangrova.security.api.UserIdentity;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the runtime filter stage-completion hook correctly generates
 * filters from build-side data and propagates them to probe-side tasks.
 */
class RuntimeFilterStageHandlerTest {

    @Test
    @DisplayName("Build-side stage completion generates filter and injects into probe-side tasks")
    void buildSideCompletion_generatesAndPropagatesFilter() {
        RuntimeFilterRegistry registry = new RuntimeFilterRegistry();
        RuntimeFilterGenerator generator = new RuntimeFilterGenerator();
        RecordingTaskDispatcher dispatcher = new RecordingTaskDispatcher();
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        ShuffleOutputCollector collector = new ShuffleOutputCollector();

        RuntimeFilterStageHandler handler = new RuntimeFilterStageHandler(
                generator, registry, dispatcher, tracker, collector);

        handler.registerFilterTarget("0", "1", "customer_id", "customer_id", PangrovaType.BIGINT);

        StageNode s0 = new StageNode(0, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode s1 = new StageNode(1, dummyFragment(), 2, "COMPUTE", List.of());
        StageDAG dag = StageDAG.fromNodes(
                List.of(s0, s1), Map.of(1, List.of(0)), List.of()
        ).withStages("q-dpp");

        Stage probeStage = dag.findStage(1);
        probeStage.addTaskId("task-1-0");
        probeStage.addTaskId("task-1-1");
        tracker.registerTask("q-dpp", "task-1-0");
        tracker.assignWorker("task-1-0", "worker-a:8080");
        tracker.registerTask("q-dpp", "task-1-1");
        tracker.assignWorker("task-1-1", "worker-b:8080");

        List<Comparable<?>> buildValues = List.of(10, 20, 30, 40, 50);
        collector.deposit("q-dpp", "0", "customer_id", buildValues);

        Stage buildStage = dag.findStage(0);
        CoordinatorSessionContext session = createSession("q-dpp");
        StageLifecycleEvent event = new StageLifecycleEvent(buildStage, dag, "q-dpp",
                new org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext.Live(session));
        handler.onFinished(event);

        Optional<RuntimeFilter> registeredFilter = registry.getFilter("q-dpp:1");
        assertThat(registeredFilter).isPresent();
        assertThat(registeredFilter.get().type()).isEqualTo(RuntimeFilter.FilterType.MIN_MAX);
        assertThat(registeredFilter.get().column()).isEqualTo("customer_id");

        assertThat(dispatcher.injections).hasSize(2);
        assertThat(dispatcher.injections.stream().map(r -> r.taskId))
                .containsExactlyInAnyOrder("task-1-0", "task-1-1");
        assertThat(dispatcher.injections.get(0).planNodeId).isEqualTo("scan_customer_id");
        assertThat(dispatcher.injections.get(0).filterProto).isNotEmpty();
    }

    @Test
    @DisplayName("Non-build stage completion does not trigger filter generation")
    void nonBuildStage_noFilterGenerated() {
        RuntimeFilterRegistry registry = new RuntimeFilterRegistry();
        RuntimeFilterGenerator generator = new RuntimeFilterGenerator();
        RecordingTaskDispatcher dispatcher = new RecordingTaskDispatcher();
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        ShuffleOutputCollector collector = new ShuffleOutputCollector();

        RuntimeFilterStageHandler handler = new RuntimeFilterStageHandler(
                generator, registry, dispatcher, tracker, collector);
        handler.registerFilterTarget("0", "1", "id", "id", PangrovaType.BIGINT);

        StageNode s2 = new StageNode(2, dummyFragment(), 1, "COMPUTE", List.of());
        StageDAG dag = StageDAG.fromNodes(
                List.of(s2), Map.of(), List.of()
        ).withStages("q-no-dpp");

        Stage stage2 = dag.findStage(2);
        CoordinatorSessionContext session = createSession("q-no-dpp");
        StageLifecycleEvent event = new StageLifecycleEvent(stage2, dag, "q-no-dpp",
                new org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext.Live(session));
        handler.onFinished(event);

        assertThat(registry.getFilter("q-no-dpp:1")).isEmpty();
        assertThat(dispatcher.injections).isEmpty();
    }

    @Test
    @DisplayName("Empty build-side values skip filter generation")
    void emptyBuildValues_skipsFilterGeneration() {
        RuntimeFilterRegistry registry = new RuntimeFilterRegistry();
        RuntimeFilterGenerator generator = new RuntimeFilterGenerator();
        RecordingTaskDispatcher dispatcher = new RecordingTaskDispatcher();
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        ShuffleOutputCollector collector = new ShuffleOutputCollector();

        RuntimeFilterStageHandler handler = new RuntimeFilterStageHandler(
                generator, registry, dispatcher, tracker, collector);
        handler.registerFilterTarget("0", "1", "col", "col", PangrovaType.BIGINT);

        StageNode s0 = new StageNode(0, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode s1 = new StageNode(1, dummyFragment(), 1, "COMPUTE", List.of());
        StageDAG dag = StageDAG.fromNodes(
                List.of(s0, s1), Map.of(1, List.of(0)), List.of()
        ).withStages("q-empty");

        Stage buildStage = dag.findStage(0);
        CoordinatorSessionContext session = createSession("q-empty");
        StageLifecycleEvent event = new StageLifecycleEvent(buildStage, dag, "q-empty",
                new org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext.Live(session));
        handler.onFinished(event);

        assertThat(registry.getFilter("q-empty:1")).isEmpty();
        assertThat(dispatcher.injections).isEmpty();
    }

    @Test
    @DisplayName("Large NDV build side generates Bloom filter")
    void largeBuildSide_generatesBloomFilter() {
        RuntimeFilterRegistry registry = new RuntimeFilterRegistry();
        RuntimeFilterGenerator generator = new RuntimeFilterGenerator();
        RecordingTaskDispatcher dispatcher = new RecordingTaskDispatcher();
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        ShuffleOutputCollector collector = new ShuffleOutputCollector();

        RuntimeFilterStageHandler handler = new RuntimeFilterStageHandler(
                generator, registry, dispatcher, tracker, collector);
        handler.registerFilterTarget("0", "1", "order_id", "order_id", PangrovaType.BIGINT);

        StageNode s0 = new StageNode(0, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode s1 = new StageNode(1, dummyFragment(), 1, "COMPUTE", List.of());
        StageDAG dag = StageDAG.fromNodes(
                List.of(s0, s1), Map.of(1, List.of(0)), List.of()
        ).withStages("q-bloom");

        Stage probeStage = dag.findStage(1);
        probeStage.addTaskId("task-1-0");
        tracker.registerTask("q-bloom", "task-1-0");
        tracker.assignWorker("task-1-0", "worker-c:8080");

        List<Comparable<?>> largeValues = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            largeValues.add(i);
        }
        collector.deposit("q-bloom", "0", "order_id", largeValues);

        Stage buildStage = dag.findStage(0);
        CoordinatorSessionContext session = createSession("q-bloom");
        StageLifecycleEvent event = new StageLifecycleEvent(buildStage, dag, "q-bloom",
                new org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext.Live(session));
        handler.onFinished(event);

        Optional<RuntimeFilter> filter = registry.getFilter("q-bloom:1");
        assertThat(filter).isPresent();
        assertThat(filter.get().type()).isEqualTo(RuntimeFilter.FilterType.BLOOM);
        assertThat(filter.get().column()).isEqualTo("order_id");

        assertThat(dispatcher.injections).hasSize(1);
        assertThat(dispatcher.injections.get(0).taskId).isEqualTo("task-1-0");
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
                "testUser", Set.of(), Map.of(), java.time.Instant.now());
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
