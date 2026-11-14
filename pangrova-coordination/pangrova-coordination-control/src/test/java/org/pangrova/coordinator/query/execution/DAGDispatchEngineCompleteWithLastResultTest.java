package org.pangrova.coordinator.query.execution;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DAGCompletionEvaluator;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.common.client.result.ResultEndpoint;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates BUG-C02 fix: {@code DAGDispatchEngine.completeWithLastResult()} must
 * complete the future in all three branches — rootId hit, fallback, and empty results.
 */
class DAGDispatchEngineCompleteWithLastResultTest {

    @Test
    @DisplayName("Normal path: rootId result exists → future completed normally")
    void rootIdResultExists() throws Exception {
        ConcurrentHashMap<Integer, DispatchResult> results = new ConcurrentHashMap<>();
        DispatchResult expected = dispatchResult("worker-1", "task-root");
        results.put(1, expected);

        StageDAG dag = buildSingleOutputDAG(1);
        CompletableFuture<DispatchResult> completion = new CompletableFuture<>();

        invokeCompleteWithLastResult(results, dag, completion);

        assertTrue(completion.isDone());
        assertFalse(completion.isCompletedExceptionally());
        assertSame(expected, completion.get());
    }

    @Test
    @DisplayName("Fallback path: rootId missing but results non-empty → future completed with first result")
    void rootIdMissingButResultsNonEmpty() throws Exception {
        ConcurrentHashMap<Integer, DispatchResult> results = new ConcurrentHashMap<>();
        DispatchResult fallback = dispatchResult("worker-2", "task-fallback");
        results.put(99, fallback);

        StageDAG dag = buildSingleOutputDAG(1);
        CompletableFuture<DispatchResult> completion = new CompletableFuture<>();

        invokeCompleteWithLastResult(results, dag, completion);

        assertTrue(completion.isDone());
        assertFalse(completion.isCompletedExceptionally());
        assertSame(fallback, completion.get());
    }

    @Test
    @DisplayName("Empty results: → future completed exceptionally with PangrovaException")
    void emptyResultsCompletesExceptionally() throws Exception {
        ConcurrentHashMap<Integer, DispatchResult> results = new ConcurrentHashMap<>();

        StageDAG dag = buildSingleOutputDAG(1);
        CompletableFuture<DispatchResult> completion = new CompletableFuture<>();

        invokeCompleteWithLastResult(results, dag, completion);

        assertTrue(completion.isDone());
        assertTrue(completion.isCompletedExceptionally());

        ExecutionException ex = assertThrows(ExecutionException.class, completion::get);
        assertInstanceOf(PangrovaException.class, ex.getCause());
        PangrovaException ae = (PangrovaException) ex.getCause();
        assertEquals(PangrovaErrorCode.RUNTIME_TASK_FAILED, ae.getErrorCode());
        assertTrue(ae.getMessage().contains("no dispatch result collected"));
    }

    // --- BUG-D18: DAG topology sink-node detection ---

    @Test
    @DisplayName("D18: Multi-stage DAG without OUTPUT — selects sink node (no downstream), not highest stageId")
    void multiStageDAGSelectsSinkNodeNotHighestStageId() throws Exception {
        // DAG: stage 1 (leaf, SCAN) → stage 3 (intermediate, AGGREGATE, highest id) → stage 2 (sink, JOIN)
        // edges: stage 2 depends on [3], stage 3 depends on [1]
        // Sink = stage 2 (nothing depends on it). Highest id = 3 (intermediate).
        // Old code would pick stage 3 (wrong). New code picks stage 2 (correct).
        StageNode scan = new StageNode(1, dummyFragment(), 1, "SCAN", List.of());
        StageNode agg = new StageNode(3, dummyFragment(), 1, "AGGREGATE", List.of());
        StageNode join = new StageNode(2, dummyFragment(), 1, "JOIN", List.of());

        Map<Integer, List<Integer>> edges = Map.of(
                3, List.of(1),   // stage 3 depends on stage 1
                2, List.of(3)    // stage 2 depends on stage 3
        );
        StageDAG dag = StageDAG.fromNodes(List.of(scan, agg, join), edges, List.of()).withStages("q-d18");

        ConcurrentHashMap<Integer, DispatchResult> results = new ConcurrentHashMap<>();
        DispatchResult sinkResult = dispatchResult("worker-sink", "task-sink");
        DispatchResult intermediateResult = dispatchResult("worker-mid", "task-mid");
        results.put(2, sinkResult);
        results.put(3, intermediateResult);

        CompletableFuture<DispatchResult> completion = new CompletableFuture<>();
        invokeCompleteWithLastResult(results, dag, completion);

        assertTrue(completion.isDone());
        assertFalse(completion.isCompletedExceptionally());
        assertSame(sinkResult, completion.get(),
                "Should select sink node (stage 2) result, not highest stageId (stage 3)");
    }

    @Test
    @DisplayName("D18: findSinkStageId returns the stage with no downstream consumers")
    void findSinkStageIdReturnsCorrectSink() throws Exception {
        // DAG: 1 → 2 → 3 (linear chain, sink = 3)
        StageNode s1 = new StageNode(1, dummyFragment(), 1, "SCAN", List.of());
        StageNode s2 = new StageNode(2, dummyFragment(), 1, "AGGREGATE", List.of());
        StageNode s3 = new StageNode(3, dummyFragment(), 1, "JOIN", List.of());

        Map<Integer, List<Integer>> edges = Map.of(
                2, List.of(1),
                3, List.of(2)
        );
        StageDAG dag = StageDAG.fromNodes(List.of(s1, s2, s3), edges, List.of()).withStages("q-sink");

        int sinkId = invokeFindSinkStageId(dag);
        assertEquals(3, sinkId, "Sink should be stage 3 (no stage depends on it)");
    }

    @Test
    @DisplayName("D18: findSinkStageId with diamond DAG returns correct sink")
    void findSinkStageIdDiamondDAG() throws Exception {
        // Diamond: 1 → 3, 2 → 3 (two leaves, one sink = 3)
        StageNode s1 = new StageNode(1, dummyFragment(), 1, "SCAN", List.of());
        StageNode s2 = new StageNode(2, dummyFragment(), 1, "SCAN", List.of());
        StageNode s3 = new StageNode(3, dummyFragment(), 1, "JOIN", List.of());

        Map<Integer, List<Integer>> edges = Map.of(
                3, List.of(1, 2)
        );
        StageDAG dag = StageDAG.fromNodes(List.of(s1, s2, s3), edges, List.of()).withStages("q-diamond");

        int sinkId = invokeFindSinkStageId(dag);
        assertEquals(3, sinkId, "Sink should be stage 3 (both 1 and 2 feed into it)");
    }

    @Test
    @DisplayName("D18: OUTPUT taskType still takes priority over topology")
    void outputTaskTypeTakesPriorityOverTopology() throws Exception {
        // Stage 1 (SCAN) → Stage 2 (OUTPUT, but not highest id) → Stage 3 has no deps but is not OUTPUT
        // OUTPUT should still win even if topology says something else
        StageNode scan = new StageNode(1, dummyFragment(), 1, "SCAN", List.of());
        StageNode output = new StageNode(2, dummyFragment(), 1, "OUTPUT", List.of());

        Map<Integer, List<Integer>> edges = Map.of(
                2, List.of(1)
        );
        StageDAG dag = StageDAG.fromNodes(List.of(scan, output), edges, List.of()).withStages("q-output");

        ConcurrentHashMap<Integer, DispatchResult> results = new ConcurrentHashMap<>();
        DispatchResult outputResult = dispatchResult("worker-out", "task-out");
        results.put(2, outputResult);

        CompletableFuture<DispatchResult> completion = new CompletableFuture<>();
        invokeCompleteWithLastResult(results, dag, completion);

        assertTrue(completion.isDone());
        assertSame(outputResult, completion.get(),
                "OUTPUT taskType should take priority over topology detection");
    }

    // --- helpers ---

    private static void invokeCompleteWithLastResult(
            ConcurrentHashMap<Integer, DispatchResult> results,
            StageDAG dag,
            CompletableFuture<DispatchResult> completion) {
        DAGCompletionEvaluator.completeWithLastResult(
                results, dag, completion, List.of());
    }

    private static int invokeFindSinkStageId(StageDAG dag) throws Exception {
        Method method = DAGCompletionEvaluator.class.getDeclaredMethod(
                "findSinkStageId", StageDAG.class);
        method.setAccessible(true);
        return (int) method.invoke(null, dag);
    }

    private static StageDAG buildSingleOutputDAG(int outputStageId) {
        StageNode output = new StageNode(outputStageId, dummyFragment(), 1, "OUTPUT", List.of());
        return StageDAG.fromNodes(List.of(output), Map.of(), List.of()).withStages("q-test");
    }

    private static DispatchResult dispatchResult(String workerHost, String taskId) {
        return new DispatchResult(workerHost, taskId,
                new ResultEndpoint("localhost", 8080, "ticket-1", "http"));
    }

    private static org.pangrova.planner.analysis.PlanFragment dummyFragment() {
        var cluster = org.apache.calcite.plan.RelOptCluster.create(
                new org.apache.calcite.plan.volcano.VolcanoPlanner(),
                new org.apache.calcite.rex.RexBuilder(
                        new org.apache.calcite.jdbc.JavaTypeFactoryImpl()));
        return new org.pangrova.planner.analysis.PlanFragment(
                org.apache.calcite.rel.logical.LogicalValues.createOneRow(cluster), -1L, -1L);
    }
}
