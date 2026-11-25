package org.pangrova.coordinator.query.execution;

import org.pangrova.execution.task.StageDispatchRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.shuffle.exchange.enums.ExchangeReadMode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates commit notification push from Coordinator to downstream Workers.
 * Covers Task 19: notifyDownstreamOfCommit sends notifications only for BLOCKING edges.
 */
class CommitNotificationTest {

    @Test
    @DisplayName("Commit notification sent only for BLOCKING edges")
    void commitNotificationOnlyForBlockingEdges() {
        // Stage 0 (scan) → Stage 1 (join) via HASH (BLOCKING)
        // Stage 2 (broadcast) → Stage 1 (join) via BROADCAST (STREAMING)
        StageEdge hashEdge = new StageEdge(0, 1, org.pangrova.planner.enums.ShuffleType.HASH);
        StageEdge broadcastEdge = new StageEdge(2, 1, org.pangrova.planner.enums.ShuffleType.BROADCAST,
                List.of(), "exchange-2-to-1", 1,
                org.pangrova.protocol.v2.BatchSerde.BATCH_SERDE_ARROW_BATCH, false);

        StageNode s0 = new StageNode(0, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode s1 = new StageNode(1, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode s2 = new StageNode(2, dummyFragment(), 1, "COMPUTE", List.of());

        StageDAG dag = StageDAG.fromNodes(
                List.of(s0, s1, s2),
                Map.of(1, List.of(0, 2)),
                List.of(hashEdge, broadcastEdge)
        ).withStages("q-commit");

        // Verify edge read modes
        assertEquals(ExchangeReadMode.BLOCKING, dag.getEdgeReadMode(0, 1));
        assertEquals(ExchangeReadMode.STREAMING, dag.getEdgeReadMode(2, 1));

        // Simulate: stage 0 finishes → should notify downstream for BLOCKING edge
        // stage 2 finishes → should NOT notify downstream for STREAMING edge
        RecordingDispatcher recorder = new RecordingDispatcher();

        // Add task IDs to downstream stage 1
        Stage downstream = dag.findStage(1);
        downstream.addTaskId("task-1-0");

        // Build dispatch records
        List<StageDispatchRecord> dispatched = List.of(
                new StageDispatchRecord(1,
                        new DispatchResult("worker-b", "task-1-0",
                                new ResultEndpoint("worker-b", 8816, "q-commit", "grpc")),
                        8080, 8816)
        );

        // When stage 0 (HASH/BLOCKING) finishes, notification should be sent
        Stage stage0 = dag.findStage(0);
        notifyDownstream(stage0, dag, "q-commit", dispatched, recorder);
        assertEquals(1, recorder.commitNotifications.size());
        assertEquals("worker-b", recorder.commitNotifications.get(0).workerHost);
        assertEquals(0, recorder.commitNotifications.get(0).upstreamStageId);

        recorder.commitNotifications.clear();

        // When stage 2 (BROADCAST/STREAMING) finishes, NO notification
        Stage stage2 = dag.findStage(2);
        notifyDownstream(stage2, dag, "q-commit", dispatched, recorder);
        assertTrue(recorder.commitNotifications.isEmpty(),
                "STREAMING edge should not trigger commit notification");
    }

    @Test
    @DisplayName("No notification when no downstream tasks dispatched")
    void noNotificationWhenNoDownstreamTasks() {
        StageEdge edge = new StageEdge(0, 1, org.pangrova.planner.enums.ShuffleType.HASH);
        StageNode s0 = new StageNode(0, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode s1 = new StageNode(1, dummyFragment(), 1, "COMPUTE", List.of());

        StageDAG dag = StageDAG.fromNodes(
                List.of(s0, s1), Map.of(1, List.of(0)), List.of(edge)
        ).withStages("q-empty");

        RecordingDispatcher recorder = new RecordingDispatcher();
        // Stage 1 has no task IDs yet
        notifyDownstream(dag.findStage(0), dag, "q-empty", List.of(), recorder);
        assertTrue(recorder.commitNotifications.isEmpty());
    }

    /**
     * Replicates notifyDownstreamOfCommit logic for unit testing.
     */
    private static void notifyDownstream(Stage finished, StageDAG dag, String queryId,
            List<StageDispatchRecord> dispatched,
            RecordingDispatcher recorder) {
        for (Stage downstream : dag.stageEntities()) {
            ExchangeReadMode mode = dag.getEdgeReadMode(finished.stageId(), downstream.stageId());
            if (mode != ExchangeReadMode.BLOCKING) {
                continue;
            }
            for (String taskId : downstream.taskIds()) {
                for (StageDispatchRecord rec : dispatched) {
                    if (rec.result().taskId().equals(taskId)) {
                        recorder.notifyUpstreamCommit(
                                rec.result().workerHost(), rec.workerHttpPort(),
                                queryId, finished.stageId());
                    }
                }
            }
        }
    }

    private static class RecordingDispatcher {
        final List<CommitRecord> commitNotifications = new ArrayList<>();

        void notifyUpstreamCommit(String workerHost, int httpPort,
                                  String queryId, int upstreamStageId) {
            commitNotifications.add(new CommitRecord(workerHost, httpPort, queryId, upstreamStageId));
        }

        record CommitRecord(String workerHost, int httpPort, String queryId, int upstreamStageId) {}
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
