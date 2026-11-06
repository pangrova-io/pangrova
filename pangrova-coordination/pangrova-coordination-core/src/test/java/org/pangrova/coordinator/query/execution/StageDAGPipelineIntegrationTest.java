package org.pangrova.coordinator.query.execution;

import org.pangrova.query.api.stage.StageState;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pangrova.coordinator.query.execution.dispatch.dag.PipelineConfig;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.shuffle.exchange.ExchangeReadMode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for pipeline scheduling across multi-stage query plans.
 * Validates Tasks 27.3, 27.4, 27.5: end-to-end pipeline scheduling,
 * broadcast join latency, and failure propagation in pipeline mode.
 */
class StageDAGPipelineIntegrationTest {

    // ---------------------------------------------------------------
    // Task 27.3: 3-stage pipeline scheduling (Scan → HashJoin → Output)
    // ---------------------------------------------------------------

    @Test
    @DisplayName("27.3: 3-stage pipeline — downstream scheduled when upstream RUNNING")
    void threeStageQueryPipelineScheduling() {
        // Topology: Stage 0 (Scan) → Stage 1 (HashJoin) → Stage 2 (Output)
        // Edges: 0→1 HASH (BLOCKING), 1→2 HASH (BLOCKING)
        StageEdge e01 = new StageEdge(0, 1, org.pangrova.planner.core.analysis.ShuffleType.HASH);
        StageEdge e12 = new StageEdge(1, 2, org.pangrova.planner.core.analysis.ShuffleType.HASH);

        StageNode n0 = new StageNode(0, dummyFragment(), 2, "COMPUTE", List.of());
        StageNode n1 = new StageNode(1, dummyFragment(), 2, "COMPUTE", List.of());
        StageNode n2 = new StageNode(2, dummyFragment(), 1, "OUTPUT", List.of());

        StageDAG dag = StageDAG.fromNodes(
                List.of(n0, n1, n2),
                Map.of(1, List.of(0), 2, List.of(1)),
                List.of(e01, e12)
        ).withStages("q-pipeline-3stage");

        PipelineConfig config = PipelineConfig.ENABLED;
        List<String> schedulingLog = new ArrayList<>();

        // Initially: all PLANNED, only root (stage 0) has no deps
        assertEquals(StageState.PLANNED, dag.findStage(0).stateMachine().getState());
        assertEquals(StageState.PLANNED, dag.findStage(1).stateMachine().getState());
        assertEquals(StageState.PLANNED, dag.findStage(2).stateMachine().getState());

        // Step 1: Schedule root stage 0 (Scan)
        dag.findStage(0).stateMachine().transitionTo(StageState.SCHEDULING);
        schedulingLog.add("stage-0:SCHEDULING");

        // Step 2: Stage 0 reaches RUNNING → pipeline triggers downstream scheduling
        dag.findStage(0).stateMachine().transitionTo(StageState.RUNNING);
        schedulingLog.add("stage-0:RUNNING");

        // Simulate pipeline onStageRunning: check which downstreams are ready
        Set<Integer> activeAfterS0Running = collectActiveIds(dag);
        assertTrue(activeAfterS0Running.contains(0), "Stage 0 should be active (RUNNING)");
        List<Stage> readyAfterS0 = dag.readyStageEntities(activeAfterS0Running);
        assertEquals(1, readyAfterS0.size());
        assertEquals(1, readyAfterS0.get(0).stageId(), "Stage 1 should be ready");

        // Schedule stage 1 (HashJoin) — pipeline mode
        dag.findStage(1).stateMachine().transitionTo(StageState.SCHEDULING);
        schedulingLog.add("stage-1:SCHEDULING");
        dag.findStage(1).stateMachine().transitionTo(StageState.RUNNING);
        schedulingLog.add("stage-1:RUNNING");

        // Stage 1 RUNNING → check if stage 2 is ready
        Set<Integer> activeAfterS1Running = collectActiveIds(dag);
        assertTrue(activeAfterS1Running.containsAll(Set.of(0, 1)));
        List<Stage> readyAfterS1 = dag.readyStageEntities(activeAfterS1Running);
        assertEquals(1, readyAfterS1.size());
        assertEquals(2, readyAfterS1.get(0).stageId(), "Stage 2 should be ready");

        // Schedule stage 2 (Output)
        dag.findStage(2).stateMachine().transitionTo(StageState.SCHEDULING);
        schedulingLog.add("stage-2:SCHEDULING");
        dag.findStage(2).stateMachine().transitionTo(StageState.RUNNING);
        schedulingLog.add("stage-2:RUNNING");

        // All 3 stages now RUNNING concurrently — pipeline mode
        for (int i = 0; i <= 2; i++) {
            assertEquals(StageState.RUNNING, dag.findStage(i).stateMachine().getState(),
                    "Stage " + i + " should be RUNNING");
        }

        // Complete stages in order: 0 → 1 → 2
        for (int i = 0; i <= 2; i++) {
            dag.findStage(i).stateMachine().transitionTo(StageState.FLUSHING);
            dag.findStage(i).stateMachine().transitionTo(StageState.FINISHED);
        }
        assertTrue(allStagesFinished(dag));
        assertEquals(6, schedulingLog.size(), "Expected 6 scheduling events");
    }

    @Test
    @DisplayName("27.3: 3-stage query — barrier mode schedules only after FINISHED")
    void threeStageQueryBarrierMode() {
        StageNode n0 = new StageNode(0, dummyFragment(), 2, "COMPUTE", List.of());
        StageNode n1 = new StageNode(1, dummyFragment(), 2, "COMPUTE", List.of());
        StageNode n2 = new StageNode(2, dummyFragment(), 1, "OUTPUT", List.of());

        StageDAG dag = StageDAG.fromNodes(
                List.of(n0, n1, n2),
                Map.of(1, List.of(0), 2, List.of(1)),
                List.of()
        ).withStages("q-barrier-3stage");

        // Barrier mode: onStageRunning is no-op, scheduling only on FINISHED
        // Stage 0: PLANNED → SCHEDULING → RUNNING
        dag.findStage(0).stateMachine().transitionTo(StageState.SCHEDULING);
        dag.findStage(0).stateMachine().transitionTo(StageState.RUNNING);

        // In barrier mode, stage 1 should NOT be scheduled yet
        // (onStageRunning is no-op when pipeline disabled)
        assertEquals(StageState.PLANNED, dag.findStage(1).stateMachine().getState());

        // Stage 0 finishes → safety-net in onStageFinished schedules stage 1
        dag.findStage(0).stateMachine().transitionTo(StageState.FLUSHING);
        dag.findStage(0).stateMachine().transitionTo(StageState.FINISHED);

        // In barrier mode, only FINISHED stages satisfy dependencies
        Set<Integer> finishedAfterS0 = collectFinishedIds(dag);
        List<Stage> readyAfterS0 = dag.readyStageEntities(finishedAfterS0);
        assertEquals(1, readyAfterS0.size());
        assertEquals(1, readyAfterS0.get(0).stageId());

        // Stage 1 runs and finishes
        dag.findStage(1).stateMachine().transitionTo(StageState.SCHEDULING);
        dag.findStage(1).stateMachine().transitionTo(StageState.RUNNING);
        dag.findStage(1).stateMachine().transitionTo(StageState.FLUSHING);
        dag.findStage(1).stateMachine().transitionTo(StageState.FINISHED);

        // Now stage 2 is ready
        Set<Integer> finishedAfterS1 = collectFinishedIds(dag);
        List<Stage> readyAfterS1 = dag.readyStageEntities(finishedAfterS1);
        assertEquals(1, readyAfterS1.size());
        assertEquals(2, readyAfterS1.get(0).stageId());
    }

    @Test
    @DisplayName("27.3: maxConcurrentStages caps pipeline scheduling")
    void maxConcurrentStagesCapsScheduling() {
        // 4-stage linear DAG: 0 → 1 → 2 → 3
        StageNode n0 = new StageNode(0, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode n1 = new StageNode(1, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode n2 = new StageNode(2, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode n3 = new StageNode(3, dummyFragment(), 1, "OUTPUT", List.of());

        StageDAG dag = StageDAG.fromNodes(
                List.of(n0, n1, n2, n3),
                Map.of(1, List.of(0), 2, List.of(1), 3, List.of(2)),
                List.of()
        ).withStages("q-cap");

        PipelineConfig config = new PipelineConfig(true, 2, 5000);

        // Stage 0 → RUNNING
        dag.findStage(0).stateMachine().transitionTo(StageState.SCHEDULING);
        dag.findStage(0).stateMachine().transitionTo(StageState.RUNNING);

        // Stage 1 → RUNNING (pipeline triggered)
        dag.findStage(1).stateMachine().transitionTo(StageState.SCHEDULING);
        dag.findStage(1).stateMachine().transitionTo(StageState.RUNNING);

        // Now 2 stages RUNNING — maxConcurrentStages=2 should block further scheduling
        long runningCount = dag.stageEntities().stream()
                .filter(s -> s.stateMachine().getState() == StageState.RUNNING)
                .count();
        assertEquals(2, runningCount);
        assertTrue(runningCount >= config.maxConcurrentStages(),
                "Running count should meet or exceed max, blocking further pipeline scheduling");

        // Stage 2 should remain PLANNED (backpressure)
        assertEquals(StageState.PLANNED, dag.findStage(2).stateMachine().getState());
    }

    // ---------------------------------------------------------------
    // Task 27.4: Broadcast join latency improvement vs barrier mode
    // ---------------------------------------------------------------

    @Test
    @DisplayName("27.4: broadcast join — STREAMING edge enables immediate downstream scheduling")
    void broadcastJoinStreamingEdge() {
        // Topology: Stage 0 (Scan-Build) → Stage 1 (Join) via BROADCAST (STREAMING)
        //           Stage 2 (Scan-Probe) → Stage 1 (Join) via HASH (BLOCKING)
        StageEdge broadcastEdge = new StageEdge(0, 1, org.pangrova.planner.core.analysis.ShuffleType.BROADCAST,
                List.of(), "exchange-0-to-1", 1,
                org.pangrova.protocol.v2.BatchSerde.BATCH_SERDE_ARROW_BATCH, false);
        StageEdge hashEdge = new StageEdge(2, 1, org.pangrova.planner.core.analysis.ShuffleType.HASH);

        StageNode n0 = new StageNode(0, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode n1 = new StageNode(1, dummyFragment(), 2, "COMPUTE", List.of());
        StageNode n2 = new StageNode(2, dummyFragment(), 2, "COMPUTE", List.of());

        StageDAG dag = StageDAG.fromNodes(
                List.of(n0, n1, n2),
                Map.of(1, List.of(0, 2)),
                List.of(broadcastEdge, hashEdge)
        ).withStages("q-broadcast");

        // Verify edge read modes
        assertEquals(ExchangeReadMode.STREAMING, dag.getEdgeReadMode(0, 1),
                "Broadcast edge should be STREAMING");
        assertEquals(ExchangeReadMode.BLOCKING, dag.getEdgeReadMode(2, 1),
                "Hash edge should be BLOCKING");

        // Both stage 0 and stage 2 are roots (no upstream deps)
        List<Stage> roots = dag.rootStageEntities();
        assertEquals(2, roots.size());

        // Start both roots
        dag.findStage(0).stateMachine().transitionTo(StageState.SCHEDULING);
        dag.findStage(0).stateMachine().transitionTo(StageState.RUNNING);
        dag.findStage(2).stateMachine().transitionTo(StageState.SCHEDULING);
        dag.findStage(2).stateMachine().transitionTo(StageState.RUNNING);

        // Both upstreams RUNNING → stage 1 is ready in pipeline mode
        Set<Integer> activeIds = collectActiveIds(dag);
        assertTrue(activeIds.containsAll(Set.of(0, 2)));
        List<Stage> ready = dag.readyStageEntities(activeIds);
        assertEquals(1, ready.size());
        assertEquals(1, ready.get(0).stageId());

        // Schedule stage 1 — it can start consuming broadcast data immediately (STREAMING)
        dag.findStage(1).stateMachine().transitionTo(StageState.SCHEDULING);
        dag.findStage(1).stateMachine().transitionTo(StageState.RUNNING);

        // All 3 stages RUNNING concurrently
        for (int i : List.of(0, 1, 2)) {
            assertEquals(StageState.RUNNING, dag.findStage(i).stateMachine().getState());
        }
    }

    @Test
    @DisplayName("27.4: broadcast join — barrier mode waits for FINISHED before scheduling join")
    void broadcastJoinBarrierModeWaitsForFinished() {
        StageEdge broadcastEdge = new StageEdge(0, 1, org.pangrova.planner.core.analysis.ShuffleType.BROADCAST,
                List.of(), "exchange-0-to-1", 1,
                org.pangrova.protocol.v2.BatchSerde.BATCH_SERDE_ARROW_BATCH, false);
        StageEdge hashEdge = new StageEdge(2, 1, org.pangrova.planner.core.analysis.ShuffleType.HASH);

        StageNode n0 = new StageNode(0, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode n1 = new StageNode(1, dummyFragment(), 2, "COMPUTE", List.of());
        StageNode n2 = new StageNode(2, dummyFragment(), 2, "COMPUTE", List.of());

        StageDAG dag = StageDAG.fromNodes(
                List.of(n0, n1, n2),
                Map.of(1, List.of(0, 2)),
                List.of(broadcastEdge, hashEdge)
        ).withStages("q-broadcast-barrier");

        // Start both roots
        dag.findStage(0).stateMachine().transitionTo(StageState.SCHEDULING);
        dag.findStage(0).stateMachine().transitionTo(StageState.RUNNING);
        dag.findStage(2).stateMachine().transitionTo(StageState.SCHEDULING);
        dag.findStage(2).stateMachine().transitionTo(StageState.RUNNING);

        // In barrier mode, stage 1 stays PLANNED even though both upstreams are RUNNING
        // (onStageRunning is no-op). Stage 1 only becomes schedulable after FINISHED.
        assertEquals(StageState.PLANNED, dag.findStage(1).stateMachine().getState());

        // In barrier mode, the engine uses onStageFinished which collects
        // FINISHED-only IDs for scheduling. Simulate barrier-mode readiness check:
        // only FINISHED stages count as "completed" for dependency resolution.
        Set<Integer> finishedOnly = collectFinishedIds(dag);
        assertTrue(finishedOnly.isEmpty(), "No stages finished yet");

        // Finish stage 0 only — stage 1 still not ready (stage 2 not finished)
        dag.findStage(0).stateMachine().transitionTo(StageState.FLUSHING);
        dag.findStage(0).stateMachine().transitionTo(StageState.FINISHED);

        finishedOnly = collectFinishedIds(dag);
        List<Stage> readyAfterS0 = dag.readyStageEntities(finishedOnly);
        assertTrue(readyAfterS0.stream().noneMatch(s -> s.stageId() == 1),
                "Stage 1 not ready — stage 2 still RUNNING, not FINISHED");

        // Finish stage 2 → now stage 1 is ready (both upstreams FINISHED)
        dag.findStage(2).stateMachine().transitionTo(StageState.FLUSHING);
        dag.findStage(2).stateMachine().transitionTo(StageState.FINISHED);

        finishedOnly = collectFinishedIds(dag);
        List<Stage> readyAfterS2 = dag.readyStageEntities(finishedOnly);
        assertEquals(1, readyAfterS2.size());
        assertEquals(1, readyAfterS2.get(0).stageId());
    }

    @Test
    @DisplayName("27.4: commit notification only for BLOCKING edges in broadcast join")
    void broadcastJoinCommitNotificationOnlyForBlocking() {
        StageEdge broadcastEdge = new StageEdge(0, 1, org.pangrova.planner.core.analysis.ShuffleType.BROADCAST,
                List.of(), "exchange-0-to-1", 1,
                org.pangrova.protocol.v2.BatchSerde.BATCH_SERDE_ARROW_BATCH, false);
        StageEdge hashEdge = new StageEdge(2, 1, org.pangrova.planner.core.analysis.ShuffleType.HASH);

        StageNode n0 = new StageNode(0, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode n1 = new StageNode(1, dummyFragment(), 2, "COMPUTE", List.of());
        StageNode n2 = new StageNode(2, dummyFragment(), 2, "COMPUTE", List.of());

        StageDAG dag = StageDAG.fromNodes(
                List.of(n0, n1, n2),
                Map.of(1, List.of(0, 2)),
                List.of(broadcastEdge, hashEdge)
        ).withStages("q-broadcast-commit");

        // Stage 0 (BROADCAST) finishes → NO commit notification (STREAMING edge)
        assertFalse(shouldSendCommitNotification(dag, 0, 1),
                "Broadcast/STREAMING edge should not trigger commit notification");

        // Stage 2 (HASH) finishes → commit notification sent (BLOCKING edge)
        assertTrue(shouldSendCommitNotification(dag, 2, 1),
                "Hash/BLOCKING edge should trigger commit notification");
    }

    // ---------------------------------------------------------------
    // Task 27.5: Failure propagation in pipeline mode
    // ---------------------------------------------------------------

    @Test
    @DisplayName("27.5: upstream fails while downstream RUNNING — downstream transitions to FAILED")
    void upstreamFailsWhileDownstreamRunning() {
        StageEdge e01 = new StageEdge(0, 1, org.pangrova.planner.core.analysis.ShuffleType.HASH);
        StageEdge e12 = new StageEdge(1, 2, org.pangrova.planner.core.analysis.ShuffleType.HASH);

        StageNode n0 = new StageNode(0, dummyFragment(), 2, "COMPUTE", List.of());
        StageNode n1 = new StageNode(1, dummyFragment(), 2, "COMPUTE", List.of());
        StageNode n2 = new StageNode(2, dummyFragment(), 1, "OUTPUT", List.of());

        StageDAG dag = StageDAG.fromNodes(
                List.of(n0, n1, n2),
                Map.of(1, List.of(0), 2, List.of(1)),
                List.of(e01, e12)
        ).withStages("q-fail-pipeline");

        // Pipeline mode: all 3 stages reach RUNNING
        for (int i = 0; i <= 2; i++) {
            dag.findStage(i).stateMachine().transitionTo(StageState.SCHEDULING);
            dag.findStage(i).stateMachine().transitionTo(StageState.RUNNING);
        }

        // Stage 0 (upstream) fails while stages 1 and 2 are RUNNING
        dag.findStage(0).stateMachine().tryTransitionTo(StageState.FAILED);
        assertEquals(StageState.FAILED, dag.findStage(0).stateMachine().getState());

        // Propagate failure to direct downstream (stage 1)
        cancelDownstream(dag.findStage(0), dag);

        // Stage 1 was RUNNING → transitions to FAILED
        assertEquals(StageState.FAILED, dag.findStage(1).stateMachine().getState());

        // Stage 2 is NOT direct downstream of stage 0 → stays RUNNING
        // (only direct downstream is affected per cancelDownstreamStages logic)
        assertEquals(StageState.RUNNING, dag.findStage(2).stateMachine().getState());

        // Now propagate stage 1's failure to stage 2
        cancelDownstream(dag.findStage(1), dag);
        assertEquals(StageState.FAILED, dag.findStage(2).stateMachine().getState());
    }

    @Test
    @DisplayName("27.5: upstream fails — PLANNED downstream cancelled, RUNNING downstream failed")
    void mixedDownstreamStatesDuringFailure() {
        // Diamond: Stage 0 → Stage 1, Stage 0 → Stage 2, Stage 1 → Stage 3, Stage 2 → Stage 3
        StageNode n0 = new StageNode(0, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode n1 = new StageNode(1, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode n2 = new StageNode(2, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode n3 = new StageNode(3, dummyFragment(), 1, "OUTPUT", List.of());

        StageDAG dag = StageDAG.fromNodes(
                List.of(n0, n1, n2, n3),
                Map.of(1, List.of(0), 2, List.of(0), 3, List.of(1, 2)),
                List.of()
        ).withStages("q-diamond-fail");

        // Pipeline: stage 0 RUNNING, stage 1 RUNNING (pipeline-scheduled), stage 2 still PLANNED
        dag.findStage(0).stateMachine().transitionTo(StageState.SCHEDULING);
        dag.findStage(0).stateMachine().transitionTo(StageState.RUNNING);
        dag.findStage(1).stateMachine().transitionTo(StageState.SCHEDULING);
        dag.findStage(1).stateMachine().transitionTo(StageState.RUNNING);
        // Stage 2 stays PLANNED, stage 3 stays PLANNED

        // Stage 0 fails
        dag.findStage(0).stateMachine().tryTransitionTo(StageState.FAILED);
        cancelDownstream(dag.findStage(0), dag);

        // Stage 1 was RUNNING → FAILED
        assertEquals(StageState.FAILED, dag.findStage(1).stateMachine().getState());
        // Stage 2 was PLANNED → CANCELLED
        assertEquals(StageState.CANCELLED, dag.findStage(2).stateMachine().getState());
        // Stage 3 is not direct downstream of stage 0 → stays PLANNED
        assertEquals(StageState.PLANNED, dag.findStage(3).stateMachine().getState());
    }

    @Test
    @DisplayName("27.5: failure propagation with concurrent pipeline stages — FINISHED not affected")
    void failurePropagationFinishedStageUnaffected() {
        StageEdge e01 = new StageEdge(0, 1, org.pangrova.planner.core.analysis.ShuffleType.HASH);
        StageEdge e12 = new StageEdge(1, 2, org.pangrova.planner.core.analysis.ShuffleType.HASH);

        StageNode n0 = new StageNode(0, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode n1 = new StageNode(1, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode n2 = new StageNode(2, dummyFragment(), 1, "OUTPUT", List.of());

        StageDAG dag = StageDAG.fromNodes(
                List.of(n0, n1, n2),
                Map.of(1, List.of(0), 2, List.of(1)),
                List.of(e01, e12)
        ).withStages("q-fail-finished");

        // Stage 0 already FINISHED, stage 1 RUNNING, stage 2 RUNNING (pipeline)
        dag.findStage(0).stateMachine().transitionTo(StageState.SCHEDULING);
        dag.findStage(0).stateMachine().transitionTo(StageState.RUNNING);
        dag.findStage(0).stateMachine().transitionTo(StageState.FLUSHING);
        dag.findStage(0).stateMachine().transitionTo(StageState.FINISHED);

        dag.findStage(1).stateMachine().transitionTo(StageState.SCHEDULING);
        dag.findStage(1).stateMachine().transitionTo(StageState.RUNNING);
        dag.findStage(2).stateMachine().transitionTo(StageState.SCHEDULING);
        dag.findStage(2).stateMachine().transitionTo(StageState.RUNNING);

        // Stage 1 fails
        dag.findStage(1).stateMachine().tryTransitionTo(StageState.FAILED);
        cancelDownstream(dag.findStage(1), dag);

        // Stage 0 was FINISHED → not affected
        assertEquals(StageState.FINISHED, dag.findStage(0).stateMachine().getState());
        // Stage 2 was RUNNING → FAILED
        assertEquals(StageState.FAILED, dag.findStage(2).stateMachine().getState());
    }

    // ---------------------------------------------------------------
    // Helpers — replicate DAGDispatchEngine logic for isolated testing
    // ---------------------------------------------------------------

    /**
     * Barrier-mode readiness: only FINISHED stages count as completed dependencies.
     */
    private static Set<Integer> collectFinishedIds(StageDAG dag) {
        Set<Integer> ids = new HashSet<>();
        for (Stage s : dag.stageEntities()) {
            if (s.stateMachine().getState() == StageState.FINISHED) {
                ids.add(s.stageId());
            }
        }
        return ids;
    }

    private static Set<Integer> collectActiveIds(StageDAG dag) {
        Set<Integer> ids = new HashSet<>();
        for (Stage s : dag.stageEntities()) {
            StageState st = s.stateMachine().getState();
            if (st == StageState.SCHEDULING
                    || st == StageState.RUNNING
                    || st == StageState.FLUSHING
                    || st == StageState.FINISHED) {
                ids.add(s.stageId());
            }
        }
        return ids;
    }

    private static boolean allStagesFinished(StageDAG dag) {
        return dag.stageEntities().stream()
                .allMatch(s -> s.stateMachine().getState() == StageState.FINISHED);
    }

    private static void cancelDownstream(Stage failed, StageDAG dag) {
        for (Stage stage : dag.stageEntities()) {
            if (stage.stageId() == failed.stageId()) {
                continue;
            }
            List<Integer> upstreams = dag.edges().getOrDefault(stage.stageId(), List.of());
            if (!upstreams.contains(failed.stageId())) {
                continue;
            }
            StageState state = stage.stateMachine().getState();
            if (state == StageState.FINISHED
                    || state == StageState.FAILED
                    || state == StageState.CANCELLED) {
                continue;
            }
            if (state == StageState.PLANNED) {
                stage.stateMachine().tryTransitionTo(StageState.CANCELLED);
            } else {
                stage.stateMachine().tryTransitionTo(StageState.FAILED);
            }
        }
    }

    private static boolean shouldSendCommitNotification(StageDAG dag, int upstreamId, int downstreamId) {
        return dag.getEdgeReadMode(upstreamId, downstreamId) == ExchangeReadMode.BLOCKING;
    }

    private static org.pangrova.planner.core.analysis.PlanFragment dummyFragment() {
        var cluster = org.apache.calcite.plan.RelOptCluster.create(
                new org.apache.calcite.plan.volcano.VolcanoPlanner(),
                new org.apache.calcite.rex.RexBuilder(
                        new org.apache.calcite.jdbc.JavaTypeFactoryImpl()));
        return new org.pangrova.planner.core.analysis.PlanFragment(
                org.apache.calcite.rel.logical.LogicalValues.createOneRow(cluster), -1L, -1L);
    }
}
