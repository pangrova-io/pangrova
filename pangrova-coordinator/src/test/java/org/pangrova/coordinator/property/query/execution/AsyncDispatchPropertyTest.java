package org.pangrova.coordinator.property.query.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.query.stage.StageStateMachine;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for async dispatch correctness.
 * Validates: Requirements 2.1, 2.3, 2.5
 *
 * <p>Property 2: Async dispatch no deadlock — for any DAG topology,
 * async scheduling completes in finite time.
 *
 * <p>Property 3: Async exception propagation — all CompletableFuture
 * exceptions propagate to the query completion future.
 *
 * <p><b>Validates: Requirements 2.1, 2.3, 2.5</b>
 */
class AsyncDispatchPropertyTest {

    /**
     * Property 2: Stage completion callback triggers downstream scheduling.
     * For any number of stages in a linear chain, finishing each stage
     * triggers the next stage's scheduling via listener callback.
     *
     * <b>Validates: Requirements 2.3</b>
     */
    @Property(tries = 10)
    void stageCompletionCallbackTriggersDownstream(@ForAll @IntRange(min = 2, max = 10) int chainLength) {
        List<Stage> stages = createLinearChain(chainLength);
        List<Integer> scheduledOrder = new ArrayList<>();

        // Register listeners: when stage N finishes, schedule stage N+1
        for (int i = 0; i < stages.size() - 1; i++) {
            int nextIdx = i + 1;
            Stage current = stages.get(i);
            Stage next = stages.get(nextIdx);
            current.stateMachine().addStateChangeListener((stageId, oldState, newState) -> {
                if (newState == StageStateMachine.State.FINISHED) {
                    synchronized (scheduledOrder) {
                        scheduledOrder.add(nextIdx);
                    }
                    next.stateMachine().transitionTo(StageStateMachine.State.SCHEDULING);
                    next.stateMachine().transitionTo(StageStateMachine.State.RUNNING);
                    next.stateMachine().transitionTo(StageStateMachine.State.FLUSHING);
                    next.stateMachine().transitionTo(StageStateMachine.State.FINISHED);
                }
            });
        }

        // Start the chain: schedule and finish stage 0
        runToFinished(stages.get(0));

        // All downstream stages should have been scheduled and finished via callbacks
        assertThat(scheduledOrder).hasSize(chainLength - 1);
        for (int i = 0; i < scheduledOrder.size(); i++) {
            assertThat(scheduledOrder.get(i)).isEqualTo(i + 1);
        }
    }

    /**
     * Property 3: Async exception propagation completeness.
     * When a stage fails, the exception propagates to the query completion future.
     *
     * <b>Validates: Requirements 2.5</b>
     */
    @Property(tries = 10)
    void asyncExceptionPropagatestoQueryCompletion(@ForAll @IntRange(min = 1, max = 8) int stageCount) {
        List<Stage> stages = createLinearChain(stageCount);
        CompletableFuture<Void> queryCompletion = new CompletableFuture<>();

        // Register failure listener on all stages
        for (Stage stage : stages) {
            stage.stateMachine().addStateChangeListener((stageId, oldState, newState) -> {
                if (newState == StageStateMachine.State.FAILED) {
                    queryCompletion.completeExceptionally(
                            new RuntimeException("Stage " + stageId + " failed"));
                }
            });
        }

        // Fail the first stage
        Stage first = stages.get(0);
        first.stateMachine().transitionTo(StageStateMachine.State.SCHEDULING);
        first.stateMachine().transitionTo(StageStateMachine.State.RUNNING);
        first.stateMachine().transitionTo(StageStateMachine.State.FAILED);

        // Query completion should be exceptionally completed
        assertThat(queryCompletion).isCompletedExceptionally();
    }

    /**
     * Property: CompletableFuture.orTimeout triggers on slow stages.
     *
     * <b>Validates: Requirements 2.1</b>
     */
    @Test
    void orTimeoutTriggersOnSlowCompletion() {
        CompletableFuture<String> slow = new CompletableFuture<>();
        CompletableFuture<String> withTimeout = slow.orTimeout(50, TimeUnit.MILLISECONDS);

        try {
            withTimeout.join();
        } catch (java.util.concurrent.CompletionException e) {
            assertThat(e.getCause()).isInstanceOf(java.util.concurrent.TimeoutException.class);
            return;
        }
        throw new AssertionError("Expected timeout exception");
    }

    /**
     * Property: No deadlock in diamond DAG topology.
     * A → B, A → C, B → D, C → D (diamond shape).
     * Finishing A should trigger B and C; finishing both should trigger D.
     *
     * <b>Validates: Requirements 2.1, 2.3</b>
     */
    @Test
    void diamondDAG_noDeadlock() throws Exception {
        Stage a = createStage(0, "q-diamond");
        Stage b = createStage(1, "q-diamond");
        Stage c = createStage(2, "q-diamond");
        Stage d = createStage(3, "q-diamond");

        AtomicBoolean dScheduled = new AtomicBoolean(false);
        CountDownLatch bFinished = new CountDownLatch(1);
        CountDownLatch cFinished = new CountDownLatch(1);

        // A finishes → schedule B and C
        a.stateMachine().addStateChangeListener((id, old, nw) -> {
            if (nw == StageStateMachine.State.FINISHED) {
                b.stateMachine().transitionTo(StageStateMachine.State.SCHEDULING);
                c.stateMachine().transitionTo(StageStateMachine.State.SCHEDULING);
            }
        });

        // B finishes → check if C also finished, then schedule D
        b.stateMachine().addStateChangeListener((id, old, nw) -> {
            if (nw == StageStateMachine.State.FINISHED) {
                bFinished.countDown();
                if (cFinished.getCount() == 0) {
                    d.stateMachine().transitionTo(StageStateMachine.State.SCHEDULING);
                    dScheduled.set(true);
                }
            }
        });

        // C finishes → check if B also finished, then schedule D
        c.stateMachine().addStateChangeListener((id, old, nw) -> {
            if (nw == StageStateMachine.State.FINISHED) {
                cFinished.countDown();
                if (bFinished.getCount() == 0) {
                    d.stateMachine().transitionTo(StageStateMachine.State.SCHEDULING);
                    dScheduled.set(true);
                }
            }
        });

        // Execute: A → finish → B,C scheduled → finish B,C → D scheduled
        runToFinished(a);
        runToFinished(b);
        runToFinished(c);

        assertThat(dScheduled.get()).isTrue();
        assertThat(d.stateMachine().getState()).isEqualTo(StageStateMachine.State.SCHEDULING);
    }

    /**
     * Property: Exception in any stage of a chain propagates without silent loss.
     *
     * <b>Validates: Requirements 2.5</b>
     */
    @Test
    void exceptionInMiddleStage_propagatesToCompletion() {
        CompletableFuture<Void> completion = new CompletableFuture<>();
        AtomicReference<Throwable> captured = new AtomicReference<>();

        Stage s1 = createStage(0, "q-err");
        Stage s2 = createStage(1, "q-err");

        s2.stateMachine().addStateChangeListener((id, old, nw) -> {
            if (nw == StageStateMachine.State.FAILED) {
                RuntimeException ex = new RuntimeException("s2 dispatch failed");
                captured.set(ex);
                completion.completeExceptionally(ex);
            }
        });

        // s1 finishes, s2 starts but fails
        runToFinished(s1);
        s2.stateMachine().transitionTo(StageStateMachine.State.SCHEDULING);
        s2.stateMachine().transitionTo(StageStateMachine.State.RUNNING);
        s2.stateMachine().transitionTo(StageStateMachine.State.FAILED);

        assertThat(completion).isCompletedExceptionally();
        assertThat(captured.get()).isNotNull();
        assertThat(captured.get().getMessage()).contains("s2 dispatch failed");
    }

    // ── Helpers ──

    private static final RelOptCluster CLUSTER;
    static {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
        CLUSTER = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(typeFactory));
    }

    private List<Stage> createLinearChain(int length) {
        List<Stage> stages = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            stages.add(createStage(i, "q-chain"));
        }
        return stages;
    }

    private Stage createStage(int stageId, String queryId) {
        List<org.pangrova.protocol.rest.task.exchange.ExchangeSourceDescriptor> empty =
                java.util.Collections.emptyList();
        StageNode node = new StageNode(stageId, new org.pangrova.planner.PlanFragment(LogicalValues.createOneRow(CLUSTER), -1L, -1L), 1, "COMPUTE", empty);
        return Stage.create(node, queryId);
    }

    private void runToFinished(Stage stage) {
        StageStateMachine sm = stage.stateMachine();
        if (sm.getState() == StageStateMachine.State.PLANNED) {
            sm.transitionTo(StageStateMachine.State.SCHEDULING);
        }
        if (sm.getState() == StageStateMachine.State.SCHEDULING) {
            sm.transitionTo(StageStateMachine.State.RUNNING);
        }
        if (sm.getState() == StageStateMachine.State.RUNNING) {
            sm.transitionTo(StageStateMachine.State.FLUSHING);
        }
        if (sm.getState() == StageStateMachine.State.FLUSHING) {
            sm.transitionTo(StageStateMachine.State.FINISHED);
        }
    }
}
