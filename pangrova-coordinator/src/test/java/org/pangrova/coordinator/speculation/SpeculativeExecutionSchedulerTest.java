package org.pangrova.coordinator.speculation;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the periodic speculation scheduler triggers straggler detection
 * for single-stage queries without requiring a stage-finish event.
 */
class SpeculativeExecutionSchedulerTest {

    private static final String QUERY_ID = "q-single-stage";
    private static final String STAGE_ID = "1";

    private CoordinatorTaskTracker taskTracker;
    private SpeculativeTaskLauncher launcher;
    private SpeculativeStageHandler stageHandler;
    private QueryManager queryManager;
    private SpeculativeExecutionScheduler scheduler;

    @BeforeEach
    void setUp() {
        taskTracker = new CoordinatorTaskTracker();
        launcher = new SpeculativeTaskLauncher();
        stageHandler = new SpeculativeStageHandler(
                new SpeculationConfig(true, SpeculationConfig.DEFAULT_DETECTION_INTERVAL_MS),
                new StragglerDetector(), launcher, taskTracker);
        queryManager = buildMinimalQueryManager();
    }

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.close();
        }
    }

    @Test
    void singleStageQuery_slowTask_triggersSpeculationWithinTenSeconds() throws Exception {
        // Register a query with the query manager so getActiveQueryIds() returns it
        QueryStateMachine sm = new QueryStateMachine();
        queryManager.registerStateMachine(QUERY_ID, sm);

        // Set up 4 tasks in a single stage: 3 fast (completed), 1 slow (straggler)
        Instant fastStart = Instant.now().minusSeconds(2);
        Instant slowStart = Instant.now().minusSeconds(60);

        for (String id : List.of("t1", "t2", "t3")) {
            taskTracker.registerTask(QUERY_ID, id);
            taskTracker.markScheduled(QUERY_ID, id);
            taskTracker.updateTaskState(QUERY_ID, id, "RUNNING");
            taskTracker.recordTaskStart(id, STAGE_ID, fastStart);
        }
        // Mark fast tasks as finished (75% completion threshold met)
        for (String id : List.of("t1", "t2", "t3")) {
            taskTracker.updateTaskState(QUERY_ID, id, "FINISHED");
        }

        // Slow task: running for 60s (well above median*1.5 threshold and 10s minimum)
        taskTracker.registerTask(QUERY_ID, "ts");
        taskTracker.markScheduled(QUERY_ID, "ts");
        taskTracker.updateTaskState(QUERY_ID, "ts", "RUNNING");
        taskTracker.recordTaskStart("ts", STAGE_ID, slowStart);
        taskTracker.assignWorker("ts", "worker-a");
        taskTracker.setDeadline("ts", Instant.now().plusSeconds(300));

        // Start the scheduler with a short interval (100ms for test speed)
        scheduler = new SpeculativeExecutionScheduler(
                stageHandler, queryManager, taskTracker, 100L);
        scheduler.start();

        // Wait for speculation to trigger (should happen within a few scheduler ticks)
        boolean speculated = awaitCondition(
                () -> !launcher.getOriginalTasksWithCopies().isEmpty(), 10_000L);

        assertThat(speculated)
                .as("Periodic scheduler should detect straggler and launch speculative copy within 10s")
                .isTrue();
        assertThat(launcher.getOriginalTasksWithCopies()).contains("ts");
    }

    @Test
    void schedulerClosesGracefully() {
        scheduler = new SpeculativeExecutionScheduler(
                stageHandler, queryManager, taskTracker, 500L);
        scheduler.start();
        scheduler.close();
        // Second close should not throw (idempotent)
        scheduler.close();
    }

    @Test
    void noActiveQueries_noErrors() throws Exception {
        // No queries registered — scheduler should run without errors
        scheduler = new SpeculativeExecutionScheduler(
                stageHandler, queryManager, taskTracker, 100L);
        scheduler.start();
        Thread.sleep(300);
        // No exception means success — scheduler handles empty query set gracefully
        scheduler.close();
    }

    private boolean awaitCondition(java.util.function.BooleanSupplier condition, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(50);
        }
        return condition.getAsBoolean();
    }

    private QueryManager buildMinimalQueryManager() {
        var scorer = new WorkerPerformanceScorer();
        var taskScheduler = new TaskScheduler(scorer);
        var wm = new org.pangrova.coordinator.worker.WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        var rtd = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());
        var ctx = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null, rtd, taskScheduler,
                wm, taskTracker, null, null, null, null, 0L);
        var dispatcher = new QueryDispatcher(ctx, null, null);
        return new QueryManager(QueryManagerContext.minimal(dispatcher, new NoOpTracingProvider()));
    }
}
