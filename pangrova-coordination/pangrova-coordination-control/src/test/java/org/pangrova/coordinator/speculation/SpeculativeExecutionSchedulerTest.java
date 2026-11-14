package org.pangrova.coordinator.speculation;

import org.pangrova.coordinator.testinfra.QueryManagerTestContexts;

import org.pangrova.coordinator.query.scheduling.CoordinatorSpeculativeWorkerInventory;
import org.pangrova.coordinator.query.scheduling.CoordinatorSpeculativeQueryInventory;
import org.pangrova.coordinator.query.scheduling.CoordinatorSpeculativeTaskRegistry;
import org.pangrova.coordinator.query.scheduling.CoordinatorSpeculativeAttemptCanceller;
import org.pangrova.coordinator.query.scheduling.CoordinatorSpeculativeAttemptDispatcher;
import org.pangrova.scheduler.speculation.SpeculationConfig;
import org.pangrova.scheduler.speculation.SpeculativeExecutionScheduler;
import org.pangrova.scheduler.speculation.SpeculativeStageHandler;
import org.pangrova.scheduler.speculation.SpeculativeTaskLauncher;
import org.pangrova.scheduler.speculation.StragglerDetector;

import java.time.Instant;
import java.util.List;

import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskStateMachine;
import org.pangrova.scheduler.api.enums.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.builtin.placement.LoadBalancedStrategy;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.common.enums.MemoryPressure;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.RepeatedTest;

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
    private WorkerManager workerManager;

    @BeforeEach
    void setUp() {
        taskTracker = new CoordinatorTaskTracker();
        launcher = new SpeculativeTaskLauncher();
        stageHandler = new SpeculativeStageHandler(
                new SpeculationConfig(true, SpeculationConfig.DEFAULT_DETECTION_INTERVAL_MS),
                new StragglerDetector(), launcher, new CoordinatorSpeculativeTaskRegistry(taskTracker));
        workerManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        workerManager.registerWorker(worker("worker-a", "host-a"));
        workerManager.registerWorker(worker("worker-b", "host-b"));
        TaskDispatcher dispatch = (request, worker) -> new DispatchResult(
                worker.host(), request.taskRequest().getAttempt().getTaskId(),
                new ResultEndpoint(worker.host(), worker.httpPort(), "result", "grpc"));
        launcher.setDispatchDependencies(
                new CoordinatorSpeculativeAttemptDispatcher(dispatch),
                new CoordinatorSpeculativeWorkerInventory(workerManager),
                new CoordinatorSpeculativeTaskRegistry(taskTracker));
        launcher.setCancelRetrier(new CoordinatorSpeculativeAttemptCanceller(
                new org.pangrova.coordinator.task.TaskCancelRetrier(dispatch, taskTracker)));
        taskTracker.addExecutionDrainListener(launcher::executionDrained);
        queryManager = buildMinimalQueryManager();
    }

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.close();
        }
    }

    @RepeatedTest(10)
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
        taskTracker.registerTaskRequest("ts",
                org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                        "ts", QUERY_ID, 1, 0, TaskKind.TASK_KIND_READ,
                        "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8)));

        // Start the scheduler with a short interval (100ms for test speed)
        scheduler = new SpeculativeExecutionScheduler(
                stageHandler, new CoordinatorSpeculativeQueryInventory(queryManager),
                new CoordinatorSpeculativeTaskRegistry(taskTracker), 100L);
        scheduler.start();

        org.awaitility.Awaitility.await("speculative copy dispatched to its alternate worker")
                .atMost(java.time.Duration.ofSeconds(10)).untilAsserted(() -> {
                    assertThat(launcher.getOriginalTasksWithCopies()).contains("ts");
                    var copy = launcher.getSpeculativeTaskId("ts");
                    assertThat(copy).isPresent();
                    String copyId = copy.orElseThrow();
                    assertThat(taskTracker.getTaskIds(QUERY_ID)).contains(copyId);
                    assertThat(taskTracker.getTaskState(QUERY_ID, copyId))
                            .isEqualTo(TaskStateMachine.State.RUNNING);
                    assertThat(taskTracker.getWorkerForTask(copyId)).isEqualTo("worker-b");
                });
    }

    @Test
    void schedulerClosesGracefully() {
        scheduler = new SpeculativeExecutionScheduler(
                stageHandler, new CoordinatorSpeculativeQueryInventory(queryManager),
                new CoordinatorSpeculativeTaskRegistry(taskTracker), 500L);
        scheduler.start();
        scheduler.close();
        // Second close should not throw (idempotent)
        scheduler.close();
    }

    @Test
    void noActiveQueries_noErrors() throws Exception {
        // No queries registered — scheduler should run without errors
        scheduler = new SpeculativeExecutionScheduler(
                stageHandler, new CoordinatorSpeculativeQueryInventory(queryManager),
                new CoordinatorSpeculativeTaskRegistry(taskTracker), 100L);
        scheduler.start();
        Thread.sleep(300);
        // No exception means success — scheduler handles empty query set gracefully
        scheduler.close();
    }

    private QueryManager buildMinimalQueryManager() {
        var scorer = new LoadBalancedStrategy();
        var taskScheduler = new TaskScheduler(scorer, ignoredTenantForPlacement -> java.util.Optional.empty());
        var rtd = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());
        var ctx = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null, rtd, taskScheduler,
                workerManager, taskTracker, null, null, null, 0L);
        var dispatcher = new QueryDispatcher(ctx, null, null);
        return new QueryManager(QueryManagerTestContexts.minimal(dispatcher, new NoOpTracingProvider()));
    }

    private static WorkerInfo worker(String id, String host) {
        return new WorkerInfo(id, host, 8080, 8081, 0, HealthState.ACTIVE,
                MemoryPressure.NONE, 0.5, 0, 1024L * 1024 * 1024,
                Instant.now(), 1.0, null, 2, null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(id));
    }
}
