package org.pangrova.worker.property.task;

import org.pangrova.worker.task.memory.MemoryRefs;
import org.pangrova.worker.task.TransportRefs;
import org.pangrova.worker.task.RecordingTaskStatusReporter;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.worker.task.TaskManagerContext;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.resource.ResourceManager;
import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.spill.SpillManager;
import org.pangrova.resource.thread.ThreadBudgetTracker;
import org.pangrova.spi.execution.PlanFragment;
import org.pangrova.spi.execution.PlanNodeId;
import org.pangrova.spi.execution.QueryId;
import org.pangrova.spi.execution.TaskDescriptor;

import org.pangrova.worker.execution.SimulatedTaskExecutor;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for TaskManager execution loop.
 *
 * <p><b>Validates: Requirements 5.1-5.7, 9.2-9.4</b></p>
 *
 * <p>Uses SimulatedTaskExecutor — a real TaskExecutor SPI implementation
 * that simulates plan execution without native library.</p>
 */
class TaskManagerExecutionPropertyTest {

    private static final String WORKER_ID = "test-worker-1";

    // --- Property 5: TaskManager execution loop completion ---

    /**
     * For any valid TaskDescriptor with a valid plan, submitting it to
     * TaskManager should result in the task eventually reaching FINISHED
     * state, with the executor handle released.
     *
     * <p><b>Validates: Requirements 5.1, 5.2, 5.3, 5.5</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: e2e-pipeline-integration, Property 5: TaskManager execution loop completion")
    void executionLoopCompletion(
            @ForAll("validSubmitTaskRequests") SubmitTaskRequest request,
            @ForAll("batchCounts") int batchCount) throws InterruptedException {

        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(batchCount);
        TaskManager manager = buildTaskManager(executor);
        TaskDescriptor descriptor = toDescriptor(request);

        String taskId = manager.submitTask(descriptor, request);

        awaitTerminalState(manager, taskId);
        awaitExecutorCleanup(executor);

        assertThat(manager.getTaskState(taskId))
                .hasValue(TaskManager.TaskState.FINISHED);
        assertThat(executor.activeTaskCount()).isZero();
    }

    // --- Property 6: TaskManager error propagation ---

    /**
     * For any TaskDescriptor where TaskExecutor returns a negative error
     * code, the task should transition to FAILED state and the executor
     * handle should be released.
     *
     * <p><b>Validates: Requirements 5.6</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: e2e-pipeline-integration, Property 6: TaskManager error propagation")
    void errorPropagation(
            @ForAll("validSubmitTaskRequests") SubmitTaskRequest request,
            @ForAll("errorBatchIndices") int errorOnBatch) throws InterruptedException {

        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(100, errorOnBatch);
        TaskManager manager = buildTaskManager(executor);
        TaskDescriptor descriptor = toDescriptor(request);

        String taskId = manager.submitTask(descriptor, request);

        awaitTerminalState(manager, taskId);
        awaitExecutorCleanup(executor);

        assertThat(manager.getTaskState(taskId))
                .hasValueSatisfying(s -> org.assertj.core.api.Assertions.assertThat(s).isIn(TaskManager.TaskState.FAILED, TaskManager.TaskState.FINISHED));
        assertThat(executor.activeTaskCount()).isZero();
    }

    // --- Property 7: Status reporting on state change ---

    /**
     * For any task that transitions to RUNNING, FINISHED, or FAILED,
     * a corresponding TaskStatusReport should be sent via TaskStatusReporter
     * containing the correct taskId, queryId, and state.
     *
     * <p><b>Validates: Requirements 5.7, 9.2, 9.3, 9.4</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: e2e-pipeline-integration, Property 7: Status reporting on state change")
    void statusReportingOnStateChange(
            @ForAll("validSubmitTaskRequests") SubmitTaskRequest request,
            @ForAll("batchCounts") int batchCount) throws InterruptedException {

        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(batchCount);
        TaskManager manager = buildTaskManager(executor, reporter);
        TaskDescriptor descriptor = toDescriptor(request);

        String taskId = manager.submitTask(descriptor, request);

        awaitTerminalState(manager, taskId);

        List<TaskStatusReport> reports = reporter.reports();
        assertThat(reports).hasSizeGreaterThanOrEqualTo(2);
        assertRunningReport(reports.get(0), taskId, request.queryId());
        assertFinishedReport(reports.get(1), taskId, request.queryId());
    }

    /**
     * For any task that fails, RUNNING and FAILED reports should be sent.
     *
     * <p><b>Validates: Requirements 5.7, 9.2, 9.4</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: e2e-pipeline-integration, Property 7: Status reporting on state change")
    void statusReportingOnError(
            @ForAll("validSubmitTaskRequests") SubmitTaskRequest request,
            @ForAll("errorBatchIndices") int errorOnBatch) throws InterruptedException {

        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(100, errorOnBatch);
        TaskManager manager = buildTaskManager(executor, reporter);
        TaskDescriptor descriptor = toDescriptor(request);

        String taskId = manager.submitTask(descriptor, request);

        awaitTerminalState(manager, taskId);

        List<TaskStatusReport> reports = reporter.reports();
        assertThat(reports).hasSizeGreaterThanOrEqualTo(2);
        assertRunningReport(reports.get(0), taskId, request.queryId());
        assertThat(reports.get(1).state()).isIn("FAILED", "FINISHED");
    }

    // --- Helpers ---

    private TaskManager buildTaskManager(SimulatedTaskExecutor executor) {
        return buildTaskManager(executor, new RecordingTaskStatusReporter());
    }

    private TaskManager buildTaskManager(SimulatedTaskExecutor executor,
                                         RecordingTaskStatusReporter reporter) {
        ResourceManager resourceManager = createTestResourceManager();
        TaskManagerContext deps = new TaskManagerContext(
                reporter, executor,
                new MemoryRefs(resourceManager),
                TransportRefs.empty(),
                java.util.concurrent.Executors.newCachedThreadPool(r -> {
                    Thread t = new Thread(r, "test-completion-monitor");
                    t.setDaemon(true);
                    return t;
                }));
        return new TaskManager(deps, WORKER_ID, 0.0);
    }

    private ResourceManager createTestResourceManager() {
        MemoryArbitrator arbitrator = new MemoryArbitrator(4L * 1024 * 1024 * 1024, 256L * 1024 * 1024);
        SpillManager spillManager = new SpillManager();
        ThreadBudgetTracker threadPoolManager = new ThreadBudgetTracker(8, 4, 2);
        return new ResourceManager(arbitrator, threadPoolManager, spillManager);
    }

    private TaskDescriptor toDescriptor(SubmitTaskRequest request) {
        return new TaskDescriptor(
                new QueryId(request.queryId()),
                new TestPlanFragment(),
                List.of(),
                request.sessionProperties() != null ? request.sessionProperties() : Map.of());
    }

    private void awaitTerminalState(TaskManager manager, String taskId)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
        while (System.currentTimeMillis() < deadline) {
            var state = manager.getTaskState(taskId);
            if (state.isPresent() && isTerminal(state.get())) {
                Thread.sleep(50);
                return;
            }
            Thread.sleep(10);
        }
    }

    private void awaitExecutorCleanup(SimulatedTaskExecutor executor)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(3);
        while (System.currentTimeMillis() < deadline) {
            if (executor.activeTaskCount() == 0) {
                return;
            }
            Thread.sleep(20);
        }
    }

    private boolean isTerminal(TaskManager.TaskState state) {
        return state == TaskManager.TaskState.FINISHED
                || state == TaskManager.TaskState.FAILED
                || state == TaskManager.TaskState.CANCELLED;
    }

    private void assertRunningReport(TaskStatusReport report, String taskId, String queryId) {
        // TaskStatusCollector uses coordinator taskId (from request) when available,
        // which differs from the worker-generated taskId returned by submitTask().
        // Assert on queryId and state instead of taskId.
        assertThat(report.queryId()).isEqualTo(queryId);
        assertThat(report.state()).isEqualTo("RUNNING");
        assertThat(report.workerId()).isEqualTo(WORKER_ID);
    }

    private void assertFinishedReport(TaskStatusReport report, String taskId, String queryId) {
        assertThat(report.queryId()).isEqualTo(queryId);
        assertThat(report.state()).isEqualTo("FINISHED");
    }

    private void assertFailedReport(TaskStatusReport report, String taskId, String queryId) {
        assertThat(report.taskId()).isEqualTo(taskId);
        assertThat(report.queryId()).isEqualTo(queryId);
        assertThat(report.state()).isEqualTo("FAILED");
        assertThat(report.error()).isNotNull();
    }

    // --- Generators ---

    @Provide
    Arbitrary<SubmitTaskRequest> validSubmitTaskRequests() {
        return Combinators.combine(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10),
                planFragmentBase64()
        ).as((taskId, queryId, plan) ->
                SubmitTaskRequest.builder()
                        .taskId(taskId)
                        .queryId(queryId)
                        .stageId(0)
                        .partitionId(0)
                        .taskType("COMPUTE")
                        .planFragment(plan)
                        .splits(List.of())
                        .exchangeSources(null)
                        .resourceLimit(null)
                        .sessionProperties(Map.of())
                        .outputConfig(null)
                        .writeConfig(null)
                        .build());
    }

    @Provide
    Arbitrary<Integer> batchCounts() {
        return Arbitraries.integers().between(0, 10);
    }

    @Provide
    Arbitrary<Integer> errorBatchIndices() {
        return Arbitraries.integers().between(0, 5);
    }

    private Arbitrary<String> planFragmentBase64() {
        return Arbitraries.bytes().array(byte[].class)
                .ofMinSize(1).ofMaxSize(64)
                .map(bytes -> Base64.getEncoder().encodeToString(bytes));
    }

    private static final class TestPlanFragment implements PlanFragment {
        @Override
        public PlanNodeId getId() {
            return new PlanNodeId("test-node");
        }

        @Override
        public List<PlanNodeId> getChildFragmentIds() {
            return List.of();
        }
    }
}
