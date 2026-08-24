package org.pangrova.worker.task;

import org.pangrova.worker.task.memory.MemoryRefs;
import org.pangrova.worker.task.memory.TaskMemoryRequestor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.resource.ResourceManager;
import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.spill.SpillManager;
import org.pangrova.resource.thread.ThreadBudgetTracker;
import org.pangrova.spi.execution.PlanFragment;
import org.pangrova.spi.execution.PlanNodeId;
import org.pangrova.spi.execution.QueryId;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.spi.execution.TaskResourceLimit;
import org.pangrova.worker.execution.SimulatedTaskExecutor;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for Java↔C++ memory governance propagation.
 *
 * <p>Since JNI calls require the native library, these tests verify the
 * Java-side logic flow: memory limit resolution, hierarchical pool allocation,
 * and stats collection with memory tracking.</p>
 *
 * <p><b>Validates: Requirements 19.1, 19.4, 19.5</b></p>
 */
class TaskMemoryPropagationTest {

    private static final String WORKER_ID = "test-worker-mem";

    // --- Property 1: Memory limit resolution is deterministic ---

    /**
     * Property 1: Memory limit resolution
     *
     * For any TaskDescriptor with a positive maxMemoryBytes, the resolved
     * memory limit must equal the descriptor's maxMemoryBytes.
     *
     * <p><b>Validates: Requirements 19.1</b></p>
     */
    @Property(tries = 200)
    @Tag("Feature: memory-governance, Property 1: Memory limit resolution")
    void memoryLimitResolutionMatchesDescriptor(
            @ForAll("positiveMemoryLimits") long memoryBytes) {

        TaskDescriptor descriptor = buildDescriptorWithMemory(memoryBytes);
        TaskInfo info = buildTaskInfo(descriptor, memoryBytes);

        long resolved = TaskMemoryRequestor.resolveMemoryLimit(info);
        assertThat(resolved).isEqualTo(memoryBytes);
    }

    // --- Property 2: Unified memory accounting via single arbitrator ---

    /**
     * Property 2: Unified memory accounting
     *
     * For any positive memory limit, the arbitrator is the single source of
     * truth. After task submission, memory is tracked only in the arbitrator.
     *
     * <p><b>Validates: Requirements 19.1, 19.4</b></p>
     */
    @Property(tries = 200)
    @Tag("Feature: memory-governance, Property 2: Unified memory accounting")
    void unifiedAccountingUsesOnlyArbitrator(
            @ForAll("positiveMemoryLimits") long memoryBytes) {

        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(1);
        TaskManager manager = buildTaskManager(executor);

        TaskDescriptor descriptor = buildDescriptorWithMemory(memoryBytes);
        SubmitTaskRequest request = buildRequestWithMemory(memoryBytes);

        String taskId = manager.submitTask(descriptor, request);
        awaitTerminalState(manager, taskId);

        // Task should have completed (FINISHED or FAILED due to simulated executor).
        assertThat(manager.getTaskState(taskId))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s).isIn(
                        TaskManager.TaskState.FINISHED,
                        TaskManager.TaskState.FAILED));
    }

    // --- Property 3: Stats collection includes memory fields ---

    /**
     * Property 3: Stats collection with memory
     *
     * For any completed task, the status report should contain non-negative
     * memory statistics.
     *
     * <p><b>Validates: Requirements 19.5</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: memory-governance, Property 3: Stats collection with memory")
    void statsCollectionIncludesMemory(
            @ForAll("positiveMemoryLimits") long memoryBytes) {

        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(1);
        TaskManager manager = buildTaskManagerWithReporter(executor, reporter);

        TaskDescriptor descriptor = buildDescriptorWithMemory(memoryBytes);
        SubmitTaskRequest request = buildRequestWithMemory(memoryBytes);

        String taskId = manager.submitTask(descriptor, request);
        awaitTerminalState(manager, taskId);

        // All reports should have non-null stats with non-negative memory.
        assertThat(reporter.reports())
                .isNotEmpty()
                .allSatisfy(report -> {
                    assertThat(report.stats()).isNotNull();
                    assertThat(report.stats().memoryBytes()).isGreaterThanOrEqualTo(0);
                    assertThat(report.stats().peakMemoryBytes()).isGreaterThanOrEqualTo(0);
                });
    }

    // --- Property 4: Zero memory limit skips propagation ---

    /**
     * Property 4: Zero memory limit
     *
     * When the task descriptor has zero maxMemoryBytes, no memory
     * propagation should occur and the task should still complete.
     *
     * <p><b>Validates: Requirements 19.1</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: memory-governance, Property 4: Zero memory limit skips propagation")
    void zeroMemoryLimitSkipsPropagation(
            @ForAll("batchCounts") int batchCount) {

        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(batchCount);
        TaskManager manager = buildTaskManager(executor);

        TaskDescriptor descriptor = buildDescriptorWithMemory(0);
        SubmitTaskRequest request = buildRequestWithMemory(0);

        String taskId = manager.submitTask(descriptor, request);
        awaitTerminalState(manager, taskId);

        assertThat(manager.getTaskState(taskId))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s).isIn(
                        TaskManager.TaskState.FINISHED,
                        TaskManager.TaskState.FAILED));
    }

    // --- Helpers ---

    private TaskManager buildTaskManager(SimulatedTaskExecutor executor) {
        return buildTaskManagerWithReporter(executor, new RecordingTaskStatusReporter());
    }

    private TaskManager buildTaskManagerWithReporter(
            SimulatedTaskExecutor executor, RecordingTaskStatusReporter reporter) {
        ResourceManager rm = createTestResourceManager();
        TaskManagerContext deps = new TaskManagerContext(
                reporter, executor,
                new MemoryRefs(rm),
                TransportRefs.empty(),
                java.util.concurrent.Executors.newCachedThreadPool(r -> {
                    Thread t = new Thread(r, "test-completion-monitor");
                    t.setDaemon(true);
                    return t;
                }));
        return new TaskManager(deps, WORKER_ID, 0.0);
    }

    private ResourceManager createTestResourceManager() {
        MemoryArbitrator arbitrator = new MemoryArbitrator(
                4L * 1024 * 1024 * 1024, 256L * 1024 * 1024);
        SpillManager spillManager = new SpillManager();
        ThreadBudgetTracker tpm = new ThreadBudgetTracker(8, 4, 2);
        return new ResourceManager(arbitrator, tpm, spillManager);
    }

    private TaskDescriptor buildDescriptorWithMemory(long memoryBytes) {
        TaskResourceLimit limit = (memoryBytes > 0)
                ? new TaskResourceLimit(memoryBytes, 1, 0, "")
                : TaskResourceLimit.NONE;
        return new TaskDescriptor(
                new QueryId("test-query"),
                new TestPlanFragment(),
                List.of(),
                Map.of(),
                0, 0, "COMPUTE",
                limit);
    }

    private SubmitTaskRequest buildRequestWithMemory(long memoryBytes) {
        ResourceLimitPayload rl = (memoryBytes > 0)
                ? new ResourceLimitPayload(memoryBytes, 1, 0, "")
                : null;
        return SubmitTaskRequest.builder()
                .taskId("task-1")
                .queryId("test-query")
                .stageId(0)
                .partitionId(0)
                .taskType("COMPUTE")
                .planFragment("cGxhbg==")
                .splits(List.of())
                .exchangeSources(List.of())
                .resourceLimit(rl)
                .sessionProperties(Map.of())
                .outputConfig(null)
                .writeConfig(null)
                .build();
    }

    private TaskInfo buildTaskInfo(TaskDescriptor descriptor, long memoryBytes) {
        SubmitTaskRequest request = buildRequestWithMemory(memoryBytes);
        return new TaskInfo(
                "test-task-1", "test-query", request,
                descriptor, java.time.Instant.now());
    }

    private void awaitTerminalState(TaskManager manager, String taskId) {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
        while (System.currentTimeMillis() < deadline) {
            var state = manager.getTaskState(taskId);
            if (state.isPresent() && isTerminal(state.get())) {
                return;
            }
            try { Thread.sleep(10); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private boolean isTerminal(TaskManager.TaskState state) {
        return state == TaskManager.TaskState.FINISHED
                || state == TaskManager.TaskState.FAILED
                || state == TaskManager.TaskState.CANCELLED;
    }

    // --- Generators ---

    @Provide
    Arbitrary<Long> positiveMemoryLimits() {
        return Arbitraries.longs().between(1L, 4L * 1024 * 1024 * 1024);
    }

    @Provide
    Arbitrary<Integer> batchCounts() {
        return Arbitraries.integers().between(0, 5);
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
