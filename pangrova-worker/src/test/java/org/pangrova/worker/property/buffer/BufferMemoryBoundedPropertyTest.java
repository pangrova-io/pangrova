package org.pangrova.worker.property.buffer;

import java.util.Base64;
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
import org.pangrova.worker.task.TransportRefs;
import org.pangrova.worker.task.memory.MemoryRefs;
import org.pangrova.worker.task.RecordingTaskStatusReporter;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.worker.task.TaskManagerContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for buffer memory governance bounds.
 *
 * <p>Verifies that total buffer memory never exceeds the arbitrator's
 * allocatable budget and is fully released after all tasks complete.</p>
 *
 * <p><b>Validates: Requirements 51.1, 51.2, 51.3, 51.4, 51.5</b></p>
 */
class BufferMemoryBoundedPropertyTest {

    private static final String WORKER_ID = "test-worker-buf-prop";
    private static final long TOTAL_BUDGET = 512L * 1024 * 1024;
    private static final long SYSTEM_RESERVED = 32L * 1024 * 1024;
    private static final long ALLOCATABLE = TOTAL_BUDGET - SYSTEM_RESERVED;
    private static final long TASK_MEMORY = 50L * 1024 * 1024;
    private static final String PLAN_BASE64 = Base64.getEncoder().encodeToString(new byte[]{1, 2, 3});

    // --- Property 25a: Buffer memory never exceeds arbitrator budget ---

    /**
     * For any number of concurrent tasks (1-10), each requesting buffer memory,
     * the total buffer memory allocated never exceeds the arbitrator's allocatable budget.
     *
     * <p><b>Validates: Requirements 51.1, 51.2, 51.5</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: e2e-pipeline-integration, Property 25: Buffer memory bounded")
    void bufferMemoryNeverExceedsBudget(@ForAll("taskCounts") int taskCount)
            throws InterruptedException {

        MemoryArbitrator arbitrator = new MemoryArbitrator(TOTAL_BUDGET, SYSTEM_RESERVED);
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(3);
        TaskManager manager = buildManager(executor, arbitrator);

        submitConcurrentTasks(manager, taskCount);
        long peakUsed = pollPeakMemory(arbitrator, manager);

        assertThat(peakUsed).isLessThanOrEqualTo(ALLOCATABLE);
    }

    // --- Property 25b: Buffer memory fully released after all tasks complete ---

    /**
     * For any number of tasks (1-10), after all tasks complete,
     * the arbitrator's usedMemory returns to 0.
     *
     * <p><b>Validates: Requirements 51.3, 51.4</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: e2e-pipeline-integration, Property 25: Buffer memory bounded")
    void bufferMemoryFullyReleasedAfterCompletion(@ForAll("taskCounts") int taskCount)
            throws InterruptedException {

        MemoryArbitrator arbitrator = new MemoryArbitrator(TOTAL_BUDGET, SYSTEM_RESERVED);
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(2);
        TaskManager manager = buildManager(executor, arbitrator);

        submitConcurrentTasks(manager, taskCount);
        awaitAllTasksComplete(manager);

        assertThat(arbitrator.getUsedMemory()).isZero();
    }

    // --- Generators ---

    @Provide
    Arbitrary<Integer> taskCounts() {
        return Arbitraries.integers().between(1, 10);
    }

    // --- Helpers ---

    private TaskManager buildManager(SimulatedTaskExecutor executor,
                                     MemoryArbitrator arbitrator) {
        ResourceManager rm = new ResourceManager(
                arbitrator, new ThreadBudgetTracker(8, 4, 2), new SpillManager());
        TaskManagerContext deps = new TaskManagerContext(
                new RecordingTaskStatusReporter(), executor,
                new MemoryRefs(rm),
                TransportRefs.empty(),
                java.util.concurrent.Executors.newCachedThreadPool(r -> {
                    Thread t = new Thread(r, "test-completion-monitor");
                    t.setDaemon(true);
                    return t;
                }));
        return new TaskManager(deps, WORKER_ID, 0.0);
    }

    private void submitConcurrentTasks(TaskManager manager, int count) {
        for (int i = 0; i < count; i++) {
            SubmitTaskRequest request = buildRequest(i);
            TaskDescriptor descriptor = buildDescriptor(i);
            manager.submitTask(descriptor, request);
        }
    }

    private SubmitTaskRequest buildRequest(int index) {
        return SubmitTaskRequest.builder()
                .taskId("task-" + index)
                .queryId("query-" + index)
                .stageId(0)
                .partitionId(0)
                .taskType("OUTPUT")
                .planFragment(PLAN_BASE64)
                .splits(List.of())
                .exchangeSources(List.of())
                .resourceLimit(new ResourceLimitPayload(TASK_MEMORY, 4, 0, ""))
                .sessionProperties(Map.of())
                .outputConfig(null)
                .writeConfig(null)
                .build();
    }

    private TaskDescriptor buildDescriptor(int index) {
        return new TaskDescriptor(
                new QueryId("query-" + index),
                new StubPlanFragment(),
                List.of(), Map.of(),
                0, 0, "OUTPUT",
                new TaskResourceLimit(TASK_MEMORY, 4, 0, ""));
    }

    private long pollPeakMemory(MemoryArbitrator arbitrator, TaskManager manager)
            throws InterruptedException {
        long peak = 0;
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
        while (System.currentTimeMillis() < deadline) {
            peak = Math.max(peak, arbitrator.getUsedMemory());
            if (manager.getRunningTaskCount() == 0) {
                break;
            }
            Thread.sleep(5);
        }
        return peak;
    }

    private void awaitAllTasksComplete(TaskManager manager) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
        while (System.currentTimeMillis() < deadline) {
            if (manager.getRunningTaskCount() == 0) {
                Thread.sleep(50);
                return;
            }
            Thread.sleep(10);
        }
    }

    /** Minimal PlanFragment for test descriptor construction. */
    private static final class StubPlanFragment implements PlanFragment {
        @Override
        public PlanNodeId getId() {
            return new PlanNodeId("root");
        }

        @Override
        public List<PlanNodeId> getChildFragmentIds() {
            return List.of();
        }
    }
}
