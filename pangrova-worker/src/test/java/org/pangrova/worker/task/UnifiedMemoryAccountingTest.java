package org.pangrova.worker.task;

import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.worker.task.memory.TaskMemoryAllocator;
import org.pangrova.worker.task.memory.TaskMemoryRequestor;
import org.pangrova.spi.execution.PlanFragment;
import org.pangrova.spi.execution.PlanNodeId;
import org.pangrova.spi.execution.QueryId;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.spi.execution.TaskResourceLimit;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies unified memory accounting: the MemoryArbitrator is the single
 * source of truth for memory usage per task. No dual accounting exists.
 */
class UnifiedMemoryAccountingTest {

    private static final long BUDGET = 1L * 1024 * 1024 * 1024; // 1GB

    @Test
    void allocateAndFree_singleCounterTracksCorrectly() {
        MemoryArbitrator arbitrator = new MemoryArbitrator(BUDGET, 0);
        TaskMemoryRequestor requestor = new TaskMemoryRequestor(arbitrator, 0.0);
        TaskMemoryAllocator allocator = new TaskMemoryAllocator(arbitrator);

        TaskInfo info = buildTaskInfo(64 * 1024 * 1024L); // 64MB

        // Before allocation: zero used
        assertThat(arbitrator.getUsedMemory()).isZero();

        // Allocate memory for task via requestor (the single path)
        requestor.requestMemoryForTask(info);

        // Verify single counter tracks the allocation
        long allocated = info.allocatedMemory();
        assertThat(allocated).isEqualTo(64 * 1024 * 1024L);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(allocated);

        // Allocator reads from the same source (arbitrator)
        assertThat(allocator.resolveCurrentMemoryUsage(info)).isEqualTo(allocated);
        assertThat(allocator.resolveNativeMemoryLimit(info)).isEqualTo(allocated);

        // Debug assertion passes: single source of truth
        allocator.assertMemoryConsistency(allocated);

        // Release memory
        requestor.releaseTaskMemory(info);

        // After release: zero used
        assertThat(arbitrator.getUsedMemory()).isZero();
        allocator.assertMemoryConsistency(0);
    }

    @Test
    void multipleAllocationsAndReleases_counterRemainsConsistent() {
        MemoryArbitrator arbitrator = new MemoryArbitrator(BUDGET, 0);
        TaskMemoryRequestor requestor = new TaskMemoryRequestor(arbitrator, 0.0);
        TaskMemoryAllocator allocator = new TaskMemoryAllocator(arbitrator);

        TaskInfo task1 = buildTaskInfo("q1", "t1", 32 * 1024 * 1024L);
        TaskInfo task2 = buildTaskInfo("q2", "t2", 48 * 1024 * 1024L);

        // Allocate task1
        requestor.requestMemoryForTask(task1);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(32 * 1024 * 1024L);

        // Allocate task2
        requestor.requestMemoryForTask(task2);
        long expectedTotal = 32 * 1024 * 1024L + 48 * 1024 * 1024L;
        assertThat(arbitrator.getUsedMemory()).isEqualTo(expectedTotal);
        allocator.assertMemoryConsistency(expectedTotal);

        // Release task1
        requestor.releaseTaskMemory(task1);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(48 * 1024 * 1024L);

        // Release task2
        requestor.releaseTaskMemory(task2);
        assertThat(arbitrator.getUsedMemory()).isZero();
        allocator.assertMemoryConsistency(0);
    }

    @Test
    void bufferMemory_trackedInSameArbitrator() {
        MemoryArbitrator arbitrator = new MemoryArbitrator(BUDGET, 0);
        TaskMemoryRequestor requestor = new TaskMemoryRequestor(arbitrator, 0.0);
        TaskMemoryAllocator allocator = new TaskMemoryAllocator(arbitrator);

        TaskInfo info = buildTaskInfo(100 * 1024 * 1024L); // 100MB

        // Allocate task memory + buffer memory
        requestor.requestMemoryForTask(info);
        requestor.requestBufferMemory(info);

        // Both tracked in the same arbitrator
        long taskMem = info.allocatedMemory();
        long bufferMem = info.allocatedBufferMemory();
        assertThat(taskMem).isEqualTo(100 * 1024 * 1024L);
        assertThat(bufferMem).isGreaterThan(0);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(taskMem + bufferMem);

        // Release both
        requestor.releaseTaskMemory(info);
        requestor.releaseBufferMemory(info);
        assertThat(arbitrator.getUsedMemory()).isZero();
        allocator.assertMemoryConsistency(0);
    }

    @Test
    void allocatorAndRequestor_shareExactlySameArbitrator() {
        MemoryArbitrator arbitrator = new MemoryArbitrator(BUDGET, 0);
        TaskMemoryAllocator allocator = new TaskMemoryAllocator(arbitrator);

        // Verify the allocator exposes the same arbitrator instance
        assertThat(allocator.getArbitrator()).isSameAs(arbitrator);
    }

    // --- Helpers ---

    private TaskInfo buildTaskInfo(long memoryBytes) {
        return buildTaskInfo("test-query", "test-task", memoryBytes);
    }

    private TaskInfo buildTaskInfo(String queryId, String taskId, long memoryBytes) {
        TaskResourceLimit limit = new TaskResourceLimit(memoryBytes, 1, 0, "");
        TaskDescriptor descriptor = new TaskDescriptor(
                new QueryId(queryId),
                new TestPlanFragment(),
                List.of(), Map.of(),
                0, 0, "COMPUTE", limit);
        SubmitTaskRequest request = SubmitTaskRequest.builder()
                .taskId(taskId)
                .queryId(queryId)
                .stageId(0)
                .partitionId(0)
                .taskType("COMPUTE")
                .planFragment("cGxhbg==")
                .splits(List.of())
                .exchangeSources(List.of())
                .resourceLimit(new ResourceLimitPayload(memoryBytes, 1, 0, ""))
                .sessionProperties(Map.of())
                .outputConfig(null)
                .writeConfig(null)
                .build();
        return new TaskInfo(taskId, queryId, request, descriptor, Instant.now());
    }

    private static final class TestPlanFragment implements PlanFragment {
        @Override
        public PlanNodeId getId() { return new PlanNodeId("test-node"); }
        @Override
        public List<PlanNodeId> getChildFragmentIds() { return List.of(); }
    }
}
