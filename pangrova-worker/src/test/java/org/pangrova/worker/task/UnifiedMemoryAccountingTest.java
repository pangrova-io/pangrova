package org.pangrova.worker.task;

import org.pangrova.worker.resource.memory.MemoryArbitrator;
import org.pangrova.worker.task.memory.TaskMemoryAllocator;
import org.pangrova.worker.task.memory.TaskMemoryRequestor;
import org.pangrova.worker.memory.NativeMemoryCoordinator;
import org.pangrova.protocol.execution.PlanFragment;
import org.pangrova.protocol.execution.PlanNodeId;
import org.pangrova.execution.api.identity.QueryId;
import org.pangrova.protocol.execution.TaskDescriptor;
import org.pangrova.protocol.execution.TaskResourceLimit;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        requestor.requestMemoryForTask(info.memoryAccount());

        // Verify single counter tracks the allocation
        long allocated = info.allocatedMemory();
        assertThat(allocated).isEqualTo(64 * 1024 * 1024L);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(allocated);

        // Allocator reads from the same source (arbitrator)
        assertThat(allocator.resolveCurrentMemoryUsage(info.memoryAccount())).isEqualTo(allocated);
        assertThat(allocator.resolveNativeMemoryLimit(info.memoryAccount())).isEqualTo(allocated);

        // Debug assertion passes: single source of truth
        allocator.assertMemoryConsistency(allocated);

        // Release memory
        requestor.releaseTaskMemory(info.memoryAccount());

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
        requestor.requestMemoryForTask(task1.memoryAccount());
        assertThat(arbitrator.getUsedMemory()).isEqualTo(32 * 1024 * 1024L);

        // Allocate task2
        requestor.requestMemoryForTask(task2.memoryAccount());
        long expectedTotal = 32 * 1024 * 1024L + 48 * 1024 * 1024L;
        assertThat(arbitrator.getUsedMemory()).isEqualTo(expectedTotal);
        allocator.assertMemoryConsistency(expectedTotal);

        // Release task1
        requestor.releaseTaskMemory(task1.memoryAccount());
        assertThat(arbitrator.getUsedMemory()).isEqualTo(48 * 1024 * 1024L);

        // Release task2
        requestor.releaseTaskMemory(task2.memoryAccount());
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
        requestor.requestMemoryForTask(info.memoryAccount());
        requestor.requestBufferMemory(info.memoryAccount());

        // Both tracked in the same arbitrator
        long taskMem = info.allocatedMemory();
        long bufferMem = info.allocatedBufferMemory();
        assertThat(taskMem).isEqualTo(100 * 1024 * 1024L);
        assertThat(bufferMem).isGreaterThan(0);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(taskMem + bufferMem);

        // Release both
        requestor.releaseTaskMemory(info.memoryAccount());
        requestor.releaseBufferMemory(info.memoryAccount());
        assertThat(arbitrator.getUsedMemory()).isZero();
        allocator.assertMemoryConsistency(0);
    }

    @Test
    void shuffleBufferRequiresExactAdmittedGrant() {
        long grant = 16L * 1024 * 1024;
        MemoryArbitrator arbitrator = new MemoryArbitrator(64L * 1024 * 1024, 0);
        TaskMemoryRequestor requestor = new TaskMemoryRequestor(arbitrator, 0.0);
        TaskInfo info = buildShuffleTaskInfo(grant);

        requestor.requestBufferMemory(info.memoryAccount());
        assertThat(info.allocatedBufferMemory()).isEqualTo(grant);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(grant);
        requestor.releaseBufferMemory(info.memoryAccount());
        assertThat(arbitrator.getUsedMemory()).isZero();
    }

    @Test
    void partialShuffleBufferAllocationIsRejectedAndRolledBack() {
        MemoryArbitrator arbitrator = new MemoryArbitrator(8L * 1024 * 1024, 0);
        TaskMemoryRequestor requestor = new TaskMemoryRequestor(arbitrator, 0.0);
        TaskInfo info = buildShuffleTaskInfo(16L * 1024 * 1024);

        assertThatThrownBy(() -> requestor.requestBufferMemory(info.memoryAccount()))
                .hasMessageContaining("complete buffer grant");
        assertThat(info.allocatedBufferMemory()).isZero();
        assertThat(arbitrator.getUsedMemory()).isZero();
    }

    @Test
    void canonicalNativeLimitIsNotChargedBeforeActualNativeGrowth() {
        MemoryArbitrator arbitrator = new MemoryArbitrator(8L * 1024 * 1024, 0);
        TaskMemoryRequestor requestor = new TaskMemoryRequestor(arbitrator, 0.0);
        TaskInfo info = buildShuffleTaskInfo(16L * 1024 * 1024);

        requestor.requestMemoryForTask(info.memoryAccount());
        assertThat(info.allocatedMemory()).isZero();
        assertThat(arbitrator.getUsedMemory()).isZero();
        assertThat(new TaskMemoryAllocator(arbitrator).resolveNativeMemoryLimit(info.memoryAccount()))
                .isEqualTo(32L * 1024 * 1024);
        NativeMemoryCoordinator nativeMemory = new NativeMemoryCoordinator(8L * 1024 * 1024,
                arbitrator);
        assertThat(nativeMemory.reserve(4L * 1024 * 1024)).isEqualTo(4L * 1024 * 1024);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(4L * 1024 * 1024);
        nativeMemory.unreserve(4L * 1024 * 1024);
        assertThat(arbitrator.getUsedMemory()).isZero();
    }

    @Test
    void insufficientNodePoolRejectsShuffleWithoutArbitratorCharge() {
        MemoryArbitrator arbitrator = new MemoryArbitrator(BUDGET, 0);
        TaskMemoryRequestor requestor = new TaskMemoryRequestor(arbitrator, 0.0);
        var root = new org.pangrova.worker.resource.memory.RootMemoryPool(
                "shuffle-root", 8L * 1024 * 1024);
        var node = root.createNodePool("shuffle-node", 8L * 1024 * 1024);
        requestor.setNodeMemoryPool(node);

        assertThatThrownBy(() -> requestor.requestBufferMemory(
                buildShuffleTaskInfo(16L * 1024 * 1024).memoryAccount()))
                .hasMessageContaining("insufficient node buffer budget");
        assertThat(node.getUsedBytes()).isZero();
        assertThat(arbitrator.getUsedMemory()).isZero();
    }

    @Test
    void missingShuffleBufferGrantIsRejected() {
        MemoryArbitrator arbitrator = new MemoryArbitrator(BUDGET, 0);
        TaskMemoryRequestor requestor = new TaskMemoryRequestor(arbitrator, 0.0);
        assertThatThrownBy(() -> requestor.requestBufferMemory(buildShuffleTaskInfo(0).memoryAccount()))
                .hasMessageContaining("positive serialized-buffer grant");
        assertThat(arbitrator.getUsedMemory()).isZero();
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
        return new TaskInfo(taskId, queryId, null, descriptor, Instant.now());
    }

    private TaskInfo buildShuffleTaskInfo(long bufferBytes) {
        String taskId = "shuffle-task";
        String queryId = "shuffle-query";
        long memoryBytes = 32L * 1024 * 1024;
        TaskDescriptor descriptor = new TaskDescriptor(
                new QueryId(queryId), new TestPlanFragment(), List.of(), Map.of(),
                0, 0, "COMPUTE", new TaskResourceLimit(memoryBytes, 1, 0, ""));
        var shuffle = org.pangrova.protocol.v2.ShuffleTaskOptions.newBuilder()
                .setSerde(org.pangrova.protocol.v2.BatchSerde.BATCH_SERDE_ARROW_BATCH)
                .setCompressionCodec("none").setBatchSize(1000).setPartitionCount(1)
                .setPersistenceMode(org.pangrova.protocol.v2.ShufflePersistenceMode
                        .SHUFFLE_PERSISTENCE_MODE_MEMORY_ONLY);
        var canonical = org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture.builder(
                        taskId, queryId, 0, 0,
                        org.pangrova.protocol.v2.TaskKind.TASK_KIND_SHUFFLE_WRITE,
                        "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .setResourceGrant(org.pangrova.protocol.v2.StageResourceGrant.newBuilder()
                        .setGrantId(taskId + ":grant").setNativeMemoryBytes(memoryBytes)
                        .setShuffleBufferBytes(bufferBytes).setCpuSlots(1))
                .setExecutionOptions(org.pangrova.protocol.v2.TaskExecutionOptions
                        .newBuilder().setShuffle(shuffle));
        return new TaskInfo(taskId, queryId,
                SubmitTaskRequest.from(canonical.build()), descriptor, Instant.now());
    }

    private static final class TestPlanFragment implements PlanFragment {
        @Override
        public PlanNodeId getId() { return new PlanNodeId("test-node"); }
        @Override
        public List<PlanNodeId> getChildFragmentIds() { return List.of(); }
    }
}
