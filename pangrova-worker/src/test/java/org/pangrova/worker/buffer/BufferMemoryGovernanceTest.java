package org.pangrova.worker.buffer;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Tag;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.testing.nativefixture.ActualNativeTaskPlans;
import org.pangrova.worker.task.ActualTaskManagerFixture;
import org.pangrova.worker.task.TaskInfo;
import org.pangrova.worker.task.TaskManager;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual buffer grants remain held until result EOF and all Native owners drain. */
@Tag("native-required")
@org.junit.jupiter.api.Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class BufferMemoryGovernanceTest {
    private static final long TASK_MEMORY = 100L * 1024 * 1024;

    @Test
    void bufferMemoryAllocatedOnTaskStart() throws Exception {
        verifyHeldBuffer(TASK_MEMORY, 2);
    }

    @Test
    void bufferMemoryReleasedOnCompletion() throws Exception {
        verifyHeldBuffer(TASK_MEMORY, 1);
    }

    @Test
    void bufferMemoryReleasedOnFailure() {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "buffer-failure")) {
            long baseline = runtime.arbitrator().getUsedMemory();
            var request = request(TASK_MEMORY, ActualNativeTaskPlans.failingCast());
            var info = tasks.submit(request);
            tasks.awaitState(info, TaskManager.TaskState.FAILED);
            assertThat(tasks.terminalReport(info).error().message()).contains("not-an-integer");
            assertThat(info.executorHandle()).isZero();
            assertThat(info.allocatedBufferMemory()).isZero();
            assertThat(runtime.arbitrator().getUsedMemory()).isEqualTo(baseline);
            assertThat(runtime.nodePool().getUsedBytes()).isZero();
        }
    }

    @Test
    void bufferGrantIsAccountedThroughBothBudgets() throws Exception {
        verifyHeldBuffer(TASK_MEMORY, 1);
    }

    @Property(tries = 5)
    void bufferAllocationBoundedByThirtyPercent(
            @ForAll @LongRange(min = 10L * 1024 * 1024, max = 1024L * 1024 * 1024) long taskMemory,
            @ForAll @IntRange(min = 1, max = 5) int batchCount) throws Exception {
        verifyHeldBuffer(taskMemory, batchCount);
    }

    @Property(tries = 5)
    void memoryReturnsToZeroAfterCompletion(
            @ForAll @IntRange(min = 1, max = 3) int taskCount,
            @ForAll @IntRange(min = 1, max = 4) int batchCount) throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "buffer-sequence")) {
            long baseline = runtime.arbitrator().getUsedMemory();
            for (int index = 0; index < taskCount; index++) {
                var info = tasks.submit(request(TASK_MEMORY, ActualNativeTaskPlans.values(batchCount, 2)));
                tasks.awaitState(info, TaskManager.TaskState.RUNNING);
                assertThat(tasks.consume(info).rows()).isEqualTo(batchCount * 2L);
                assertThat(runtime.arbitrator().getUsedMemory()).isEqualTo(baseline);
            }
        }
    }

    @Property(tries = 5)
    void distinctCanonicalTasksKeepIndependentBufferReservations(@ForAll @IntRange(min = 0, max = 99) int taskSuffix)
            throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "buffer-key-" + taskSuffix)) {
            var request = request(TASK_MEMORY, ActualNativeTaskPlans.values(1, 2)).toBuilder();
            request.getAttemptBuilder().setTaskId("task-key-" + taskSuffix);
            var info = tasks.submit(request.build());
            tasks.awaitState(info, TaskManager.TaskState.RUNNING);
            var other = tasks.submit(request(TASK_MEMORY, ActualNativeTaskPlans.values(1, 2)));
            tasks.awaitState(other, TaskManager.TaskState.RUNNING);
            assertHeldGrant(runtime, info, TASK_MEMORY);
            assertHeldGrant(runtime, other, TASK_MEMORY);
            assertThat(runtime.arbitrator().getUsedMemory()).isGreaterThanOrEqualTo(2 * bufferGrant(TASK_MEMORY));
            assertThat(tasks.consume(info).values()).containsExactly(0L, 1L);
            assertThat(info.allocatedBufferMemory()).isZero();
            assertHeldGrant(runtime, other, TASK_MEMORY);
            assertThat(tasks.consume(other).values()).containsExactly(0L, 1L);
            assertThat(runtime.arbitrator().getUsedMemory()).isZero();
            assertThat(runtime.nodePool().getUsedBytes()).isZero();
        }
    }

    private static void verifyHeldBuffer(long taskMemory, int batches) throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "buffer-grant")) {
            long baseline = runtime.arbitrator().getUsedMemory();
            var info = tasks.submit(request(taskMemory, ActualNativeTaskPlans.values(batches, 2)));
            tasks.awaitState(info, TaskManager.TaskState.RUNNING);
            assertHeldGrant(runtime, info, taskMemory);
            assertThat(tasks.consume(info).rows()).isEqualTo(batches * 2L);
            assertThat(runtime.arbitrator().getUsedMemory()).isEqualTo(baseline);
        }
    }

    private static void assertHeldGrant(ActualNativeTaskFixture runtime, TaskInfo info, long memory) {
        assertThat(info.executorHandle()).isPositive();
        assertThat(info.allocatedBufferMemory()).isEqualTo(bufferGrant(memory));
        assertThat(runtime.arbitrator().getUsedMemory()).isGreaterThanOrEqualTo(bufferGrant(memory));
        assertThat(runtime.nodePool().getUsedBytes()).isGreaterThanOrEqualTo(bufferGrant(memory));
        assertThat(runtime.arbitrator().getUsedMemory()).isLessThanOrEqualTo(ActualNativeTaskFixture.TOTAL_MEMORY_BYTES);
    }

    private static TaskRequest request(long memory, byte[] plan) {
        var request = ActualNativeTaskFixture.planTaskRequest(true, plan).toBuilder();
        request.getResourceGrantBuilder().setNativeMemoryBytes(memory).setShuffleBufferBytes(bufferGrant(memory));
        return request.build();
    }

    private static long bufferGrant(long memory) { return (long) (memory * 0.30); }
}
