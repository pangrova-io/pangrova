package org.pangrova.worker.property.buffer;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.testing.nativefixture.ActualNativeTaskPlans;
import org.pangrova.worker.task.ActualTaskManagerFixture;
import org.pangrova.worker.task.TaskInfo;
import org.pangrova.worker.task.TaskManager;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Concurrent real JNI results hold exact grants until their actual streams are consumed. */
@Tag("native-required")
@org.junit.jupiter.api.Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class BufferMemoryBoundedPropertyTest {
    private static final long BUFFER_BYTES = 8L * 1024 * 1024;

    @Property(tries = 100)
    void bufferMemoryNeverExceedsBudget(@ForAll("taskCounts") int taskCount) throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "concurrent-buffer-bound")) {
            var infos = submitConcurrentTasks(tasks, taskCount, 3);
            for (var info : infos) tasks.awaitState(info, TaskManager.TaskState.FINISHED);
            long heldGrants = runtime.arbitrator().snapshot().entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith("buffer:"))
                    .mapToLong(java.util.Map.Entry::getValue).sum();
            assertThat(heldGrants).isEqualTo(taskCount * BUFFER_BYTES);
            assertThat(runtime.arbitrator().getUsedMemory())
                    .isLessThanOrEqualTo(ActualNativeTaskFixture.TOTAL_MEMORY_BYTES);
            for (var info : infos) assertThat(tasks.consume(info).rows()).isEqualTo(6);
        }
    }

    @Property(tries = 100)
    void bufferMemoryFullyReleasedAfterCompletion(@ForAll("taskCounts") int taskCount) throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "concurrent-buffer-release")) {
            long baseline = runtime.arbitrator().getUsedMemory();
            var infos = submitConcurrentTasks(tasks, taskCount, 2);
            for (var info : infos) tasks.awaitState(info, TaskManager.TaskState.FINISHED);
            for (var info : infos) {
                assertThat(info.allocatedBufferMemory()).isEqualTo(BUFFER_BYTES);
                assertThat(tasks.consume(info).rows()).isEqualTo(4);
                assertThat(runtime.arbitrator().snapshot()).doesNotContainKey("buffer:" + info.taskId());
            }
            assertThat(runtime.arbitrator().getUsedMemory()).isEqualTo(baseline);
            assertThat(tasks.manager().getRunningTaskCount()).isZero();
        }
    }

    private static List<TaskInfo> submitConcurrentTasks(ActualTaskManagerFixture tasks, int count, int batches) {
        var infos = new ArrayList<TaskInfo>();
        for (int index = 0; index < count; index++) {
            infos.add(tasks.submit(ActualNativeTaskFixture.planTaskRequest(true,
                    ActualNativeTaskPlans.values(batches, 2))));
        }
        return List.copyOf(infos);
    }

    @Provide Arbitrary<Integer> taskCounts() { return Arbitraries.integers().between(1, 10); }
}
