package org.pangrova.worker.task;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.convert.TaskRequestDtoConverter;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.testing.nativefixture.ActualNativeTaskPlans;
import org.pangrova.worker.task.memory.TaskMemoryRequestor;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Canonical budget projection plus actual JNI memory statistics and owner cleanup. */
@Tag("native-required")
@org.junit.jupiter.api.Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class TaskMemoryPropagationTest {
    @Property(tries = 200)
    void memoryLimitResolutionMatchesDescriptor(@ForAll("positiveMemoryLimits") long memoryBytes) {
        var submitted = SubmitTaskRequest.from(request(memoryBytes, ActualNativeTaskPlans.values(1, 1)));
        var descriptor = TaskRequestDtoConverter.toTaskDescriptor(submitted);
        var info = new TaskInfo("memory-projection", submitted.taskRequest().getAttempt().getQueryOrJobId(),
                submitted, descriptor, Instant.now());
        assertThat(descriptor.resourceLimit().maxMemoryBytes()).isEqualTo(memoryBytes);
        assertThat(TaskMemoryRequestor.resolveMemoryLimit(info)).isEqualTo(memoryBytes);
    }

    @Property(tries = 200)
    void unifiedAccountingUsesOnlyArbitrator(@ForAll("positiveMemoryLimits") long memoryBytes) throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "memory-accounting")) {
            long baseline = runtime.arbitrator().getUsedMemory();
            long nodeBaseline = runtime.nodePool().getUsedBytes();
            var info = tasks.submit(request(memoryBytes, ActualNativeTaskPlans.values(1, 1)));
            awaitActualTerminal(tasks, info);
            assertThat(info.allocatedMemory()).isZero();
            assertActualOutcome(tasks, info);
            if (info.getState() == TaskManager.TaskState.FINISHED) {
                assertThat(runtime.arbitrator().snapshot()).containsEntry("buffer:" + info.taskId(),
                        info.request().taskRequest().getResourceGrant().getShuffleBufferBytes());
                assertThat(tasks.consume(info).values()).containsExactly(0L);
            }
            assertThat(runtime.arbitrator().getUsedMemory()).isEqualTo(baseline);
            assertThat(runtime.nodePool().getUsedBytes()).isEqualTo(nodeBaseline);
            assertThat(info.executorHandle()).isZero();
        }
    }

    @Property(tries = 100)
    void statsCollectionIncludesMemory(@ForAll("positiveMemoryLimits") long memoryBytes) throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "memory-stats")) {
            var info = tasks.submit(request(memoryBytes, ActualNativeTaskPlans.values(1, 1)));
            awaitActualTerminal(tasks, info);
            assertActualOutcome(tasks, info);
            assertThat(tasks.reports().reports()).isNotEmpty().allSatisfy(report -> {
                assertThat(report.stats()).isNotNull();
                assertThat(report.stats().memoryBytes()).isGreaterThanOrEqualTo(0);
                assertThat(report.stats().peakMemoryBytes()).isGreaterThanOrEqualTo(0);
            });
            if (info.getState() == TaskManager.TaskState.FINISHED) {
                assertThat(tasks.terminalReport(info).stats().peakMemoryBytes()).isPositive();
                tasks.consume(info);
            }
        }
    }

    @Property(tries = 100)
    void zeroMemoryLimitIsRejectedBeforeNativeRegistration(@ForAll("batchCounts") int batchCount) {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "zero-memory")) {
            long baseline = runtime.arbitrator().getUsedMemory();
            assertThatThrownBy(() -> tasks.submit(request(0, ActualNativeTaskPlans.values(batchCount, 1))))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("resource grant is incomplete");
            assertThat(tasks.manager().tasks).isEmpty();
            assertThat(tasks.reports().reports()).isEmpty();
            assertThat(runtime.arbitrator().getUsedMemory()).isEqualTo(baseline);
        }
    }

    private static void awaitActualTerminal(ActualTaskManagerFixture tasks, TaskInfo info) {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(info.getState()).isIn(TaskManager.TaskState.FINISHED, TaskManager.TaskState.FAILED);
            assertThat(tasks.reports().reports()).anyMatch(report ->
                    report.taskId().equals(info.request().taskRequest().getAttempt().getTaskId())
                            && (report.state().equals("FINISHED") || report.state().equals("FAILED")));
        });
    }

    private static void assertActualOutcome(ActualTaskManagerFixture tasks, TaskInfo info) throws Exception {
        var terminal = tasks.terminalReport(info);
        var nativeReport = TaskReport.parseFrom(terminal.canonicalTaskReport());
        assertThat(nativeReport.getAttempt()).isEqualTo(info.request().taskRequest().getAttempt());
        assertThat(nativeReport.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE);
        if (info.getState() == TaskManager.TaskState.FINISHED) {
            assertThat(nativeReport.getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
            assertThat(nativeReport.hasError()).isFalse();
        } else {
            assertThat(nativeReport.getState()).isEqualTo(TaskState.TASK_STATE_FAILED);
            assertThat(nativeReport.getError().getMessage()).containsIgnoringCase("memory");
            assertThat(info.executorHandle()).isZero();
            assertThat(info.allocatedBufferMemory()).isZero();
        }
    }

    private static TaskRequest request(long bytes, byte[] plan) {
        var request = ActualNativeTaskFixture.planTaskRequest(true, plan).toBuilder();
        request.getResourceGrantBuilder().setNativeMemoryBytes(bytes);
        return request.build();
    }

    @Provide Arbitrary<Long> positiveMemoryLimits() {
        return Arbitraries.longs().between(1L, 4L * 1024 * 1024 * 1024);
    }
    @Provide Arbitrary<Integer> batchCounts() { return Arbitraries.integers().between(0, 5); }
}
