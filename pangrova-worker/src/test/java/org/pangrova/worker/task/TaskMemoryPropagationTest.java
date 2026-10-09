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
import org.pangrova.testing.nativefixture.ActualNativeResultReader;
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
    @org.junit.jupiter.api.Test
    void resultDecodeBudgetFailureCannotPublishFinished() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "result-decode-budget")) {
            long available = runtime.arbitrator().getAvailableMemory();
            // The producer fits one actual capacity quantum; the result decoder
            // needs a second quantum and must fail within this admitted grant.
            var info = tasks.submit(request(1_278_854, ActualNativeTaskPlans.values(1, 1)));
            assertThatThrownBy(() -> ActualNativeResultReader.read(runtime, info.request().taskRequest()))
                    .isInstanceOf(org.apache.arrow.flight.FlightRuntimeException.class)
                    .hasMessageContaining("MEM_CAP_EXCEEDED");
            tasks.awaitState(info, TaskManager.TaskState.FAILED);
            var receipt = tasks.releasedReport(info);
            assertThat(receipt.getState()).isEqualTo(TaskState.TASK_STATE_FAILED);
            assertThat(receipt.getError().getMessage()).contains("MEM_CAP_EXCEEDED");
            assertThat(tasks.reports().reports()).noneMatch(report -> report.state().equals("FINISHED"));
            assertThat(info.executorHandle()).isZero();
            assertThat(runtime.arbitrator().getAvailableMemory()).isEqualTo(available);
        }
    }

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
            assertThat(info.allocatedMemory()).isZero();
            consumeActualResult(runtime, tasks, info);
            assertActualOutcome(tasks, info);
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
            consumeActualResult(runtime, tasks, info);
            assertActualOutcome(tasks, info);
            assertThat(tasks.reports().reports()).isNotEmpty().allSatisfy(report -> {
                if (report.stats() == null) {
                    assertThat(report.state()).isEqualTo("FAILED");
                    assertThat(info.executorReleaseReport()).isNotEmpty();
                } else {
                    assertThat(report.stats().memoryBytes()).isGreaterThanOrEqualTo(0);
                    assertThat(report.stats().peakMemoryBytes()).isGreaterThanOrEqualTo(0);
                }
            });
            if (info.getState() == TaskManager.TaskState.FINISHED) {
                assertThat(tasks.terminalReport(info).stats().peakMemoryBytes()).isPositive();
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

    private static void consumeActualResult(ActualNativeTaskFixture runtime,
                                            ActualTaskManagerFixture tasks, TaskInfo info) throws Exception {
        try {
            assertThat(ActualNativeResultReader.read(runtime, info.request().taskRequest()).values())
                    .containsExactly(0L);
            tasks.awaitState(info, TaskManager.TaskState.FINISHED);
        } catch (org.apache.arrow.flight.FlightRuntimeException failure) {
            // A small actual Native grant may fail. Only its released, classified
            // memory-failure receipt below can qualify that failed execution.
            awaitActualTerminal(tasks, info);
            assertThat(info.getState()).describedAs("Actual result stream failed: %s", failure)
                    .isEqualTo(TaskManager.TaskState.FAILED);
        }
        if (info.getState() == TaskManager.TaskState.FINISHED) {
            long handle = info.executorHandle();
            await().atMost(Duration.ofSeconds(20)).until(() ->
                    runtime.drainEvidence(handle, info.request().taskRequest()).getDrained());
            tasks.manager().releaseTaskExecutor(info.taskId());
        }
    }

    private static void assertActualOutcome(ActualTaskManagerFixture tasks, TaskInfo info) throws Exception {
        var terminal = tasks.terminalReport(info);
        var nativeReport = TaskReport.parseFrom(terminal.canonicalTaskReport());
        assertThat(nativeReport.getAttempt()).isEqualTo(info.request().taskRequest().getAttempt());
        if (info.getState() == TaskManager.TaskState.FINISHED) {
            assertThat(nativeReport.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE);
            assertThat(nativeReport.getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
            assertThat(nativeReport.hasError()).isFalse();
        } else {
            assertThat(nativeReport.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED);
            var released = TaskReport.parseFrom(info.executorReleaseReport());
            assertThat(released.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE);
            assertThat(released.getAttempt()).isEqualTo(info.request().taskRequest().getAttempt());
            assertThat(released.getState()).isEqualTo(TaskState.TASK_STATE_FAILED);
            assertThat(released.getError().getMessage()).containsIgnoringCase("memory");
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
