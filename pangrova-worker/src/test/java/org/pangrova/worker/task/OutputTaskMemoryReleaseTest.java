package org.pangrova.worker.task;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.resource.ResourceManager;
import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.spill.SpillManager;
import org.pangrova.resource.thread.ThreadBudgetTracker;
import org.pangrova.spi.execution.PlanFragment;
import org.pangrova.spi.execution.PlanNodeId;
import org.pangrova.spi.execution.QueryId;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.spi.execution.TaskResourceLimit;
import org.pangrova.worker.bootstrap.phase.ActualNativeTaskFixture;
import org.pangrova.worker.task.memory.MemoryRefs;
import org.pangrova.worker.task.memory.TaskMemoryRequestor;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Actual JNI result retention and task-budget release, not a SQL/distributed E2E. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class OutputTaskMemoryReleaseTest {
    private ActualNativeTaskFixture runtime;
    private TaskManager manager;
    private final RecordingTaskStatusReporter reports = new RecordingTaskStatusReporter();

    @AfterEach
    void closeActualOwners() {
        if (manager != null) {
            manager.releaseTerminalTaskExecutors();
            if (runtime != null) runtime.stopTransports();
            manager.close();
        }
        if (runtime != null) runtime.close();
    }

    @Test
    void outputTaskReleasesMemoryOnlyAfterActualResultEof() throws Exception {
        runtime = new ActualNativeTaskFixture();
        long availableBeforeTask = runtime.arbitrator().getAvailableMemory();
        TaskRequest request = runtime.valuesRequest(true);
        TaskInfo info = submit(request, "OUTPUT", reports);
        awaitFinishedReport();
        long handle = info.executorHandle();
        assertThat(handle).isPositive();
        assertThat(info.allocatedBufferMemory()).isEqualTo(request.getResourceGrant().getShuffleBufferBytes());
        assertThat(runtime.arbitrator().getAvailableMemory())
                .isLessThanOrEqualTo(availableBeforeTask - info.allocatedBufferMemory());
        assertThat(runtime.drainEvidence(handle, request).getDrained()).isFalse();

        consumeActualResult(request);
        await().atMost(Duration.ofSeconds(20)).until(() -> runtime.drainEvidence(handle, request).getDrained());
        assertThat(manager.cleanupTerminalTasks(Duration.ofHours(1))).isEqualTo(1);
        assertThat(info.executorHandle()).isZero();
        assertThat(info.allocatedBufferMemory()).isZero();
        assertThat(info.allocatedMemory()).isZero();
        assertThat(runtime.arbitrator().getAvailableMemory()).isEqualTo(availableBeforeTask);
        TaskReport receipt = TaskReport.parseFrom(info.executorReleaseReport());
        assertThat(receipt.getAttempt()).isEqualTo(request.getAttempt());
        assertThat(receipt.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE);
        assertThat(receipt.getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
        assertThat(receipt.hasNativeTaskStats()).isTrue();
        assertThat(receipt.getNativeTaskStats().toByteArray()).isEqualTo(info.terminalTaskStats());
    }

    @Test
    void memoryReleaseIsIdempotent() {
        var arbitrator = new MemoryArbitrator(ActualNativeTaskFixture.TOTAL_MEMORY_BYTES, 0);
        var requestor = new TaskMemoryRequestor(arbitrator, 0.0);
        var request = ActualNativeTaskFixture.valuesTaskRequest(true);
        var info = new TaskInfo("idempotent-release", request.getAttempt().getQueryOrJobId(),
                SubmitTaskRequest.from(request), descriptor(request, "OUTPUT"), Instant.now());
        requestor.requestMemoryForTask(info);
        requestor.requestBufferMemory(info);
        assertThat(arbitrator.getAvailableMemory()).isLessThan(ActualNativeTaskFixture.TOTAL_MEMORY_BYTES);
        requestor.releaseTaskMemory(info);
        requestor.releaseBufferMemory(info);
        long afterFirst = arbitrator.getAvailableMemory();
        requestor.releaseTaskMemory(info);
        requestor.releaseBufferMemory(info);
        assertThat(arbitrator.getAvailableMemory()).isEqualTo(afterFirst);
        assertThat(afterFirst).isEqualTo(ActualNativeTaskFixture.TOTAL_MEMORY_BYTES);
    }

    @Test
    void outputTaskReportsOneCanonicalFinishOnlyAfterNativeCompletion() throws Exception {
        runtime = new ActualNativeTaskFixture();
        TaskRequest request = runtime.valuesRequest(true);
        TaskInfo info = submit(request, "OUTPUT", reports);
        awaitFinishedReport();
        var finished = reports.reports().stream().filter(report -> "FINISHED".equals(report.state())).toList();
        assertThat(finished).hasSize(1);
        TaskReport report = TaskReport.parseFrom(finished.get(0).canonicalTaskReport());
        assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
        assertThat(report.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE);
        assertThat(report.getAttempt()).isEqualTo(request.getAttempt());
        assertThat(report).isEqualTo(TaskReport.parseFrom(runtime.executor().getTaskStatus(info.executorHandle())));
        assertThat(info.terminalTaskStats()).isNotEmpty();
        assertThat(reports.reports()).anyMatch(value -> "RUNNING".equals(value.state())
                && value.reportSequence() < finished.get(0).reportSequence());
        assertThat(info.executorHandle()).isPositive();
        assertThat(info.allocatedBufferMemory()).isPositive();
        manager.releaseTaskExecutor(info.taskId());
        assertThat(info.executorHandle()).isZero();
        assertThat(info.allocatedBufferMemory()).isZero();
    }

    @Test
    void computeTaskReleasesMemoryBeforeFinishedReport() throws Exception {
        runtime = new ActualNativeTaskFixture();
        long availableBeforeTask = runtime.arbitrator().getAvailableMemory();
        var memoryAtFinished = new AtomicLong(-1);
        TaskStatusReporter observation = report -> {
            if ("FINISHED".equals(report.state())) {
                memoryAtFinished.set(runtime.arbitrator().getAvailableMemory());
            }
            reports.report(report);
        };
        TaskInfo info = submit(runtime.valuesRequest(false), "COMPUTE", observation);
        awaitFinishedReport();
        assertThat(info.executorHandle()).isZero();
        assertThat(info.allocatedMemory()).isZero();
        assertThat(info.allocatedBufferMemory()).isZero();
        assertThat(memoryAtFinished.get()).isGreaterThanOrEqualTo(availableBeforeTask);
        assertThat(reports.reports().stream().filter(report -> "FINISHED".equals(report.state()))).hasSize(1);
    }

    @Test
    void explicitResultRetirementReleasesTheUnclaimedQueueOnce() throws Exception {
        runtime = new ActualNativeTaskFixture();
        long availableBeforeTask = runtime.arbitrator().getAvailableMemory();
        TaskInfo info = submit(runtime.valuesRequest(true), "OUTPUT", reports);
        awaitFinishedReport();
        assertThat(info.executorHandle()).isPositive();
        assertThat(info.allocatedBufferMemory()).isPositive();
        manager.releaseTaskExecutor(info.taskId());
        long afterFirst = runtime.arbitrator().getAvailableMemory();
        byte[] firstReceipt = info.executorReleaseReport();
        manager.releaseTaskExecutor(info.taskId());
        assertThat(info.executorReleaseReport()).isEqualTo(firstReceipt);
        assertThat(info.executorHandle()).isZero();
        assertThat(info.allocatedBufferMemory()).isZero();
        assertThat(runtime.arbitrator().getAvailableMemory()).isEqualTo(afterFirst);
        assertThat(afterFirst).isEqualTo(availableBeforeTask);
    }

    @Test
    void missingResultTransportRejectsBeforeCreatingAnActualNativeHandle() throws Exception {
        runtime = new ActualNativeTaskFixture();
        long availableBeforeTask = runtime.arbitrator().getAvailableMemory();
        var resources = new ResourceManager(runtime.arbitrator(), new ThreadBudgetTracker(8, 4, 2), new SpillManager());
        var monitor = Executors.newSingleThreadExecutor(task -> new Thread(task, "unavailable-result-completion"));
        manager = new TaskManager(new TaskManagerContext(reports, runtime.executor(),
                new MemoryRefs(resources, runtime.nodePool(), null), new TransportRefs(null, null, null), monitor),
                "unavailable-result-worker", 0.0);
        var request = runtime.valuesRequest(true);
        String id = manager.submitTask(descriptor(request, "OUTPUT"), SubmitTaskRequest.from(request));
        var info = manager.tasks.get(id);
        assertThat(info.getState()).isEqualTo(TaskManager.TaskState.FAILED);
        assertThat(info.executorHandle()).isZero();
        assertThat(info.allocatedBufferMemory()).isZero();
        assertThat(runtime.arbitrator().getAvailableMemory()).isEqualTo(availableBeforeTask);
        assertThat(reports.reports()).noneMatch(report -> "FINISHED".equals(report.state()));
        var failed = reports.reports().stream().filter(report -> "FAILED".equals(report.state())).findFirst().orElseThrow();
        assertThat(failed.error().message()).contains("result transport is unavailable");
        assertThat(TaskReport.parseFrom(failed.canonicalTaskReport()).getReportOrigin())
                .isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED);
    }

    @Test
    void shutdownQuiescesActualTransportsAndReleasesTaskGrantsBeforeReturning() throws Exception {
        runtime = new ActualNativeTaskFixture();
        long availableBeforeTask = runtime.arbitrator().getAvailableMemory();
        var request = runtime.valuesRequest(true);
        var info = submit(request, "OUTPUT", reports);
        awaitFinishedReport();
        assertThat(info.executorHandle()).isPositive();
        manager.close();
        assertThat(info.executorHandle()).isZero();
        assertThat(info.allocatedBufferMemory()).isZero();
        assertThat(runtime.transports().resultTransport().boundPort()).isEqualTo(-1);
        assertThat(runtime.arbitrator().getAvailableMemory()).isEqualTo(availableBeforeTask);
        assertThatThrownBy(() -> manager.submitTask(descriptor(request, "OUTPUT"), SubmitTaskRequest.from(request)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("draining");
        manager.close();
        assertThat(runtime.arbitrator().getAvailableMemory()).isEqualTo(availableBeforeTask);
    }

    private TaskInfo submit(TaskRequest request, String taskType, TaskStatusReporter reporter) {
        var resources = new ResourceManager(runtime.arbitrator(), new ThreadBudgetTracker(8, 4, 2), new SpillManager());
        var monitor = Executors.newSingleThreadExecutor(task -> new Thread(task, "actual-native-completion"));
        manager = new TaskManager(new TaskManagerContext(reporter, runtime.executor(),
                new MemoryRefs(resources, runtime.nodePool(), null), runtime.transports(), monitor),
                "actual-native-budget-worker", 0.0);
        String id = manager.submitTask(descriptor(request, taskType), SubmitTaskRequest.from(request));
        return manager.tasks.get(id);
    }

    private void awaitFinishedReport() {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(reports.reports()).anyMatch(report -> "FINISHED".equals(report.state())));
    }

    private void consumeActualResult(TaskRequest request) throws Exception {
        try (var allocator = new RootAllocator(ActualNativeTaskFixture.TASK_MEMORY_BYTES);
             var client = FlightClient.builder(allocator, Location.forGrpcInsecure("127.0.0.1", runtime.resultPort())).build();
             var stream = client.getStream(new Ticket(runtime.resultTicket(request).getBytes(StandardCharsets.UTF_8)))) {
            assertThat(stream.next()).isTrue();
            assertThat(stream.getRoot().getRowCount()).isEqualTo(1);
            assertThat(stream.getRoot().getFieldVectors()).hasSize(1);
            assertThat(stream.getRoot().getVector(0).getObject(0)).isEqualTo(42L);
            assertThat(stream.next()).isFalse();
        }
    }

    private static TaskDescriptor descriptor(TaskRequest request, String taskType) {
        PlanFragment fragment = new PlanFragment() {
            @Override public PlanNodeId getId() { return new PlanNodeId("actual-values"); }
            @Override public List<PlanNodeId> getChildFragmentIds() { return List.of(); }
        };
        return new TaskDescriptor(new QueryId(request.getAttempt().getQueryOrJobId()), fragment,
                List.of(), Map.of(), 0, 0, taskType,
                new TaskResourceLimit(request.getResourceGrant().getNativeMemoryBytes(), 1, 0, ""));
    }
}
