package org.pangrova.worker.task;

import com.sun.net.httpserver.HttpServer;
import org.pangrova.worker.task.report.RemoteTaskStatusReporter;
import org.pangrova.worker.task.report.TaskStatusReporter;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.protocol.http.TaskStatusAck;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.rest.task.TaskStatusWire;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.worker.resource.ResourceManager;
import org.pangrova.worker.resource.memory.MemoryArbitrator;
import org.pangrova.worker.resource.spill.SpillManager;
import org.pangrova.worker.resource.thread.ThreadBudgetTracker;
import org.pangrova.protocol.execution.PlanFragment;
import org.pangrova.protocol.execution.PlanNodeId;
import org.pangrova.execution.api.identity.QueryId;
import org.pangrova.protocol.execution.TaskDescriptor;
import org.pangrova.protocol.execution.TaskResourceLimit;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.testing.testkit.constants.TestObservability;
import org.pangrova.worker.task.memory.MemoryRefs;
import org.pangrova.worker.task.memory.TaskMemoryRequestor;

import java.nio.charset.StandardCharsets;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
    private java.util.concurrent.ScheduledThreadPoolExecutor resultCleanup;
    private final RecordingTaskStatusReporter reports = new RecordingTaskStatusReporter();

    @AfterEach
    void closeActualOwners() {
        Throwable failure = null;
        if (resultCleanup != null) {
            failure = closeOwner(() -> org.pangrova.common.concurrent.ExecutorDrain.shutdownAndAwait(
                    resultCleanup, 3_000), failure);
        }
        if (manager != null) {
            failure = closeOwner(manager::releaseTerminalTaskExecutors, failure);
            // This fixture may own real transports intentionally absent from TaskManager's dependencies.
            if (runtime != null) failure = closeOwner(runtime::stopTransports, failure);
            failure = closeOwner(manager::close, failure);
        }
        if (runtime != null) failure = closeOwner(runtime::close, failure);
        if (failure instanceof Error error) throw error;
        if (failure instanceof RuntimeException exception) throw exception;
    }

    @Test
    void outputTaskReleasesMemoryOnlyAfterActualResultEof() throws Exception {
        runtime = new ActualNativeTaskFixture();
        long availableBeforeTask = runtime.arbitrator().getAvailableMemory();
        TaskRequest request = runtime.valuesRequest(true);
        TaskInfo info = submit(request, "OUTPUT", reports);
        awaitUnconsumedResult(info, request);
        long handle = info.executorHandle();
        assertThat(handle).isPositive();
        assertThat(info.allocatedBufferMemory()).isEqualTo(request.getResourceGrant().getShuffleBufferBytes());
        assertThat(runtime.arbitrator().getAvailableMemory())
                .isLessThanOrEqualTo(availableBeforeTask - info.allocatedBufferMemory());
        assertThat(runtime.drainEvidence(handle, request).getDrained()).isFalse();

        startResultCleanup();
        consumeActualResult(request);
        awaitFinishedReport();
        await().atMost(Duration.ofSeconds(20)).until(() -> manager.tasks.get(info.taskId()) == null);
        assertThat(info.executorHandle()).isZero();
        assertThat(info.allocatedBufferMemory()).isZero();
        assertThat(info.allocatedMemory()).isZero();
        assertThat(runtime.arbitrator().getAvailableMemory()).isEqualTo(availableBeforeTask);
        TaskReport receipt = TaskReport.parseFrom(info.executorReleaseReport());
        assertThat(receipt.getAttempt()).isEqualTo(request.getAttempt());
        assertThat(receipt.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE);
        assertThat(receipt.getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
        assertThat(receipt.hasNativeTaskStats()).isTrue();
        assertThat(receipt.getNativeTaskStats()).isEqualTo(info.terminalTaskStats());
        var released = reports.reports().stream().filter(value -> "FINISHED".equals(value.state())).toList();
        assertThat(released).hasSize(2);
        assertThat(released.get(1).reportSequence()).isGreaterThan(released.get(0).reportSequence());
        assertThat(TaskReport.parseFrom(released.get(1).canonicalTaskReport())).isEqualTo(receipt);
        assertThat(TaskReport.parseFrom(released.get(0).canonicalTaskReport()).getExecutionDrainState())
                .isEqualTo(org.pangrova.protocol.v2.ExecutionDrainState.EXECUTION_DRAIN_STATE_UNSPECIFIED);
    }

    @Test
    void actualReleasePublicationFailureRetainsOneEnvelopeUntilNextCleanupSweep() throws Exception {
        runtime = new ActualNativeTaskFixture();
        long available = runtime.arbitrator().getAvailableMemory();
        var request = runtime.valuesRequest(true);
        var rejectFirstObservation = new java.util.concurrent.atomic.AtomicBoolean(true);
        TaskStatusReporter observer = report -> {
            reports.report(report);
            if (isNativeReleased(report) && rejectFirstObservation.compareAndSet(true, false)) {
                throw new IllegalStateException("Actual released-report observation interrupted");
            }
        };
        TaskInfo info = submit(request, "OUTPUT", observer);
        awaitUnconsumedResult(info, request);
        startResultCleanup();
        consumeActualResult(request);
        awaitFinishedReport();
        await().atMost(Duration.ofSeconds(20)).untilFalse(rejectFirstObservation);
        resultCleanup.submit(() -> { }).get(3, java.util.concurrent.TimeUnit.SECONDS);
        assertThat(manager.tasks.get(info.taskId())).isSameAs(info);
        assertThat(info.resourcesReleased()).isTrue();
        assertThat(runtime.arbitrator().getAvailableMemory()).isEqualTo(available);
        assertThat(manager.cleanupTerminalTasks(Duration.ofHours(1))).isEqualTo(1);
        assertThat(manager.tasks.get(info.taskId())).isNull();
        var attempts = reports.reports().stream().filter(OutputTaskMemoryReleaseTest::isNativeReleased).toList();
        assertThat(attempts).hasSize(2);
        assertThat(attempts.get(1)).isSameAs(attempts.get(0));
        assertThat(attempts.get(1).canonicalTaskReport()).isEqualTo(info.executorReleaseReport());
        assertThat(runtime.arbitrator().getAvailableMemory()).isEqualTo(available);
    }

    @Test
    void periodicRetryKeepsTheExactReleasedEnvelopeUntilItsAcknowledgement() throws Exception {
        runtime = new ActualNativeTaskFixture();
        long available = runtime.arbitrator().getAvailableMemory();
        var request = runtime.valuesRequest(true);
        var failures = new java.util.concurrent.atomic.AtomicBoolean(true);
        var retryAcknowledgement = new CompletableFuture<Void>();
        var retryStarted = new CountDownLatch(1);
        TaskInfo info = submit(request, "OUTPUT", new TaskStatusReporter() {
            @Override
            public void report(TaskStatusReport report) {
                reports.report(report);
            }

            @Override
            public CompletionStage<Void> reportTerminal(TaskStatusReport report) {
                report(report);
                if (!isNativeReleased(report)) return CompletableFuture.completedFuture(null);
                if (failures.getAndSet(false)) {
                    return CompletableFuture.failedFuture(new IllegalStateException("Released ACK lost"));
                }
                retryStarted.countDown();
                return retryAcknowledgement;
            }
        });
        try {
            awaitUnconsumedResult(info, request);
            startResultCleanup(Duration.ofSeconds(1));
            consumeActualResult(request);
            assertThat(retryStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(manager.tasks.get(info.taskId())).isSameAs(info);
            assertThat(info.resourcesReleased()).isTrue();
            assertThat(runtime.arbitrator().getAvailableMemory()).isEqualTo(available);
            var attempts = reports.reports().stream().filter(OutputTaskMemoryReleaseTest::isNativeReleased).toList();
            assertThat(attempts).hasSize(2);
            assertThat(attempts.get(1)).isSameAs(attempts.get(0));
        } finally {
            retryAcknowledgement.complete(null);
        }
        await().atMost(Duration.ofSeconds(5)).until(() -> manager.tasks.get(info.taskId()) == null);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void blockedHttpAcknowledgementDoesNotDelayEitherResultsPhysicalRelease(boolean holdReleasedReport)
            throws Exception {
        runtime = new ActualNativeTaskFixture();
        long available = runtime.arbitrator().getAvailableMemory();
        var request = runtime.valuesRequest(true);
        var secondRequest = runtime.valuesRequest(true);
        var held = new CountDownLatch(1);
        var acknowledge = new CountDownLatch(1);
        var endpoint = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var httpHandlers = Executors.newFixedThreadPool(2);
        endpoint.setExecutor(httpHandlers);
        endpoint.createContext("/v1/task/status-report", exchange -> {
            try (exchange) {
                var report = TaskStatusWire.decode(exchange.getRequestBody().readAllBytes());
                reports.report(report);
                if (request.getAttempt().getTaskId().equals(report.taskId())
                        && "FINISHED".equals(report.state()) && isNativeReleased(report) == holdReleasedReport) {
                    held.countDown();
                    if (!acknowledge.await(20, TimeUnit.SECONDS)) {
                        exchange.sendResponseHeaders(504, -1);
                        return;
                    }
                }
                var ack = TaskStatusAck.newBuilder().setTaskId(report.taskId())
                        .setSequence(report.reportSequence()).setDirective(TaskStatusAck.Directive.CONTINUE)
                        .build().toByteArray();
                exchange.getResponseHeaders().set("Content-Type", TaskStatusWire.MEDIA_TYPE);
                exchange.sendResponseHeaders(200, ack.length);
                exchange.getResponseBody().write(ack);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        });
        endpoint.start();
        try (var reporter = new RemoteTaskStatusReporter("http://127.0.0.1:" + endpoint.getAddress().getPort(),
                TestObservability.TRACING, TestObservability.METRICS, null)) {
            TaskInfo info = submit(request, "OUTPUT", reporter);
            try {
                awaitUnconsumedResult(info, request);
                startResultCleanup();
                consumeActualResult(request);
                assertThat(held.await(5, TimeUnit.SECONDS)).isTrue();
                await().atMost(Duration.ofSeconds(5)).until(info::resourcesReleased);
                assertThat(runtime.arbitrator().getAvailableMemory()).isEqualTo(available);
                assertThat(reports.reports()).filteredOn(OutputTaskMemoryReleaseTest::isNativeReleased)
                        .hasSize(holdReleasedReport ? 1 : 0);

                String next = manager.submitTask(descriptor(secondRequest, "OUTPUT"), SubmitTaskRequest.from(secondRequest));
                TaskInfo second = manager.tasks.get(next);
                consumeActualResult(secondRequest);
                await().atMost(Duration.ofSeconds(5)).until(() -> manager.tasks.get(second.taskId()) == null);
                assertThat(second.resourcesReleased()).isTrue();
                assertThat(runtime.arbitrator().getAvailableMemory()).isEqualTo(available);
                assertThat(manager.tasks.get(info.taskId())).isSameAs(info);
                assertThat(reports.reports()).filteredOn(OutputTaskMemoryReleaseTest::isNativeReleased)
                        .hasSize(holdReleasedReport ? 2 : 1)
                        .anyMatch(report -> report.taskId().equals(secondRequest.getAttempt().getTaskId()));
            } finally {
                acknowledge.countDown();
            }
            await().atMost(Duration.ofSeconds(5)).until(() -> manager.tasks.get(info.taskId()) == null);
            assertThat(reports.reports()).filteredOn(OutputTaskMemoryReleaseTest::isNativeReleased).hasSize(2);
        } finally {
            endpoint.stop(0);
            org.pangrova.common.concurrent.ExecutorDrain.shutdownAndAwait(httpHandlers, 3000);
        }
    }

    private static boolean isNativeReleased(org.pangrova.protocol.rest.task.TaskStatusReport report) {
        if (report.canonicalTaskReport() == null) return false;
        try {
            return TaskReport.parseFrom(report.canonicalTaskReport()).getExecutionDrainState()
                    == org.pangrova.protocol.v2.ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED;
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
            throw new IllegalStateException("Actual producer supplied malformed canonical metadata", malformed);
        }
    }

    @Test
    void memoryReleaseIsIdempotent() {
        var arbitrator = new MemoryArbitrator(ActualNativeTaskFixture.TOTAL_MEMORY_BYTES, 0);
        var requestor = new TaskMemoryRequestor(arbitrator, 0.0);
        var request = ActualNativeTaskFixture.valuesTaskRequest(true);
        var info = new TaskInfo("idempotent-release", request.getAttempt().getQueryOrJobId(),
                SubmitTaskRequest.from(request), descriptor(request, "OUTPUT"), Instant.now());
        requestor.requestMemoryForTask(info.memoryAccount());
        requestor.requestBufferMemory(info.memoryAccount());
        assertThat(arbitrator.getAvailableMemory()).isLessThan(ActualNativeTaskFixture.TOTAL_MEMORY_BYTES);
        requestor.releaseTaskMemory(info.memoryAccount());
        requestor.releaseBufferMemory(info.memoryAccount());
        long afterFirst = arbitrator.getAvailableMemory();
        requestor.releaseTaskMemory(info.memoryAccount());
        requestor.releaseBufferMemory(info.memoryAccount());
        assertThat(arbitrator.getAvailableMemory()).isEqualTo(afterFirst);
        assertThat(afterFirst).isEqualTo(ActualNativeTaskFixture.TOTAL_MEMORY_BYTES);
    }

    @Test
    void outputTaskReportsOneCanonicalFinishOnlyAfterNativeCompletion() throws Exception {
        runtime = new ActualNativeTaskFixture();
        TaskRequest request = runtime.valuesRequest(true);
        TaskInfo info = submit(request, "OUTPUT", reports);
        awaitUnconsumedResult(info, request);
        consumeActualResult(request);
        awaitFinishedReport();
        var finished = reports.reports().stream().filter(report -> "FINISHED".equals(report.state())).toList();
        assertThat(finished).hasSize(1);
        TaskReport report = TaskReport.parseFrom(finished.get(0).canonicalTaskReport());
        assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
        assertThat(report.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE);
        assertThat(report.getAttempt()).isEqualTo(request.getAttempt());
        assertThat(report).isEqualTo(TaskReport.parseFrom(runtime.executor().getTaskStatus(info.executorHandle())));
        assertThat(info.terminalTaskStats()).isEqualTo(report.getNativeTaskStats());
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
        var request = runtime.valuesRequest(true);
        TaskInfo info = submit(request, "OUTPUT", reports);
        awaitUnconsumedResult(info, request);
        assertThat(info.executorHandle()).isPositive();
        assertThat(info.allocatedBufferMemory()).isPositive();
        manager.releaseTaskExecutor(info.taskId());
        long afterFirst = runtime.arbitrator().getAvailableMemory();
        byte[] firstReceipt = info.executorReleaseReport();
        assertCancelledRetirement(info, request);
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
                "unavailable-result-worker", 0.0, runtime.admissionIncarnation());
        var request = runtime.valuesRequest(true);
        String id = manager.submitTask(descriptor(request, "OUTPUT"), SubmitTaskRequest.from(request));
        var info = manager.tasks.get(id);
        assertThat(info.getState()).isEqualTo(org.pangrova.worker.enums.TaskState.FAILED);
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
        awaitUnconsumedResult(info, request);
        assertThat(info.executorHandle()).isPositive();
        assertThat(info.allocatedBufferMemory()).isPositive();
        assertThat(runtime.arbitrator().getAvailableMemory())
                .isLessThanOrEqualTo(availableBeforeTask - info.allocatedBufferMemory());
        manager.close();
        assertCancelledRetirement(info, request);
        assertThat(info.executorHandle()).isZero();
        assertThat(info.allocatedBufferMemory()).isZero();
        assertThat(runtime.transports().resultTransport().boundPort()).isEqualTo(-1);
        assertThat(runtime.arbitrator().getAvailableMemory()).isEqualTo(availableBeforeTask);
        assertThatThrownBy(() -> manager.submitTask(descriptor(request, "OUTPUT"), SubmitTaskRequest.from(request)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("draining");
        manager.close();
        assertThat(runtime.arbitrator().getAvailableMemory()).isEqualTo(availableBeforeTask);
    }

    @Test
    void shutdownPreservesTheActuallyConsumedNativeFinish() throws Exception {
        runtime = new ActualNativeTaskFixture();
        long availableBeforeTask = runtime.arbitrator().getAvailableMemory();
        var request = runtime.valuesRequest(true);
        var info = submit(request, "OUTPUT", reports);
        awaitUnconsumedResult(info, request);
        consumeActualResult(request);
        awaitFinishedReport();
        assertThat(info.executorHandle()).isPositive();
        manager.close();
        var receipt = TaskReport.parseFrom(info.executorReleaseReport());
        assertThat(info.getState()).isEqualTo(org.pangrova.worker.enums.TaskState.FINISHED);
        assertThat(receipt.getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
        assertThat(receipt.getAttempt()).isEqualTo(request.getAttempt());
        assertThat(receipt.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE);
        assertThat(receipt.getExecutionDrainState()).isEqualTo(org.pangrova.protocol.v2.ExecutionDrainState
                .EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED);
        assertThat(receipt.hasError()).isFalse();
        assertThat(receipt.hasNativeTaskStats()).isTrue();
        assertThat(info.executorHandle()).isZero();
        assertThat(info.allocatedBufferMemory()).isZero();
        assertThat(runtime.arbitrator().getAvailableMemory()).isEqualTo(availableBeforeTask);
        assertThat(reports.reports()).noneMatch(report -> "CANCELLED".equals(report.state()));
        assertThat(reports.reports().stream().filter(OutputTaskMemoryReleaseTest::isNativeReleased))
                .singleElement().satisfies(report -> assertThat(report.state()).isEqualTo("FINISHED"));
    }

    private static Throwable closeOwner(Runnable operation, Throwable aggregate) {
        try {
            operation.run();
        } catch (RuntimeException | Error failure) {
            if (aggregate == null) return failure;
            if (aggregate != failure) aggregate.addSuppressed(failure);
        }
        return aggregate;
    }

    private void awaitUnconsumedResult(TaskInfo info, TaskRequest request) throws Exception {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(info.getState()).isEqualTo(org.pangrova.worker.enums.TaskState.RUNNING);
            assertThat(TaskReport.parseFrom(runtime.executor().getTaskStatus(info.executorHandle())).getState())
                    .isEqualTo(TaskState.TASK_STATE_RUNNING);
        });
        assertThat(reports.reports()).anyMatch(report -> "RUNNING".equals(report.state()));
        assertThat(reports.reports()).noneMatch(report -> "FINISHED".equals(report.state()));
        assertThat(runtime.drainEvidence(info.executorHandle(), request).getDrained()).isFalse();
    }

    private void assertCancelledRetirement(TaskInfo info, TaskRequest request) throws Exception {
        var actual = TaskReport.parseFrom(info.executorReleaseReport());
        assertThat(info.getState()).isEqualTo(org.pangrova.worker.enums.TaskState.CANCELLED);
        assertThat(info.resourcesReleased()).isTrue();
        assertThat(actual.getState()).isEqualTo(TaskState.TASK_STATE_CANCELLED);
        assertThat(actual.getAttempt()).isEqualTo(request.getAttempt());
        assertThat(actual.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE);
        assertThat(actual.getExecutionDrainState()).isEqualTo(org.pangrova.protocol.v2.ExecutionDrainState
                .EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED);
        assertThat(info.allocatedMemory()).isZero();
        assertThat(reports.reports()).noneMatch(report -> "FINISHED".equals(report.state()));
        assertThat(reports.reports().stream().filter(OutputTaskMemoryReleaseTest::isNativeReleased))
                .singleElement().satisfies(report -> {
                    assertThat(report.state()).isEqualTo("CANCELLED");
                    assertThat(TaskReport.parseFrom(report.canonicalTaskReport()).getExecutionDrainState())
                            .isEqualTo(org.pangrova.protocol.v2.ExecutionDrainState
                                    .EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED);
                });
    }

    private TaskInfo submit(TaskRequest request, String taskType, TaskStatusReporter reporter) {
        var resources = new ResourceManager(runtime.arbitrator(), new ThreadBudgetTracker(8, 4, 2), new SpillManager());
        var monitor = Executors.newFixedThreadPool(2, task -> new Thread(task, "actual-native-completion"));
        manager = new TaskManager(new TaskManagerContext(reporter, runtime.executor(),
                new MemoryRefs(resources, runtime.nodePool(), null), runtime.transports(), monitor),
                "actual-native-budget-worker", 0.0, runtime.admissionIncarnation());
        String id = manager.submitTask(descriptor(request, taskType), SubmitTaskRequest.from(request));
        return manager.tasks.get(id);
    }

    private void awaitFinishedReport() {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(reports.reports()).anyMatch(report -> "FINISHED".equals(report.state())));
    }

    private void startResultCleanup() {
        startResultCleanup(Duration.ofHours(1));
    }

    private void startResultCleanup(Duration interval) {
        resultCleanup = new java.util.concurrent.ScheduledThreadPoolExecutor(1);
        resultCleanup.setRemoveOnCancelPolicy(true);
        manager.startPeriodicCleanup(resultCleanup, Duration.ofHours(1), interval);
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
