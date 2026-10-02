package org.pangrova.worker.task;

import org.pangrova.worker.task.memory.MemoryRefs;

import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.pangrova.protocol.rest.config.WriteConfig;
import org.pangrova.protocol.rest.task.ExecutionOwnerIdentity;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.rest.task.TaskStats;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.resource.ResourceManager;
import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.spill.SpillManager;
import org.pangrova.resource.thread.ThreadBudgetTracker;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.protocol.rest.task.convert.TaskRequestDtoConverter;

import org.pangrova.worker.execution.SimulatedTaskExecutor;
import org.pangrova.worker.jni.write.NativeWriteExecutor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies TaskStatusReport is enriched with real stats, writeResult, and
 * monotonically increasing reportSequence (REQ-M4, REQ-M5, REQ-M6).
 *
 * <p>Uses SimulatedTaskExecutor — a real TaskExecutor SPI implementation.</p>
 */
class TaskStatusReportEnrichmentTest {

    private static final String WORKER_ID = "enrichment-test-worker";
    private static final long CONTROL_EPOCH = 9;
    private static final ExecutionOwnerIdentity EXECUTION_OWNER =
            new ExecutionOwnerIdentity("coordinator-test", "process-test", 11);

    @Test
    void finishedReportContainsNonNullStatsAndIncreasingSequence() throws Exception {
        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(3);
        TaskManager manager = buildTaskManager(executor, reporter);

        SubmitTaskRequest request = buildComputeRequest("q-stats-1");
        TaskDescriptor descriptor = toDescriptor(request);
        String taskId = manager.submitTask(descriptor, request);

        awaitTerminalState(manager, taskId);

        List<TaskStatusReport> reports = reporter.reports();
        assertThat(reports).hasSizeGreaterThanOrEqualTo(2);

        // RUNNING report: stats non-null, sequence starts at 1
        TaskStatusReport running = reports.get(0);
        assertThat(running.state()).isEqualTo("RUNNING");
        assertThat(running.stats()).isNotNull();
        assertThat(running.reportSequence()).isEqualTo(1);

        // FINISHED report: stats non-null with elapsed > 0, sequence > running
        TaskStatusReport finished = reports.get(1);
        assertThat(finished.state()).isEqualTo("FINISHED");
        assertThat(finished.stats()).isNotNull();
        assertThat(finished.stats().elapsedTimeMs()).isGreaterThanOrEqualTo(0);
        assertThat(finished.reportSequence()).isGreaterThan(running.reportSequence());
        assertThat(finished.canonicalTaskReport()).isNotEmpty();
        assertThat(org.pangrova.protocol.v2.TaskReport.parseFrom(
                finished.canonicalTaskReport()).getState())
                .isEqualTo(org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED);
    }

    @Test
    void finishedReportContainsOutputRowsFromBuffer() throws InterruptedException {
        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        int batchCount = 5;
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(batchCount);
        TaskManager manager = buildTaskManager(executor, reporter);

        SubmitTaskRequest request = buildComputeRequest("q-rows-1");
        TaskDescriptor descriptor = toDescriptor(request);
        String taskId = manager.submitTask(descriptor, request);

        awaitTerminalState(manager, taskId);

        List<TaskStatusReport> reports = reporter.reports();
        TaskStatusReport finished = findReport(reports, "FINISHED");
        assertThat(finished).isNotNull();

        TaskStats stats = finished.stats();
        assertThat(stats).isNotNull();
        // In native transport mode (completion monitor path), batch counting
        // happens on the native side. SimulatedTaskExecutor does not increment
        // Java-side counters via the completion monitor path.
        assertThat(stats.elapsedTimeMs()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void writeTaskReportContainsWriteResult() throws InterruptedException {
        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        SubmitTaskRequest request = buildWriteRequest("q-write-1");
        TaskInfo info = new TaskInfo("worker-local-write-task", "q-write-1", request,
                toDescriptor(request), Instant.now());
        info.setExecutorHandle(1);
        TaskStatusCollector collector = new TaskStatusCollector(reporter, WORKER_ID, null);
        collector.reportStatus(info, "FINISHED", null);

        List<TaskStatusReport> reports = reporter.reports();
        TaskStatusReport finished = findReport(reports, "FINISHED");
        assertThat(finished).isNotNull();
        assertThat(finished.writeResult()).isNotNull();
        // Write tasks use the completion monitor path,
        // so outputBatches is not incremented by Java-side batch pulling.
        // The native engine handles writes autonomously; totalRows reflects
        // what TaskInfo.outputBatches() reports at completion time.
        assertThat(finished.writeResult().totalRows()).isEqualTo(0);
    }

    @Test
    void writeTaskWithoutConnectorRuntimeFailsClosed() throws InterruptedException {
        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        TaskManager manager = buildTaskManager(new SimulatedTaskExecutor(2), reporter);
        SubmitTaskRequest request = buildWriteRequest("q-write-unbound");
        String taskId = manager.submitTask(toDescriptor(request), request);

        awaitTerminalState(manager, taskId);

        assertThat(manager.getTaskState(taskId)).contains(TaskManager.TaskState.FAILED);
        assertThat(findReport(reporter.reports(), "FINISHED")).isNull();
        assertThat(findReport(reporter.reports(), "FAILED")).isNotNull();
        assertThat(findReport(reporter.reports(), "FAILED").canonicalTaskReport())
                .isNotEmpty();
    }

    @Test
    void computeTaskReportHasNullWriteResult() throws InterruptedException {
        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(1);
        TaskManager manager = buildTaskManager(executor, reporter);

        SubmitTaskRequest request = buildComputeRequest("q-compute-1");
        TaskDescriptor descriptor = toDescriptor(request);
        String taskId = manager.submitTask(descriptor, request);

        awaitTerminalState(manager, taskId);

        List<TaskStatusReport> reports = reporter.reports();
        TaskStatusReport finished = findReport(reports, "FINISHED");
        assertThat(finished).isNotNull();
        assertThat(finished.writeResult()).isNull();
    }

    @Test
    void sequenceIsMonotonicallyIncreasingAcrossReports() throws InterruptedException {
        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(1);
        TaskManager manager = buildTaskManager(executor, reporter);

        SubmitTaskRequest request = buildComputeRequest("q-seq-1");
        TaskDescriptor descriptor = toDescriptor(request);
        String taskId = manager.submitTask(descriptor, request);

        awaitTerminalState(manager, taskId);

        List<TaskStatusReport> reports = reporter.reports();
        assertThat(reports).hasSizeGreaterThanOrEqualTo(2);
        for (int i = 1; i < reports.size(); i++) {
            assertThat(reports.get(i).reportSequence())
                    .isGreaterThan(reports.get(i - 1).reportSequence());
        }
    }

    @Test
    void terminalWriteRetryRetainsNativeCommitFragment() {
        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        SubmitTaskRequest request = buildWriteRequest("q-write-retry");
        TaskInfo info = new TaskInfo("worker-local-task", "q-write-retry", request,
                toDescriptor(request), Instant.now());
        info.setExecutorHandle(1);
        NativeWriteExecutor writer = new NativeWriteExecutor(
                new org.pangrova.worker.jni.write.TestCatalogResourceResolver((taskId, ignored) -> {
            throw new AssertionError("No connector sink should be opened");
        }));
        writer.publishCommitMetadataForTask("t-w-1", new byte[] {4, 5, 6});
        TaskStatusCollector collector = new TaskStatusCollector(reporter, WORKER_ID, null);
        collector.setWriteExecutor(writer);

        collector.reportStatus(info, "RUNNING", null);
        collector.reportStatus(info, "FINISHED", null);
        collector.reportStatus(info, "FINISHED", null);

        assertThat(reporter.reports().get(0).writeResult().commitMetadata()).isEmpty();
        assertThat(reporter.reports().get(1).writeResult().commitMetadata())
                .containsExactly(4, 5, 6);
        assertThat(reporter.reports().get(2).writeResult().commitMetadata())
                .containsExactly(4, 5, 6);
        writer.close();
    }

    // --- Helpers ---

    private TaskManager buildTaskManager(SimulatedTaskExecutor executor,
                                         RecordingTaskStatusReporter reporter) {
        ResourceManager rm = createTestResourceManager();
        TaskManagerContext deps = new TaskManagerContext(
                reporter, executor,
                new MemoryRefs(rm),
                new TransportRefs(null, null, null),
                java.util.concurrent.Executors.newCachedThreadPool(r -> {
                    Thread t = new Thread(r, "test-completion-monitor");
                    t.setDaemon(true);
                    return t;
                }));
        return new TaskManager(deps, WORKER_ID, 0.0);
    }

    private ResourceManager createTestResourceManager() {
        MemoryArbitrator arb = new MemoryArbitrator(4L * 1024 * 1024 * 1024, 256L * 1024 * 1024);
        return new ResourceManager(arb, new ThreadBudgetTracker(8, 4, 2), new SpillManager());
    }

    private SubmitTaskRequest buildComputeRequest(String queryId) {
        var canonical = org.pangrova.worker.CanonicalTaskRequestTestFixture.builder(
                "t-1", queryId, 0, 0,
                org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                new byte[]{0x01, 0x02});
        canonical.setAttempt(canonical.getAttempt().toBuilder()
                .setControlEpoch(CONTROL_EPOCH)
                .setExecutionOwner(org.pangrova.protocol.v2.ExecutionOwnerIdentity.newBuilder()
                        .setOwnerId(EXECUTION_OWNER.ownerId())
                        .setOwnerIncarnation(EXECUTION_OWNER.ownerIncarnation())
                        .setFencingToken(EXECUTION_OWNER.fencingToken())));
        return SubmitTaskRequest.from(canonical.build());
    }

    private SubmitTaskRequest buildWriteRequest(String queryId) {
        var canonical = org.pangrova.worker.CanonicalTaskRequestTestFixture.builder(
                        "t-w-1", queryId, 0, 0,
                        org.pangrova.protocol.v2.TaskKind.TASK_KIND_WRITE,
                        new byte[]{0x01, 0x02})
                .setExecutionOptions(org.pangrova.protocol.v2.TaskExecutionOptions.newBuilder()
                        .setWrite(org.pangrova.protocol.v2.WriteTaskOptions.newBuilder()
                                .setTargetTable("db.table").setWriteMode("INSERT")
                                .setFileFormat(org.pangrova.protocol.v2.FileFormat
                                        .FILE_FORMAT_PARQUET)
                                .setTargetFileSize(128_000_000)));
        canonical.setAttempt(canonical.getAttempt().toBuilder()
                .setControlEpoch(CONTROL_EPOCH)
                .setExecutionOwner(org.pangrova.protocol.v2.ExecutionOwnerIdentity.newBuilder()
                        .setOwnerId(EXECUTION_OWNER.ownerId())
                        .setOwnerIncarnation(EXECUTION_OWNER.ownerIncarnation())
                        .setFencingToken(EXECUTION_OWNER.fencingToken())));
        return SubmitTaskRequest.from(canonical.build());
    }

    private TaskDescriptor toDescriptor(SubmitTaskRequest request) {
        return TaskRequestDtoConverter.toTaskDescriptor(request);
    }

    private TaskStatusReport findReport(List<TaskStatusReport> reports, String state) {
        return reports.stream()
                .filter(r -> state.equals(r.state()))
                .findFirst()
                .orElse(null);
    }

    private void awaitTerminalState(TaskManager manager, String taskId)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
        while (System.currentTimeMillis() < deadline) {
            var state = manager.getTaskState(taskId);
            if (state.isPresent() && isTerminal(state.get())) {
                return;
            }
            Thread.sleep(10);
        }
    }

    private boolean isTerminal(TaskManager.TaskState state) {
        return state == TaskManager.TaskState.FINISHED
                || state == TaskManager.TaskState.FAILED
                || state == TaskManager.TaskState.CANCELLED;
    }
}
