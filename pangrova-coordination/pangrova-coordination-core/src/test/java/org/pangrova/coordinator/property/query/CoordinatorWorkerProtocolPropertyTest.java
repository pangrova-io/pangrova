package org.pangrova.coordinator.property.query;

import com.google.protobuf.ByteString;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.query.task.UnregisteredWorkerException;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.core.scoring.WorkerPerformanceScorer;
import org.pangrova.observability.sdk.noop.NoOpTracingProvider;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.rest.config.OutputConfig;
import org.pangrova.protocol.resource.StageResourceGrantCodec;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.rest.config.WriteConfig;
import org.pangrova.client.core.error.ErrorDetail;
import org.pangrova.protocol.rest.task.*;
import org.pangrova.protocol.rest.task.exchange.ExchangeSourceDescriptor;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordination.inmemory.InMemoryWorkerRegistry;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property 36: Coordinator-Worker protocol field completeness.
 * <p>For any Coordinator-Worker coordination protocol message
 * (SubmitTaskRequest/Response, TaskStatusReport/Response,
 * task attempt and exchange bindings), all fields set by the sender
 * are preserved after construction — no field loss or default
 * value override.</p>
 *
 * <p><b>Validates: Requirements 54.1, 54.2, 54.4</b></p>
 */
@Tag("Feature: architecture-remediation")
@Tag("Property 36: Coordinator-Worker protocol field completeness")
class CoordinatorWorkerProtocolPropertyTest {

    private static final String WORKER_ID = "w-proto";
    private static final AtomicLong SEQ = new AtomicLong(0);

    // ── Property 36: SubmitTaskRequest field completeness ──

    @Property(tries = 10)
    void submitTaskRequestPreservesAllIdentityFields(
            @ForAll("taskIds") String taskId,
            @ForAll("queryIds") String queryId,
            @ForAll @IntRange(min = 0, max = 10) int stageId,
            @ForAll @IntRange(min = 0, max = 100) int partitionId) {

        var canonical = org.pangrova.coordinator.CanonicalTaskRequestTestFixture.builder(
                        taskId, queryId, stageId, partitionId,
                        org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                        "plan-bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .addSplits(org.pangrova.protocol.v2.DataSplit.newBuilder()
                        .setSplitId("split1"))
                .build();
        SubmitTaskRequest req = SubmitTaskRequest.from(canonical);

        assertThat(req.taskRequest()).isEqualTo(canonical);
        assertThat(req.taskRequest().getAttempt().getTaskId()).isEqualTo(taskId);
        assertThat(req.taskRequest().getAttempt().getQueryOrJobId()).isEqualTo(queryId);
        assertThat(req.taskRequest().getAttempt().getStageId()).isEqualTo(stageId);
        assertThat(req.taskRequest().getAttempt().getPartitionId()).isEqualTo(partitionId);
        assertThat(req.taskRequest().getTaskKind())
                .isEqualTo(org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ);
        assertThat(req.taskRequest().getSplits(0).getSplitId()).isEqualTo("split1");
    }

    // ── Property 36: TaskStatusReport identity mirrors SubmitTaskRequest ──

    @Property(tries = 10)
    void taskStatusReportIdentityMirrorsSubmitRequest(
            @ForAll("taskIds") String taskId,
            @ForAll("queryIds") String queryId,
            @ForAll @IntRange(min = 0, max = 10) int stageId,
            @ForAll @IntRange(min = 0, max = 100) int partitionId) {

        SubmitTaskRequest req =
                org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                        taskId, queryId, stageId, partitionId,
                        org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                        "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        var attempt = req.taskRequest().getAttempt();

        TaskStatusReport report = new TaskStatusReport(
                attempt.getTaskId(), attempt.getQueryOrJobId(),
                attempt.getStageId(), attempt.getPartitionId(),
                "COMPUTE", WORKER_ID, "RUNNING",
                null, null, null, 1L, System.currentTimeMillis(), null, null,
                TaskAttemptDescriptor.from(attempt));

        assertThat(report.taskId()).isEqualTo(attempt.getTaskId());
        assertThat(report.queryId()).isEqualTo(attempt.getQueryOrJobId());
        assertThat(report.stageId()).isEqualTo(attempt.getStageId());
        assertThat(report.partitionId()).isEqualTo(attempt.getPartitionId());
        assertThat(report.taskType()).isEqualTo("COMPUTE");
    }

    // ── Property 36: Sequence-based dedup in TaskStatusReceiver ──

    @Property(tries = 10)
    void staleReportsAreRejectedBySequenceDedup(
            @ForAll("taskIds") String taskId,
            @ForAll("queryIds") String queryId) {

        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        wm.registerWorker(TestWorkerInfoBuilder.active(WORKER_ID, "localhost", 8080, 9090));
        QueryManager qm = buildMinimalQueryManager(tracker, wm);
        TaskStatusReceiver receiver = new TaskStatusReceiver(
                TaskStatusReceiverContext.withoutSpeculation(qm, tracker, wm));

        tracker.registerTask(queryId, taskId);
        tracker.registerTaskAttempt(taskId, TaskAttemptDescriptor.of(1, 0));
        registerReadRequest(tracker, queryId, taskId, TaskAttemptDescriptor.of(1, 0));
        tracker.markScheduled(queryId, taskId);

        QueryStateMachine sm = new QueryStateMachine();
        qm.registerStateMachine(queryId, sm);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);

        long seq1 = SEQ.incrementAndGet();
        long seq2 = SEQ.incrementAndGet();

        // First report accepted
        TaskStatusReport exact = report(taskId, queryId, "RUNNING", seq2);
        boolean accepted1 = receiver.onTaskStatus(exact);
        assertThat(accepted1).isTrue();

        // Stale report (lower sequence) rejected
        boolean accepted2 = receiver.onTaskStatus(report(taskId, queryId, "RUNNING", seq1));
        assertThat(accepted2).as("Stale report with seq=%d should be rejected", seq1).isFalse();

        // Exact completed publication is reacknowledged after a lost HTTP ACK.
        boolean accepted3 = receiver.onTaskStatus(exact);
        assertThat(accepted3).as("Exact published seq=%d should be reacknowledged", seq2).isTrue();
    }

    @Example
    void unknownTaskIdentityIsRejectedFailClosed() {
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        WorkerManager wm = new WorkerManager(
                20_000, 60_000, new InMemoryWorkerRegistry());
        wm.registerWorker(TestWorkerInfoBuilder.active(
                WORKER_ID, "localhost", 8080, 9090));
        tracker.registerTask("q-unknown", "t-unknown");
        tracker.markScheduled("q-unknown", "t-unknown");
        TaskStatusReceiver receiver = new TaskStatusReceiver(
                TaskStatusReceiverContext.withoutSpeculation(
                        buildMinimalQueryManager(tracker, wm), tracker, wm));

        assertThat(receiver.onTaskStatus(report(
                "t-unknown", "q-unknown", "RUNNING", 1L))).isFalse();
    }

    @Example
    void scheduledTaskWithoutIssuedRequestIsRejectedBeforeSequenceReservation() {
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        WorkerManager wm = new WorkerManager(
                20_000, 60_000, new InMemoryWorkerRegistry());
        wm.registerWorker(TestWorkerInfoBuilder.active(
                WORKER_ID, "localhost", 8080, 9090));
        tracker.registerTask("q-unbound", "t-unbound");
        tracker.registerTaskAttempt("t-unbound", TaskAttemptDescriptor.of(1, 0));
        tracker.markScheduled("q-unbound", "t-unbound");
        TaskStatusReceiver receiver = new TaskStatusReceiver(
                TaskStatusReceiverContext.withoutSpeculation(
                        buildMinimalQueryManager(tracker, wm), tracker, wm));

        assertThat(receiver.onTaskStatus(report(
                "t-unbound", "q-unbound", "RUNNING", 7L))).isFalse();
    }

    @Example
    void staleAttemptIsRejectedBeforeSequenceIsConsumed() {
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        WorkerManager wm = new WorkerManager(
                20_000, 60_000, new InMemoryWorkerRegistry());
        wm.registerWorker(TestWorkerInfoBuilder.active(
                WORKER_ID, "localhost", 8080, 9090));
        tracker.registerTask("q-fence", "t-fence");
        TaskAttemptDescriptor expected = TaskAttemptDescriptor.of(2, 1);
        tracker.registerTaskAttempt("t-fence", expected);
        registerReadRequest(tracker, "q-fence", "t-fence", expected);
        tracker.markScheduled("q-fence", "t-fence");
        TaskStatusReceiver receiver = new TaskStatusReceiver(
                TaskStatusReceiverContext.withoutSpeculation(
                        buildMinimalQueryManager(tracker, wm), tracker, wm));

        assertThat(receiver.onTaskStatus(report(
                "t-fence", "q-fence", "RUNNING", 7L))).isFalse();
        TaskStatusReport matching = new TaskStatusReport(
                "t-fence", "q-fence", 0, 0, "COMPUTE", WORKER_ID,
                "RUNNING", null, null, null, 7L, System.currentTimeMillis(),
                null, null, expected);
        assertThat(receiver.onTaskStatus(matching)).isTrue();
    }

    @Example
    void statusFromDifferentRegisteredWorkerIsRejectedBeforeSequenceIsConsumed() {
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        WorkerManager wm = new WorkerManager(
                20_000, 60_000, new InMemoryWorkerRegistry());
        wm.registerWorker(TestWorkerInfoBuilder.active(
                WORKER_ID, "localhost", 8080, 9090));
        wm.registerWorker(TestWorkerInfoBuilder.active(
                "w-other", "localhost", 8081, 9091));
        tracker.registerTask("q-worker-fence", "t-worker-fence");
        tracker.registerTaskAttempt(
                "t-worker-fence", TaskAttemptDescriptor.of(1, 0));
        registerReadRequest(tracker, "q-worker-fence", "t-worker-fence",
                TaskAttemptDescriptor.of(1, 0));
        tracker.assignWorker("t-worker-fence", WORKER_ID);
        tracker.markScheduled("q-worker-fence", "t-worker-fence");
        TaskStatusReceiver receiver = new TaskStatusReceiver(
                TaskStatusReceiverContext.withoutSpeculation(
                        buildMinimalQueryManager(tracker, wm), tracker, wm));
        TaskStatusReport wrongWorker = new TaskStatusReport(
                "t-worker-fence", "q-worker-fence", 0, 0, "COMPUTE",
                "w-other", "RUNNING", null, null, null, 7L,
                System.currentTimeMillis(), null, null,
                TaskAttemptDescriptor.of(1, 0));

        assertThat(receiver.onTaskStatus(wrongWorker)).isFalse();
        assertThat(receiver.onTaskStatus(report(
                "t-worker-fence", "q-worker-fence", "RUNNING", 7L))).isTrue();
    }

    @Example
    void artifactCommitFailureIsRetryableAndStaleTerminalHasNoSideEffect()
            throws Exception {
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        WorkerManager wm = new WorkerManager(
                20_000, 60_000, new InMemoryWorkerRegistry());
        wm.registerWorker(TestWorkerInfoBuilder.active(
                WORKER_ID, "localhost", 8080, 9090));
        TaskAttemptDescriptor attempt = TaskAttemptDescriptor.of(9, 0);
        long ownerEpoch = 9;
        var target = org.pangrova.protocol.v2.ArtifactWriteBinding.newBuilder()
                .setArtifactId("index-1")
                .setArtifactType("DISKANN")
                .setGeneration("2")
                .setSourceId("catalog.schema.table")
                .setSourceSnapshot("snapshot-1")
                .setStagingUri("s3://bucket/staging/job-1")
                .addExpectedSourceObjectIds("source-1")
                .setPublicationFencingToken(attempt.fencingToken())
                .setWorkUnitId("page-1")
                .setIdempotencyKey("job-1:page-1")
                .setWorkKind(org.pangrova.protocol.v2.ArtifactWorkKind
                        .ARTIFACT_WORK_KIND_SOURCE_MAP)
                .setExecutionKind(org.pangrova.protocol.v2.ArtifactExecutionKind
                        .ARTIFACT_EXECUTION_KIND_BUILD)
                .addDependencies(org.pangrova.protocol.v2.ArtifactDependency.newBuilder()
                        .setKind("SOURCE_SNAPSHOT")
                        .setId("catalog.schema.table")
                        .setVersion("snapshot-1"))
                .setDependencySetChecksum(org.pangrova.protocol.v2.Checksum.newBuilder()
                        .setAlgorithm(org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(com.google.protobuf.ByteString.copyFrom(
                                java.util.HexFormat.of().parseHex(
                                        org.pangrova.generation.api.GenerationDependencySet
                                                .sourceSnapshot("catalog.schema.table",
                                                        "snapshot-1").sha256()))))
                .setEmptyPartitionPolicy(org.pangrova.protocol.v2
                        .ArtifactEmptyPartitionPolicy
                        .ARTIFACT_EMPTY_PARTITION_POLICY_FORBID)
                .build();
        var binding = org.pangrova.protocol.v2.OperatorBinding.newBuilder()
                .setPlanNodeId("build")
                .setOutputArtifact(target)
                .build();
        var canonical = org.pangrova.coordinator.CanonicalTaskRequestTestFixture.builder(
                        "page-1", "job-1", 4, 0,
                        org.pangrova.protocol.v2.TaskKind.TASK_KIND_BACKGROUND_BUILD,
                        new byte[]{1})
                .setAttempt(org.pangrova.protocol.v2.TaskAttemptId.newBuilder()
                        .setQueryOrJobId("job-1").setTaskId("page-1")
                        .setStageId(4).setPlanEpoch(attempt.planEpoch())
                        .setAttemptId(attempt.attemptId())
                        .setFencingToken(attempt.fencingToken())
                        .setControlEpoch(ownerEpoch)
                        .setExecutionOwner(org.pangrova.protocol.v2.ExecutionOwnerIdentity
                                .newBuilder().setOwnerId("coordinator-a")
                                .setOwnerIncarnation("process-a")
                                .setFencingToken(ownerEpoch)))
                .addOperatorBindings(binding)
                .setResourceGrant(StageResourceGrant.newBuilder()
                        .setGrantId("page-1:grant").setNativeMemoryBytes(4096)
                        .setCpuSlots(1).setSpillBytes(4096))
                .build();
        SubmitTaskRequest request = SubmitTaskRequest.from(canonical);
        tracker.registerTask("job-1", "page-1");
        tracker.registerTaskAttempt("page-1", attempt);
        tracker.registerTaskRequest("page-1", request);
        tracker.assignWorker("page-1", WORKER_ID);
        tracker.markScheduled("job-1", "page-1");
        tracker.registerTask("job-1", "other-page");
        tracker.registerTaskAttempt("other-page", attempt);
        tracker.markScheduled("job-1", "other-page");

        AtomicBoolean durable = new AtomicBoolean(false);
        AtomicInteger commitAttempts = new AtomicInteger();
        CountDownLatch retryCommitEntered = new CountDownLatch(1);
        CountDownLatch releaseRetryCommit = new CountDownLatch(1);
        TaskStatusReceiver receiver = new TaskStatusReceiver(
                new TaskStatusReceiverContext(
                        buildMinimalQueryManager(tracker, wm), tracker, wm,
                        null, null, null, null, null, null, null, null, null,
                        new org.pangrova.coordinator.query.task.report.GenerationWorkReportSink() {
                            @Override
                            public boolean commit(String tenantId, org.pangrova.protocol.v2.TaskReport status,
                                    byte[] originalWireBytes) {
                                assertThat(originalWireBytes).isEqualTo(status.toByteArray());
                                if (commitAttempts.incrementAndGet() == 2) {
                                    retryCommitEntered.countDown();
                                    await(releaseRetryCommit);
                                }
                                return durable.get();
                            }

                            @Override
                            public boolean ownsExecution(String tenantId, String executionId) {
                                return false;
                            }
                        }));
        TaskStatusReport terminal = artifactReport(request, target);

        assertThat(receiver.onTaskStatus(terminal)).isFalse();
        assertThat(commitAttempts).hasValue(1);
        assertThat(receiver.onTaskStatus(
                artifactReport(request, target, 10L))).isFalse();
        assertThat(commitAttempts).hasValue(1);
        assertThat(tracker.getTaskState("job-1", "page-1"))
                .isEqualTo(org.pangrova.coordinator.task.TaskStateMachine.State.SCHEDULED);
        tracker.updateTaskState("job-1", "page-1", "RUNNING");
        durable.set(true);
        ExecutorService reports = Executors.newFixedThreadPool(2);
        try {
            var retry = reports.submit(() -> receiver.onTaskStatus(terminal));
            assertThat(retryCommitEntered.await(5, TimeUnit.SECONDS)).isTrue();
            var stale = reports.submit(() -> receiver.onTaskStatus(
                    artifactReport(request, target, 10L)));
            assertThat(stale.isDone()).isFalse();
            releaseRetryCommit.countDown();

            assertThat(retry.get(5, TimeUnit.SECONDS)).isTrue();
            // A different sequence is not the published fact, even when its
            // Native payload is identical. The exact original can be ACKed
            // after a lost response without repeating the durable commit.
            assertThat(stale.get(5, TimeUnit.SECONDS)).isFalse();
            assertThat(receiver.onTaskStatus(terminal)).isTrue();
            assertThat(commitAttempts).hasValue(2);
            assertThat(tracker.getTaskState("job-1", "page-1"))
                    .isEqualTo(org.pangrova.coordinator.task.TaskStateMachine.State.FINISHED);
        } finally {
            releaseRetryCommit.countDown();
            reports.shutdownNow();
        }
    }

    // ── Property 36: Worker registration request field completeness ──

    @Property(tries = 10)
    void workerRegistrationRequestPreservesFields(
            @ForAll("workerIds") String workerId) {

        WorkerRegistrationRequest req = new WorkerRegistrationRequest(
                workerId, "host-" + workerId, 8080, 9090, 0,
                2, "rack-1", List.of("local-data"), null,
                org.pangrova.protocol.capability.inventory.CapabilityInventoryCodec.encode(
                        org.pangrova.coordinator.worker.TestWorkerInfoBuilder
                                .canonicalInventory()),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(workerId));

        assertThat(req.workerId()).isEqualTo(workerId);
        assertThat(req.httpHost()).isEqualTo("host-" + workerId);
        assertThat(req.httpPort()).isEqualTo(8080);
        assertThat(req.transportPort()).isEqualTo(9090);
        assertThat(req.rackId()).isEqualTo("rack-1");
        assertThat(req.dataLocality()).containsExactly("local-data");
    }

    // ── Property 36: Unregistered worker rejection ──

    @Property(tries = 10)
    void unregisteredWorkerReportsAreRejected(
            @ForAll("taskIds") String taskId,
            @ForAll("queryIds") String queryId,
            @ForAll("workerIds") String unknownWorker) {

        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        QueryManager qm = buildMinimalQueryManager(tracker, wm);
        TaskStatusReceiver receiver = new TaskStatusReceiver(
                TaskStatusReceiverContext.withoutSpeculation(qm, tracker, wm));

        TaskStatusReport rpt = new TaskStatusReport(
                taskId, queryId, 0, 0, "COMPUTE", unknownWorker,
                "RUNNING", null, null, null, 1L, System.currentTimeMillis(),
                null, null, TaskAttemptDescriptor.of(1, 0));

        assertThatThrownBy(() -> receiver.onTaskStatus(rpt))
                .isInstanceOf(UnregisteredWorkerException.class);
    }

    // ── Property 36: SubmitTaskRequest ALL fields completeness ──

    @Property(tries = 10)
    void submitTaskRequestPreservesAllFields(
            @ForAll("taskIds") String taskId,
            @ForAll("queryIds") String queryId,
            @ForAll @IntRange(min = 0, max = 10) int stageId,
            @ForAll @IntRange(min = 0, max = 100) int partitionId) {

        StageResourceGrant resourceGrant = StageResourceGrant.newBuilder()
                .setGrantId(taskId + ":grant")
                .setNativeMemoryBytes(1024L)
                .setCpuSlots(4)
                .setSpillBytes(2048L)
                .setObjectStoreReadBytes(1024L)
                .setObjectStoreWriteBytes(2048L)
                .build();
        Map<String, String> sessionProps = Map.of("key1", "val1", "key2", "val2");

        var options = org.pangrova.protocol.v2.TaskExecutionOptions.newBuilder()
                .putAllSessionProperties(sessionProps)
                .setDirectResultOutput(true)
                .setWrite(org.pangrova.protocol.v2.WriteTaskOptions.newBuilder()
                        .setTargetTable("catalog.db.table")
                        .setWriteMode("APPEND")
                        .setFileFormat(org.pangrova.protocol.v2.FileFormat.FILE_FORMAT_PARQUET)
                        .setTargetFileSize(128_000_000L));
        var canonical = org.pangrova.coordinator.CanonicalTaskRequestTestFixture.builder(
                        taskId, queryId, stageId, partitionId,
                        org.pangrova.protocol.v2.TaskKind.TASK_KIND_WRITE,
                        "plan-bytes-full".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .addSplits(org.pangrova.protocol.v2.DataSplit.newBuilder()
                        .setSplitId("split-a"))
                .addSplits(org.pangrova.protocol.v2.DataSplit.newBuilder()
                        .setSplitId("split-b"))
                .setResourceGrant(resourceGrant)
                .setExecutionOptions(options)
                .build();
        SubmitTaskRequest req = SubmitTaskRequest.from(canonical);

        assertThat(req.taskRequest()).isEqualTo(canonical);
        assertThat(canonical.getAttempt().getTaskId()).isEqualTo(taskId);
        assertThat(canonical.getAttempt().getQueryOrJobId()).isEqualTo(queryId);
        assertThat(canonical.getAttempt().getStageId()).isEqualTo(stageId);
        assertThat(canonical.getAttempt().getPartitionId()).isEqualTo(partitionId);
        assertThat(canonical.getTaskKind())
                .isEqualTo(org.pangrova.protocol.v2.TaskKind.TASK_KIND_WRITE);
        assertThat(canonical.getSubstraitPlan().toStringUtf8()).isEqualTo("plan-bytes-full");
        assertThat(canonical.getSplitsList()).extracting("splitId")
                .containsExactly("split-a", "split-b");
        assertThat(canonical.getResourceGrant()).isEqualTo(resourceGrant);
        assertThat(canonical.getExecutionOptions().getSessionPropertiesMap())
                .isEqualTo(sessionProps);
        assertThat(canonical.getExecutionOptions().getDirectResultOutput()).isTrue();
        assertThat(canonical.getExecutionOptions().getWrite().getTargetTable())
                .isEqualTo("catalog.db.table");
    }

    // ── Property 36: SubmitTaskResponse field completeness ──

    @Property(tries = 10)
    void submitTaskResponsePreservesAllFields(
            @ForAll("taskIds") String taskId,
            @ForAll boolean accepted) {

        String rejectReason = accepted ? null : "memory_exceeded";
        SubmitTaskResponse resp = new SubmitTaskResponse(
                accepted, rejectReason, taskId, null, null);

        assertThat(resp.accepted()).isEqualTo(accepted);
        assertThat(resp.rejectReason()).isEqualTo(rejectReason);
        assertThat(resp.taskId()).isEqualTo(taskId);
    }

    // ── Property 36: TaskStatusReport ALL fields completeness ──

    @Property(tries = 10)
    void taskStatusReportPreservesAllFields(
            @ForAll("taskIds") String taskId,
            @ForAll("queryIds") String queryId,
            @ForAll @IntRange(min = 0, max = 10) int stageId,
            @ForAll @IntRange(min = 0, max = 100) int partitionId,
            @ForAll("workerIds") String workerId) {

        long seq = SEQ.incrementAndGet();
        long ts = System.currentTimeMillis();
        ErrorDetail error = new ErrorDetail("EXEC_FAILED", "oom", null);

        TaskStatusReport report = new TaskStatusReport(
                taskId, queryId, stageId, partitionId, "COMPUTE",
                workerId, "FAILED", null, error, null, seq, ts,
                null, null, TaskAttemptDescriptor.of(1, 0));

        assertThat(report.taskId()).isEqualTo(taskId);
        assertThat(report.queryId()).isEqualTo(queryId);
        assertThat(report.stageId()).isEqualTo(stageId);
        assertThat(report.partitionId()).isEqualTo(partitionId);
        assertThat(report.taskType()).isEqualTo("COMPUTE");
        assertThat(report.workerId()).isEqualTo(workerId);
        assertThat(report.state()).isEqualTo("FAILED");
        assertThat(report.error()).isEqualTo(error);
        assertThat(report.reportSequence()).isEqualTo(seq);
        assertThat(report.timestampMs()).isEqualTo(ts);
    }

    // ── Property 36: TaskStatusReportResponse field completeness ──

    @Property(tries = 10)
    void taskStatusReportResponsePreservesAllFields(
            @ForAll("taskIds") String taskId) {

        TaskStatusReportResponse resp = new TaskStatusReportResponse(taskId, "CANCEL");

        assertThat(resp.taskId()).isEqualTo(taskId);
        assertThat(resp.command()).isEqualTo("CANCEL");
    }

    // ── Providers ──

    @Provide
    Arbitrary<String> taskIds() {
        return Arbitraries.strings().alpha().ofMinLength(2).ofMaxLength(6).map(s -> "t-" + s);
    }

    @Provide
    Arbitrary<String> queryIds() {
        return Arbitraries.strings().alpha().ofMinLength(2).ofMaxLength(6).map(s -> "q-" + s);
    }

    @Provide
    Arbitrary<String> workerIds() {
        return Arbitraries.strings().alpha().ofMinLength(2).ofMaxLength(6).map(s -> "w-" + s);
    }

    private static void registerReadRequest(CoordinatorTaskTracker tracker,
            String queryId, String taskId, TaskAttemptDescriptor attempt) {
        var builder = org.pangrova.coordinator.CanonicalTaskRequestTestFixture.builder(
                taskId, queryId, 0, 0,
                org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ, new byte[]{1});
        builder.setAttempt(builder.getAttempt().toBuilder()
                .setPlanEpoch(attempt.planEpoch())
                .setAttemptId(attempt.attemptId())
                .setFencingToken(attempt.fencingToken()));
        tracker.registerTaskRequest(taskId, SubmitTaskRequest.from(builder.build()));
    }

    private static TaskStatusReport report(String taskId, String queryId, String state, long seq) {
        return new TaskStatusReport(
                taskId, queryId, 0, 0, "COMPUTE", WORKER_ID,
                state, null, null, null, seq, System.currentTimeMillis(),
                null, null, TaskAttemptDescriptor.of(1, 0));
    }

    private static ExchangeSourceDescriptor exchangeSource(String taskId) {
        return new ExchangeSourceDescriptor(
                "exchange-input-ex-1", "ex-1", 1,
                taskId, 0, 0, 0,
                1L, 0, (1L << 32) + 1L,
                "h1", 9090, "", "signed-ticket",
                System.currentTimeMillis() + 600_000L,
                0L, 0L, "BLOCKING");
    }

    private static TaskStatusReport artifactReport(
            SubmitTaskRequest request,
            org.pangrova.protocol.v2.ArtifactWriteBinding target) {
        return artifactReport(request, target, 11L);
    }

    private static TaskStatusReport artifactReport(
            SubmitTaskRequest request,
            org.pangrova.protocol.v2.ArtifactWriteBinding target,
            long sequence) {
        var dispatchedAttempt = request.taskRequest().getAttempt();
        var object = org.pangrova.protocol.v2.ArtifactObject.newBuilder()
                .setObjectId("object-1")
                .setOpaqueUri(target.getStagingUri() + "/object-1")
                .setObjectRole("GRAPH")
                .setSourceObjectId("source-1")
                .setSizeBytes(64)
                .setRowCount(1)
                .setOrdinal(0)
                .setScope(org.pangrova.protocol.v2.ArtifactObjectScope
                        .ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT)
                .setChecksum(sha256Checksum(new byte[]{1}))
                .build();
        var unsigned = org.pangrova.protocol.v2.ArtifactWorkReport.newBuilder()
                .setJobId(dispatchedAttempt.getQueryOrJobId())
                .setPageId(dispatchedAttempt.getTaskId())
                .setFencingToken(dispatchedAttempt.getFencingToken())
                .setSourceId(target.getSourceId())
                .setSourceSnapshot(target.getSourceSnapshot())
                .setStagingGeneration(target.getGeneration())
                .addOutputs(object)
                .build();
        var work = unsigned.toBuilder()
                .setReportChecksum(sha256Checksum(unsigned.toByteArray()))
                .build();
        var nativeAttempt = org.pangrova.protocol.v2.TaskAttemptId.newBuilder()
                .mergeFrom(dispatchedAttempt)
                .build();
        var status = org.pangrova.protocol.v2.TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(nativeAttempt)
                .setState(org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED)
                .setCoverage(org.pangrova.protocol.v2.CoverageReport.newBuilder()
                        .setCompletionState(org.pangrova.protocol.v2.CompletionState
                                .COMPLETION_STATE_COMPLETE)
                        .setExpectedWorkUnits(1)
                        .setCompletedWorkUnits(1)
                        .setCoverageRatio(1.0))
                .setArtifactWork(work)
                .build();
        return new TaskStatusReport(
                dispatchedAttempt.getTaskId(), dispatchedAttempt.getQueryOrJobId(),
                dispatchedAttempt.getStageId(), dispatchedAttempt.getPartitionId(),
                "BACKGROUND_BUILD", WORKER_ID, "FINISHED",
                null, null, null, sequence, System.currentTimeMillis(), null, null,
                TaskAttemptDescriptor.from(dispatchedAttempt), status.toByteArray());
    }

    private static org.pangrova.protocol.v2.Checksum sha256Checksum(byte[] bytes) {
        try {
            return org.pangrova.protocol.v2.Checksum.newBuilder()
                    .setAlgorithm(org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256)
                    .setDigest(ByteString.copyFrom(
                            java.security.MessageDigest.getInstance("SHA-256").digest(bytes)))
                    .build();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("timed out waiting for test latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for test latch", e);
        }
    }

    private static QueryManager buildMinimalQueryManager(
            CoordinatorTaskTracker tracker, WorkerManager workerManager) {
        var dispatcher = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());
        var scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        var deps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null, dispatcher, scheduler,
                workerManager, tracker, null, null, null, 0L);
        var queryDispatcher = new QueryDispatcher(
                deps, null, null);
        return new QueryManager(
                QueryManagerContext.minimal(queryDispatcher, new NoOpTracingProvider()));
    }
}
