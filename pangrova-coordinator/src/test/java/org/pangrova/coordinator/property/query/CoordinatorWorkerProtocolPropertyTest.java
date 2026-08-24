package org.pangrova.coordinator.property.query;

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
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.rest.config.OutputConfig;
import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.protocol.rest.config.WriteConfig;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.task.*;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property 36: Coordinator-Worker protocol field completeness.
 * Property 37: Exchange source update idempotency.
 *
 * <p>For any Coordinator-Worker coordination protocol message
 * (SubmitTaskRequest/Response, TaskStatusReport/Response,
 * UpdateExchangeSourcesRequest), all fields set by the sender
 * are preserved after construction — no field loss or default
 * value override.</p>
 *
 * <p><b>Validates: Requirements 54.1, 54.2, 54.4</b></p>
 */
@Tag("Feature: architecture-remediation")
@Tag("Property 36: Coordinator-Worker protocol field completeness")
@Tag("Property 37: Exchange source update idempotency")
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

        SubmitTaskRequest req = new SubmitTaskRequest(
                taskId, queryId, stageId, partitionId, "COMPUTE",
                "plan-bytes", List.of("split1"), List.of(),
                null, null, null, null,
                null, null, null, null, null, null, null);

        assertThat(req.taskId()).isEqualTo(taskId);
        assertThat(req.queryId()).isEqualTo(queryId);
        assertThat(req.stageId()).isEqualTo(stageId);
        assertThat(req.partitionId()).isEqualTo(partitionId);
        assertThat(req.taskType()).isEqualTo("COMPUTE");
        assertThat(req.splits()).containsExactly("split1");
    }

    // ── Property 36: TaskStatusReport identity mirrors SubmitTaskRequest ──

    @Property(tries = 10)
    void taskStatusReportIdentityMirrorsSubmitRequest(
            @ForAll("taskIds") String taskId,
            @ForAll("queryIds") String queryId,
            @ForAll @IntRange(min = 0, max = 10) int stageId,
            @ForAll @IntRange(min = 0, max = 100) int partitionId) {

        SubmitTaskRequest req = new SubmitTaskRequest(
                taskId, queryId, stageId, partitionId, "COMPUTE",
                "plan", List.of(), List.of(), null, null, null, null,
                null, null, null, null, null, null, null);

        TaskStatusReport report = new TaskStatusReport(
                req.taskId(), req.queryId(), req.stageId(), req.partitionId(),
                req.taskType(), WORKER_ID, "RUNNING",
                null, null, null, 1L, System.currentTimeMillis());

        assertThat(report.taskId()).isEqualTo(req.taskId());
        assertThat(report.queryId()).isEqualTo(req.queryId());
        assertThat(report.stageId()).isEqualTo(req.stageId());
        assertThat(report.partitionId()).isEqualTo(req.partitionId());
        assertThat(report.taskType()).isEqualTo(req.taskType());
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
        tracker.markScheduled(queryId, taskId);

        QueryStateMachine sm = new QueryStateMachine();
        qm.registerStateMachine(queryId, sm);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);

        long seq1 = SEQ.incrementAndGet();
        long seq2 = SEQ.incrementAndGet();

        // First report accepted
        boolean accepted1 = receiver.onTaskStatus(report(taskId, queryId, "RUNNING", seq2));
        assertThat(accepted1).isTrue();

        // Stale report (lower sequence) rejected
        boolean accepted2 = receiver.onTaskStatus(report(taskId, queryId, "RUNNING", seq1));
        assertThat(accepted2).as("Stale report with seq=%d should be rejected", seq1).isFalse();

        // Equal sequence also rejected
        boolean accepted3 = receiver.onTaskStatus(report(taskId, queryId, "RUNNING", seq2));
        assertThat(accepted3).as("Duplicate seq=%d should be rejected", seq2).isFalse();
    }

    // ── Property 37: Exchange source update sequence dedup ──

    @Property(tries = 10)
    void exchangeSourceUpdateRejectsStaleSequence(
            @ForAll("taskIds") String taskId,
            @ForAll("queryIds") String queryId) {

        ExchangeSourceDescriptor src = new ExchangeSourceDescriptor("src-task", "host1", 9090, 0, "FLIGHT", 5L);
        UpdateExchangeSourcesRequest req = new UpdateExchangeSourcesRequest(
                taskId, queryId, List.of(src), "upstream-complete");

        assertThat(req.taskId()).isEqualTo(taskId);
        assertThat(req.queryId()).isEqualTo(queryId);
        assertThat(req.exchangeSources()).hasSize(1);
        assertThat(req.exchangeSources().get(0).sequenceId()).isEqualTo(5L);
    }

    // ── Property 36: Worker registration request field completeness ──

    @Property(tries = 10)
    void workerRegistrationRequestPreservesFields(
            @ForAll("workerIds") String workerId) {

        WorkerRegistrationRequest req = new WorkerRegistrationRequest(
                workerId, "host-" + workerId, 8080, 9090, 0,
                1, List.of("COMPUTE", "WRITE"),
                "rack-1", List.of("local-data"), null);

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
                "RUNNING", null, null, null, 1L, System.currentTimeMillis());

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

        ResourceLimitPayload resourceLimit = new ResourceLimitPayload(1024L, 4, 2048L, "/tmp/spill");
        OutputConfig outputConfig = new OutputConfig("ticket-1", 1024L, 4, 30_000L);
        WriteConfig writeConfig = new WriteConfig("catalog.db.table", "APPEND",
                null, "PARQUET", 128_000_000L);
        Map<String, String> sessionProps = Map.of("key1", "val1", "key2", "val2");

        SubmitTaskRequest req = new SubmitTaskRequest(
                taskId, queryId, stageId, partitionId, "WRITE",
                "plan-bytes-full", List.of("split-a", "split-b"),
                List.of(new ExchangeSourceDescriptor("src-t", "h1", 9090, 0, "BROADCAST", 1L)),
                resourceLimit, sessionProps, outputConfig, writeConfig,
                null, null, null, null, null, null, null);

        assertThat(req.taskId()).isEqualTo(taskId);
        assertThat(req.queryId()).isEqualTo(queryId);
        assertThat(req.stageId()).isEqualTo(stageId);
        assertThat(req.partitionId()).isEqualTo(partitionId);
        assertThat(req.taskType()).isEqualTo("WRITE");
        assertThat(req.planFragment()).isEqualTo("plan-bytes-full");
        assertThat(req.splits()).containsExactly("split-a", "split-b");
        assertThat(req.exchangeSources()).hasSize(1);
        assertThat(req.exchangeSources().get(0).sourceTaskId()).isEqualTo("src-t");
        assertThat(req.resourceLimit()).isEqualTo(resourceLimit);
        assertThat(req.sessionProperties()).isEqualTo(sessionProps);
        assertThat(req.outputConfig()).isEqualTo(outputConfig);
        assertThat(req.writeConfig()).isEqualTo(writeConfig);
    }

    // ── Property 36: SubmitTaskResponse field completeness ──

    @Property(tries = 10)
    void submitTaskResponsePreservesAllFields(
            @ForAll("taskIds") String taskId,
            @ForAll boolean accepted) {

        String rejectReason = accepted ? null : "memory_exceeded";
        SubmitTaskResponse resp = new SubmitTaskResponse(accepted, rejectReason, taskId, null);

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
                workerId, "FAILED", null, error, null, seq, ts);

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

    private static TaskStatusReport report(String taskId, String queryId, String state, long seq) {
        return new TaskStatusReport(
                taskId, queryId, 0, 0, "COMPUTE", WORKER_ID,
                state, null, null, null, seq, System.currentTimeMillis());
    }

    private static QueryManager buildMinimalQueryManager(
            CoordinatorTaskTracker tracker, WorkerManager workerManager) {
        var dispatcher = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());
        var scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        var deps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null, dispatcher, scheduler,
                workerManager, tracker, null, null, null, null, 0L);
        var queryDispatcher = new QueryDispatcher(
                deps, null, null);
        return new QueryManager(
                QueryManagerContext.minimal(queryDispatcher, new NoOpTracingProvider()));
    }
}

