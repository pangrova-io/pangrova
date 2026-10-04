package org.pangrova.coordinator.query.task;

import com.google.protobuf.CodedOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.coordinator.CanonicalTaskRequestTestFixture;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.DurableStageRuntime;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.TaskTerminalRegistry;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.query.stage.StageStateMachine;
import org.pangrova.coordinator.query.task.report.DurableArtifactWorkReportSink;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.CoordinatorArtifactAuthorityFixtures;
import org.pangrova.coordinator.testinfra.CoordinatorArtifactExecutionFixtures;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.metadata.artifact.build.ArtifactBuildDispatchSpec;
import org.pangrova.metadata.artifact.build.ArtifactBuildJob;
import org.pangrova.metadata.artifact.build.ArtifactBuildJobKey;
import org.pangrova.metadata.artifact.build.ArtifactBuildJobState;
import org.pangrova.metadata.artifact.build.ArtifactBuildLease;
import org.pangrova.metadata.artifact.build.ArtifactBuildPage;
import org.pangrova.metadata.artifact.build.ArtifactExchangeRegistration;
import org.pangrova.metadata.artifact.build.ArtifactExecutionOwner;
import org.pangrova.metadata.artifact.build.ArtifactStageGraph;
import org.pangrova.metadata.artifact.build.template.ArtifactStageTemplate;
import org.pangrova.metadata.artifact.contract.ArtifactBuildPageKind;
import org.pangrova.metadata.artifact.contract.ArtifactDependencySet;
import org.pangrova.metadata.artifact.contract.ArtifactEmptyPartitionPolicy;
import org.pangrova.metadata.artifact.contract.ArtifactExecutionKind;
import org.pangrova.metadata.artifact.contract.ArtifactTypeIds;
import org.pangrova.metadata.artifact.jdbc.JdbcArtifactStageExchangeStore;
import org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactBuildStore;
import org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactStageExecutionStore;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.AuthorizedStageGraph;
import org.pangrova.planner.SerializedPlanFragment;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.CompletionState;
import org.pangrova.protocol.v2.CoverageReport;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.security.authz.DeferredAuthorizationScope;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.coordination.ExecutionAnchor;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real PostgreSQL authority/page commits feeding the ordinary task-to-stage lifecycle. */
class TaskStatusReceiverDurableProducerIntegrationTest {
    private static final ArtifactBuildJobKey KEY = new ArtifactBuildJobKey("report-tenant", "report-job");
    private static final String TASK = "producer-task";
    private DataSource dataSource;
    private JdbcArtifactBuildStore builds;
    private JdbcArtifactStageExecutionStore stages;
    private CoordinatorArtifactExecutionFixtures executions;
    private ExecutionAnchor anchor;
    private ArtifactBuildLease lease;
    private ArtifactBuildDispatchSpec spec;
    private CoordinatorTaskTracker tracker;
    private QueryManager queries;
    private TaskStatusReceiver receiver;
    private Stage producer;
    private SubmitTaskRequest request;
    private final AtomicBoolean committedAtStageFinish = new AtomicBoolean();

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("durable_native_report");
        builds = new JdbcArtifactBuildStore(dataSource);
        stages = new JdbcArtifactStageExecutionStore(dataSource);
        executions = new CoordinatorArtifactExecutionFixtures(dataSource);
        spec = specification();
        CoordinatorArtifactAuthorityFixtures.provision(dataSource, KEY.tenantId(), spec.principal(), spec.authorizationScope());
        builds.createJob(job(), spec, List.of(
                new ArtifactBuildPage(KEY.jobId(), "producer-0", 1, 0, 0,
                        ArtifactBuildPageKind.RELATIONAL_COMPUTE, List.of(), List.of(), 3),
                new ArtifactBuildPage(KEY.jobId(), "terminal-0", 0, 0, 1,
                        ArtifactBuildPageKind.RELATIONAL_PARTITION, List.of(), List.of("producer-0"), 3)));
        anchor = executions.takeOwnership(KEY.tenantId(), KEY.jobId(), "report-coordinator", 8);
        registerExchange();
        lease = stages.preparePartition(claim(1, 0), value -> value.lease()).orElseThrow();
        tracker = new CoordinatorTaskTracker();
        var workers = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        workers.registerWorker(TestWorkerInfoBuilder.active("worker-1", "localhost", 8080, 8081));
        queries = queryManager(workers, tracker);
        var terminal = new TaskTerminalRegistry();
        producer = producerStage();
        producer.addTaskId(TASK);
        terminal.register(KEY.executionId(), TASK, producer);
        producer.stateMachine().addStateChangeListener((stageId, oldState, newState) -> {
            if (newState == StageStateMachine.State.FINISHED) {
                committedAtStageFinish.set(builds.findJob(KEY.tenantId(), KEY.jobId()).orElseThrow().completedPages() == 1);
            }
        });
        receiver = new TaskStatusReceiver(new TaskStatusReceiverContext(queries, tracker, workers,
                null, null, null, null, null, null, terminal::onTerminal, null, null,
                new DurableArtifactWorkReportSink(builds, stages)));
        request = request(7);
        registerTask(request);
    }

    @AfterEach
    void closeProcesses() {
        if (executions != null) executions.close();
    }

    @Test
    void originalNativeWireCommitsBeforeTheOrdinaryDagTerminalAndNeverCreatesAUserQuery() throws Exception {
        byte[] original = originalWire(request);
        assertThat(original).isNotEqualTo(TaskReport.parseFrom(original).toByteArray());
        assertThat(receiver.onTaskStatus(status(request, original))).isTrue();
        assertThat(committedAtStageFinish).isTrue();
        assertThat(tracker.getTaskState(KEY.executionId(), TASK))
                .isEqualTo(org.pangrova.coordinator.task.TaskStateMachine.State.FINISHED);
        assertThat(producer.stateMachine().getState()).isEqualTo(StageStateMachine.State.FINISHED);
        assertThat(queries.getQueryState(KEY.executionId())).isEmpty();
        assertThat(stages.preparePartition(claim(0, 1), value -> value.lease())).isEmpty();
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "SELECT report_bytes,report_sha256 FROM artifact_build_pages WHERE tenant_id=? AND job_id=? AND page_id=?")) {
            statement.setString(1, KEY.tenantId());
            statement.setString(2, KEY.jobId());
            statement.setString(3, lease.pageId());
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getBytes(1)).isEqualTo(original);
                assertThat(rows.getString(2)).isEqualTo(DigestUtils.sha256Hex(original));
            }
        }
    }

    @Test
    void aFormerOwnerCannotAdvanceTheOrdinaryDagOrThePersistedPage() throws Exception {
        executions.takeOwnership(KEY.tenantId(), KEY.jobId(), "replacement-coordinator", 9);
        assertThat(receiver.onTaskStatus(status(request, originalWire(request)))).isFalse();
        assertThat(producer.stateMachine().getState()).isEqualTo(StageStateMachine.State.RUNNING);
        assertThat(builds.findJob(KEY.tenantId(), KEY.jobId()).orElseThrow().completedPages()).isZero();
        assertThat(committedAtStageFinish).isFalse();
    }

    @Test
    void pendingCompletionCannotCommitAndIsRetryableAfterRealScheduling() throws Exception {
        tracker.unregisterUndispatchedTask(KEY.executionId(), TASK);
        tracker.registerTask(KEY.executionId(), TASK);
        tracker.registerTaskRequest(TASK, request);
        tracker.registerTaskAttempt(TASK, TaskAttemptDescriptor.from(request.taskRequest().getAttempt()));
        var finished = status(request, originalWire(request));
        assertThat(receiver.onTaskStatus(finished)).isFalse();
        assertThat(builds.findJob(KEY.tenantId(), KEY.jobId()).orElseThrow().completedPages()).isZero();
        assertThat(producer.stateMachine().getState()).isEqualTo(StageStateMachine.State.RUNNING);
        assertThat(tracker.getTaskState(KEY.executionId(), TASK))
                .isEqualTo(org.pangrova.coordinator.task.TaskStateMachine.State.PENDING);
        tracker.markScheduled(KEY.executionId(), TASK);
        assertThat(receiver.onTaskStatus(finished)).isTrue();
        assertThat(tracker.getTaskState(KEY.executionId(), TASK))
                .isEqualTo(org.pangrova.coordinator.task.TaskStateMachine.State.FINISHED);
    }

    @Test
    void duplicateTerminalNeverRecommitsOrReopensTheSharedStage() throws Exception {
        var finished = status(request, originalWire(request));
        assertThat(receiver.onTaskStatus(finished)).isTrue();
        var duplicate = new TaskStatusReport(TASK, KEY.executionId(), 1, 0, "COMPUTE", "worker-1", "FINISHED",
                null, null, null, 2, System.currentTimeMillis(), null, null,
                finished.attempt(), finished.canonicalTaskReport());
        assertThat(receiver.onTaskStatus(duplicate)).isFalse();
        assertThat(builds.findJob(KEY.tenantId(), KEY.jobId()).orElseThrow().completedPages()).isEqualTo(1);
        assertThat(producer.stateMachine().getState()).isEqualTo(StageStateMachine.State.FINISHED);
    }

    @Test
    void preparedIdentityWithoutRegisteredTaskCannotCommitAProducerWinner() throws Exception {
        tracker.unregisterUndispatchedTask(KEY.executionId(), TASK);
        tracker.registerTaskRequest(TASK, request);
        tracker.registerTaskAttempt(TASK, TaskAttemptDescriptor.from(request.taskRequest().getAttempt()));
        assertThat(receiver.onTaskStatus(status(request, originalWire(request)))).isFalse();
        assertThat(builds.findJob(KEY.tenantId(), KEY.jobId()).orElseThrow().completedPages()).isZero();
        assertThat(producer.stateMachine().getState()).isEqualTo(StageStateMachine.State.RUNNING);
        assertThat(committedAtStageFinish).isFalse();
    }

    @Test
    void restartLoadsTheOriginalWinnerWithoutRewritingItToTheNewAuthority() throws Exception {
        byte[] original = originalWire(request);
        assertThat(receiver.onTaskStatus(status(request, original))).isTrue();
        var replacement = executions.takeOwnership(KEY.tenantId(), KEY.jobId(), "replacement-coordinator", 9);
        var runtime = runtime(replacement);
        var progress = runtime.inspect(1, 0);
        assertThat(progress.winner().orElseThrow().originalWireBytes()).isEqualTo(original);
        var restored = runtime.winner(1, 0, progress);
        assertThat(restored.getAttempt()).isEqualTo(request.taskRequest().getAttempt());
        assertThat(restored.getAttempt().getExecutionOwner().getOwnerId()).isEqualTo(anchor.ownerId());
        assertThat(runtime.control().authority().executionAnchor().ownerId()).isEqualTo(replacement.ownerId());
        assertThat(runtime.control().planEpoch(1)).isEqualTo(7);
        assertThat(queries.getQueryState(KEY.executionId())).isEmpty();
    }

    @Test
    void corruptedWinnerCannotChangeTheOriginalAllocationEpochEvenWithAnUpdatedStorageSha() throws Exception {
        assertThat(receiver.onTaskStatus(status(request, originalWire(request)))).isTrue();
        var replacement = executions.takeOwnership(KEY.tenantId(), KEY.jobId(), "replacement-coordinator", 9);
        byte[] changed = originalWire(request(8));
        try (var connection = dataSource.getConnection(); var update = connection.prepareStatement(
                "UPDATE artifact_build_pages SET report_bytes=?,report_sha256=? WHERE tenant_id=? AND job_id=? "
                        + "AND page_id=?")) {
            update.setBytes(1, changed);
            update.setString(2, DigestUtils.sha256Hex(changed));
            update.setString(3, KEY.tenantId());
            update.setString(4, KEY.jobId());
            update.setString(5, lease.pageId());
            assertThat(update.executeUpdate()).isEqualTo(1);
        }
        var runtime = runtime(replacement);
        assertThatThrownBy(() -> runtime.winner(1, 0, runtime.inspect(1, 0)))
                .hasMessageContaining("original allocation epoch");
    }

    private DurableStageRuntime runtime(ExecutionAnchor current) {
        return new DurableStageRuntime(stages.prepareExecution(KEY, current), new DurableStageRuntime.Dependencies(
                stages, new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()),
                new TaskScheduler(new WorkerPerformanceScorer()), () -> 9, Duration.ofMinutes(2),
                (task, acknowledgement) -> { throw new AssertionError("Winner replay must not dispatch a Worker"); },
                new JdbcArtifactStageExchangeStore(dataSource)));
    }

    private void registerTask(SubmitTaskRequest submitted) {
        tracker.registerTask(KEY.executionId(), TASK);
        tracker.markScheduled(KEY.executionId(), TASK);
        tracker.registerTaskRequest(TASK, submitted);
        tracker.registerTaskAttempt(TASK, TaskAttemptDescriptor.from(submitted.taskRequest().getAttempt()));
    }

    private SubmitTaskRequest request(long epoch) {
        var builder = CanonicalTaskRequestTestFixture.builder(TASK, KEY.executionId(), 1, 0,
                TaskKind.TASK_KIND_SHUFFLE_WRITE, new byte[]{1});
        builder.getAttemptBuilder().setPlanEpoch(epoch).setAttemptId(lease.attemptId())
                .setFencingToken(lease.fencingToken()).setControlEpoch(lease.ownerEpoch())
                .setExecutionOwner(ExecutionOwnerIdentity.newBuilder().setOwnerId(anchor.ownerId())
                        .setOwnerIncarnation(anchor.ownerIncarnation()).setFencingToken(anchor.fencingToken()));
        builder.getSecurityBuilder().setTenantId(KEY.tenantId()).setPrincipal(spec.principal());
        return SubmitTaskRequest.from(builder.build());
    }

    private static byte[] originalWire(SubmitTaskRequest submitted) throws Exception {
        var coverage = CoverageReport.newBuilder().setExpectedWorkUnits(1).setCompletedWorkUnits(1)
                .setCoverageRatio(1).setCompletionState(CompletionState.COMPLETION_STATE_COMPLETE).build();
        var bytes = new ByteArrayOutputStream();
        var output = CodedOutputStream.newInstance(bytes);
        output.writeEnum(TaskReport.REPORT_ORIGIN_FIELD_NUMBER, TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE.getNumber());
        output.writeMessage(TaskReport.COVERAGE_FIELD_NUMBER, coverage);
        output.writeEnum(TaskReport.STATE_FIELD_NUMBER, TaskState.TASK_STATE_FINISHED.getNumber());
        output.writeMessage(TaskReport.ATTEMPT_FIELD_NUMBER, submitted.taskRequest().getAttempt());
        output.flush();
        return bytes.toByteArray();
    }

    private static TaskStatusReport status(SubmitTaskRequest submitted, byte[] bytes) {
        var attempt = submitted.taskRequest().getAttempt();
        return new TaskStatusReport(TASK, KEY.executionId(), 1, 0, "COMPUTE", "worker-1", "FINISHED",
                null, null, null, 1, System.currentTimeMillis(), null, null, TaskAttemptDescriptor.from(attempt), bytes);
    }

    private JdbcArtifactStageExecutionStore.PartitionClaim claim(int stage, int ordinal) {
        return new JdbcArtifactStageExecutionStore.PartitionClaim(KEY, anchor, stage, 0, ordinal,
                8, Duration.ofMinutes(1), Instant.now());
    }

    private Stage producerStage() {
        var serialized = new AuthorizedStageGraph.SerializedStage(1, 1, "COMPUTE",
                new SerializedPlanFragment(new byte[]{1}, List.of(), List.of()), List.of(), List.of("id"), List.of(), -1, -1);
        var stage = Stage.create(new StageNode(1, serialized, 1, "COMPUTE", List.of()), KEY.executionId());
        stage.stateMachine().tryTransitionTo(StageStateMachine.State.SCHEDULING);
        stage.stateMachine().tryTransitionTo(StageStateMachine.State.RUNNING);
        return stage;
    }

    private void registerExchange() {
        var address = new ArtifactExchangeRegistration.Address("shuffle.internal", 9080, "shuffle.internal");
        new JdbcArtifactStageExchangeStore(dataSource).register(KEY, ArtifactExecutionOwner.from(anchor),
                new ArtifactExchangeRegistration("exchange-1-0", KEY.executionId(), "allocation-1", 1, 7,
                        spec.stageGraph().sha256(), spec.stageGraph().requireStage(1).planSha256(), "41", 1,
                        "c".repeat(64), 1, List.of(new ArtifactExchangeRegistration.Endpoint(address, 0, 1)), address,
                        ArtifactExecutionOwner.from(anchor)));
    }

    private static ArtifactBuildDispatchSpec specification() {
        var graph = new ArtifactStageGraph(ArtifactStageGraph.FORMAT_VERSION, ArtifactStageGraph.Purpose.RELATIONAL_EXPORT,
                List.of(ArtifactStageGraph.StagePlan.of(1, 1, ArtifactBuildPageKind.RELATIONAL_COMPUTE,
                                new byte[]{1}, ArtifactStageTemplate.of(new byte[0], List.of(), List.of()),
                                List.of("lake.raw.items"), List.of("id"), false),
                        ArtifactStageGraph.StagePlan.of(0, 1, ArtifactBuildPageKind.RELATIONAL_PARTITION,
                                new byte[]{2}, ArtifactStageTemplate.of(new byte[]{3}, List.of(), List.of()),
                                List.of(), List.of("id"), true)),
                List.of(new ArtifactStageGraph.Exchange("exchange-1-0", 1, 0,
                        ArtifactStageGraph.Exchange.Distribution.SINGLETON, List.of(), 1, "BATCH_SERDE_ARROW_BATCH", true)), List.of());
        return ArtifactBuildDispatchSpec.of(KEY.jobId(), graph, KEY.tenantId(), "report-principal",
                DeferredAuthorizationScope.raw(new QualifiedTableName("lake", "raw", "items"), Set.of("id"),
                        new QualifiedTableName("lake", "features", "output")),
                new ArtifactBuildDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000));
    }

    private static ArtifactBuildJob job() {
        var now = Instant.now();
        return new ArtifactBuildJob(KEY.tenantId(), KEY.jobId(), "feature-view/output", ArtifactTypeIds.TABULAR_EXPORT,
                "generation", "lake.raw.items", "41", ArtifactExecutionKind.EXPORT,
                ArtifactDependencySet.sourceSnapshot("lake.raw.items", "41"), ArtifactEmptyPartitionPolicy.ALLOW_ZERO_ROWS,
                "s3://warehouse/report-job", "pangrova.tabular-export.arrow-stream", 1, 0, "a".repeat(64),
                "native", "", KEY.jobId(), 2, 0, ArtifactBuildJobState.QUEUED, 0, now, now, now);
    }

    private static QueryManager queryManager(WorkerManager workers, CoordinatorTaskTracker tracker) {
        var scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        var remote = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());
        var dispatcher = new QueryDispatcher(QueryDispatcherContext.withDefaults(new SqlCompiler(), null, null, null,
                remote, scheduler, workers, tracker, null, null, null, 0L), null, null);
        return new QueryManager(QueryManagerContext.minimal(dispatcher, new NoOpTracingProvider()));
    }
}
