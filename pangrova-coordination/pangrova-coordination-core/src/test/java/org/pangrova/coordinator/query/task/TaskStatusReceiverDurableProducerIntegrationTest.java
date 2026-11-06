package org.pangrova.coordinator.query.task;

import org.pangrova.coordinator.testinfra.QueryManagerTestContexts;

import org.pangrova.query.api.stage.StageState;

import org.pangrova.coordinator.testinfra.ModelAuthorityFixtures;

import org.pangrova.generation.core.submission.GenerationDispatchSpecFactory;

import org.pangrova.generation.codec.execution.GenerationStageGraphCodec;

import com.google.protobuf.CodedOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.coordinator.CanonicalTaskRequestTestFixture;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.StageCompletionCallback;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.DurableStageRuntime;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.TaskTerminalRegistry;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.query.task.report.DurableGenerationWorkReportSink;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.CoordinatorGenerationAuthorityFixtures;
import org.pangrova.coordinator.testinfra.CoordinatorGenerationExecutionFixtures;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.builtin.placement.LoadBalancedStrategy;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.identity.GenerationJobKey;
import org.pangrova.generation.api.job.GenerationJobState;
import org.pangrova.generation.api.page.GenerationLease;
import org.pangrova.generation.api.page.GenerationPage;
import org.pangrova.generation.api.exchange.GenerationExchangeRegistration;
import org.pangrova.generation.api.execution.GenerationExecutionOwner;
import org.pangrova.generation.api.graph.GenerationStageGraph;
import org.pangrova.generation.api.graph.template.GenerationStageTemplate;
import org.pangrova.generation.api.execution.GenerationPageKind;
import org.pangrova.generation.api.dependency.GenerationDependencySet;
import org.pangrova.generation.api.output.GenerationEmptyPartitionPolicy;
import org.pangrova.generation.api.execution.GenerationExecutionKind;
import org.pangrova.generation.api.output.GenerationOutputTypeIds;
import org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStore;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.core.analysis.AuthorizedStageGraph;
import org.pangrova.planner.api.fragment.SerializedPlanFragment;
import org.pangrova.planner.core.analysis.SqlCompiler;
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
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.query.api.ownership.ExecutionAnchor;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;

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
    private static final GenerationJobKey KEY = new GenerationJobKey("report-tenant", "report-job");
    private static final String TASK = "producer-task";
    private DataSource dataSource;
    private PostgresGenerationJobStore builds;
    private GenerationStageExecutionStore stages;
    private CoordinatorGenerationExecutionFixtures executions;
    private ExecutionAnchor anchor;
    private GenerationLease lease;
    private GenerationDispatchSpec spec;
    private CoordinatorTaskTracker tracker;
    private QueryManager queries;
    private TaskStatusReceiver receiver;
    private Stage producer;
    private SubmitTaskRequest request;
    private final AtomicBoolean committedAtStageFinish = new AtomicBoolean();

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("durable_native_report");
        builds = ModelAuthorityFixtures.artifacts().builds(dataSource);
        stages = ModelAuthorityFixtures.artifacts().stages(dataSource);
        executions = new CoordinatorGenerationExecutionFixtures(dataSource);
        spec = specification();
        CoordinatorGenerationAuthorityFixtures.provision(dataSource, KEY.tenantId(), spec.principal(), spec.authorizationScope());
        builds.createJob(job(), spec, List.of(
                new GenerationPage(KEY.jobId(), "producer-0", 1, 0, 0,
                        GenerationPageKind.RELATIONAL_COMPUTE, List.of(), List.of(), 3),
                new GenerationPage(KEY.jobId(), "terminal-0", 0, 0, 1,
                        GenerationPageKind.RELATIONAL_PARTITION, List.of(), List.of("producer-0"), 3)));
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
            if (newState == StageState.FINISHED) {
                committedAtStageFinish.set(builds.findJob(KEY.tenantId(), KEY.jobId()).orElseThrow().completedPages() == 1);
            }
        });
        StageCompletionCallback callback = new StageCompletionCallback() {
            @Override
            public void onTaskTerminal(String queryId, String taskId, String state,
                    org.pangrova.query.api.error.QueryFailure error) {
                terminal.onTerminal(queryId, taskId, state, error);
            }

            @Override
            public void onQueryTerminal(String queryId, java.util.function.Consumer<Boolean> listener) {
                listener.accept(true);
            }
        };
        receiver = new TaskStatusReceiver(new TaskStatusReceiverContext(queries, tracker, workers,
                null, null, null, null, null, null, callback, null, new org.pangrova.persistence.query.memory.transaction.InMemoryWriteCommitJournal(),
                new DurableGenerationWorkReportSink(builds, stages)));
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
        assertThat(producer.stateMachine().getState()).isEqualTo(StageState.FINISHED);
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
        assertThat(producer.stateMachine().getState()).isEqualTo(StageState.RUNNING);
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
        assertThat(producer.stateMachine().getState()).isEqualTo(StageState.RUNNING);
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
        assertThat(receiver.onTaskStatus(finished)).isTrue();
        var duplicate = new TaskStatusReport(TASK, KEY.executionId(), 1, 0, "COMPUTE", "worker-1", "FINISHED",
                null, null, null, 2, System.currentTimeMillis(), null, null,
                finished.attempt(), finished.canonicalTaskReport());
        assertThat(receiver.onTaskStatus(duplicate)).isFalse();
        assertThat(builds.findJob(KEY.tenantId(), KEY.jobId()).orElseThrow().completedPages()).isEqualTo(1);
        assertThat(producer.stateMachine().getState()).isEqualTo(StageState.FINISHED);
    }

    @Test
    void preparedIdentityWithoutRegisteredTaskCannotCommitAProducerWinner() throws Exception {
        tracker.unregisterUndispatchedTask(KEY.executionId(), TASK);
        tracker.registerTaskRequest(TASK, request);
        tracker.registerTaskAttempt(TASK, TaskAttemptDescriptor.from(request.taskRequest().getAttempt()));
        assertThat(receiver.onTaskStatus(status(request, originalWire(request)))).isFalse();
        assertThat(builds.findJob(KEY.tenantId(), KEY.jobId()).orElseThrow().completedPages()).isZero();
        assertThat(producer.stateMachine().getState()).isEqualTo(StageState.RUNNING);
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
                new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty()), () -> 9, Duration.ofMinutes(2),
                (task, acknowledgement) -> { throw new AssertionError("Winner replay must not dispatch a Worker"); },
                ModelAuthorityFixtures.artifacts().exchanges(dataSource), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory()));
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

    private GenerationStageExecutionStore.PartitionClaim claim(int stage, int ordinal) {
        return new GenerationStageExecutionStore.PartitionClaim(KEY, anchor, stage, 0, ordinal,
                8, Duration.ofMinutes(1), Instant.now());
    }

    private Stage producerStage() {
        var serialized = new AuthorizedStageGraph.SerializedStage(1, 1, "COMPUTE",
                new SerializedPlanFragment(new byte[]{1}, List.of(), List.of()), List.of(), List.of("id"), List.of(), List.of(), -1, -1);
        var stage = Stage.create(new StageNode(1, serialized, 1, "COMPUTE", List.of()), KEY.executionId());
        stage.stateMachine().tryTransitionTo(StageState.SCHEDULING);
        stage.stateMachine().tryTransitionTo(StageState.RUNNING);
        return stage;
    }

    private void registerExchange() {
        var address = new GenerationExchangeRegistration.Address("shuffle.internal", 9080, "shuffle.internal");
        ModelAuthorityFixtures.artifacts().exchanges(dataSource).register(KEY, GenerationExecutionOwner.from(anchor),
                new GenerationExchangeRegistration("exchange-1-0", KEY.executionId(), "allocation-1", 1, 7,
                        GenerationStageGraphCodec.sha256(spec.stageGraph()), spec.stageGraph().requireStage(1).planSha256(), "41", 1,
                        "c".repeat(64), 1, List.of(new GenerationExchangeRegistration.Endpoint(address, 0, 1)), address,
                        GenerationExecutionOwner.from(anchor)));
    }

    private static GenerationDispatchSpec specification() {
        var graph = new GenerationStageGraph(GenerationStageGraph.FORMAT_VERSION, GenerationStageGraph.Purpose.RELATIONAL_EXPORT,
                List.of(GenerationStageGraph.StagePlan.of(1, 1, GenerationPageKind.RELATIONAL_COMPUTE,
                                new byte[]{1}, GenerationStageTemplate.of(new byte[0], List.of(), List.of()),
                                List.of("lake.raw.items"), List.of("id"), false),
                        GenerationStageGraph.StagePlan.of(0, 1, GenerationPageKind.RELATIONAL_PARTITION,
                                new byte[]{2}, GenerationStageTemplate.of(new byte[]{3}, List.of(), List.of()),
                                List.of(), List.of("id"), true)),
                List.of(new GenerationStageGraph.Exchange("exchange-1-0", 1, 0,
                        GenerationStageGraph.Exchange.Distribution.SINGLETON, List.of(), 1, "BATCH_SERDE_ARROW_BATCH", true)), List.of());
        return GenerationDispatchSpecFactory.create(KEY.jobId(), graph, KEY.tenantId(), "report-principal",
                DeferredAuthorizationScope.raw(new QualifiedTableName("lake", "raw", "items"), Set.of("id"),
                        new QualifiedTableName("lake", "features", "output")),
                new GenerationDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000));
    }

    private static GenerationJob job() {
        var now = Instant.now();
        return new GenerationJob(KEY.tenantId(), KEY.jobId(), "feature-view/output", GenerationOutputTypeIds.TABULAR_EXPORT,
                "generation", "lake.raw.items", "41", GenerationExecutionKind.EXPORT,
                GenerationDependencySet.sourceSnapshot("lake.raw.items", "41"), GenerationEmptyPartitionPolicy.ALLOW_ZERO_ROWS,
                "s3://warehouse/report-job", "pangrova.tabular-export.arrow-stream", 1, 0, "a".repeat(64),
                "native", "", KEY.jobId(), 2, 0, GenerationJobState.QUEUED, 0, now, now, now);
    }

    private static QueryManager queryManager(WorkerManager workers, CoordinatorTaskTracker tracker) {
        var scheduler = new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty());
        var remote = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());
        var dispatcher = new QueryDispatcher(QueryDispatcherContext.withDefaults(new SqlCompiler(), null, null, null,
                remote, scheduler, workers, tracker, null, null, null, 0L), null, null);
        return new QueryManager(QueryManagerTestContexts.minimal(dispatcher, new NoOpTracingProvider()));
    }
}
