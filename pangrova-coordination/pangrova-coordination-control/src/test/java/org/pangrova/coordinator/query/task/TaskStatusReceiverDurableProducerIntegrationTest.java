package org.pangrova.coordinator.query.task;

import org.pangrova.generation.api.enums.GenerationExchangeDistribution;

import org.pangrova.generation.api.enums.GenerationGraphPurpose;
import org.pangrova.shuffle.api.descriptor.ShuffleProviderDescriptor;
import org.pangrova.coordinator.testinfra.QueryManagerTestContexts;
import org.pangrova.query.api.enums.StageState;
import org.pangrova.coordinator.testinfra.ModelAuthorityFixtures;
import org.pangrova.generation.submission.GenerationDispatchSpecFactory;


import com.google.protobuf.CodedOutputStream;
import io.substrait.proto.ExchangeRel;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.coordinator.CanonicalTaskRequestTestFixture;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.query.api.stage.StageCompletionCallback;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.DurablePartitionRequest;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.DurableStageRuntime;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.TaskTerminalRegistry;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.query.task.report.DurableGenerationWorkReportSink;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;
import org.pangrova.coordinator.testinfra.CoordinatorGenerationExecutionFixtures;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.strategies.placement.LoadBalancedStrategy;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.identity.GenerationJobKey;
import org.pangrova.generation.api.enums.GenerationJobState;
import org.pangrova.generation.api.page.GenerationLease;
import org.pangrova.generation.api.page.GenerationPage;
import org.pangrova.coordinator.query.execution.dispatch.shuffle.generation.DurableStageShuffleEvidence;
import org.pangrova.protocol.shuffle.codec.ShuffleProducerSetChecksum;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.shuffle.api.ShuffleRegistration;
import org.pangrova.shuffle.api.coordinator.ShuffleLifecycleEvidence.Allocation;
import org.pangrova.generation.api.exception.GenerationExecutionOwnerLostException;
import org.pangrova.generation.api.execution.GenerationExecutionOwner;
import org.pangrova.generation.api.graph.GenerationStageGraph;
import org.pangrova.generation.api.graph.template.GenerationStageTemplate;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.generation.api.dependency.GenerationDependencySet;
import org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy;
import org.pangrova.generation.api.enums.GenerationExecutionKind;
import org.pangrova.generation.api.constants.GenerationOutputTypeIds;
import org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStore;
import org.pangrova.persistence.generation.postgresql.query.PostgresGenerationQueries;
import org.pangrova.generation.api.page.GenerationSplit;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.analysis.AuthorizedStageGraph;
import org.pangrova.planner.api.fragment.SerializedPlanFragment;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.protocol.rest.config.ShuffleConfig;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.substrait.SubstraitProtocolVersion;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.shuffle.api.ShuffleAccess;
import org.pangrova.shuffle.api.ShuffleHandle;
import org.pangrova.shuffle.api.ShuffleServerEndpoint;
import org.pangrova.shuffle.api.ShuffleServiceAddress;
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
import org.pangrova.execution.api.ownership.ExecutionAnchor;
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
        GenerationAuthorityFixtures.provision(dataSource, KEY.tenantId(), spec.principal(), spec.authorizationScope());
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
        var terminal = new TaskTerminalRegistry(KEY.executionId());
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

            @Override public boolean ownsQueryCompletion(String queryId) { return true; }

            @Override
            public boolean isQueryComplete(String queryId) {
                return producer.stateMachine().getState() == StageState.FINISHED;
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
        installStatementCounters();
        long before = executionRouteReads();
        var exchangeBefore = exchangeReads();
        String exactRegistration = "WITH exact_registration_edge AS MATERIALIZED%";
        long exactRegistrationBefore = statementReads(exactRegistration);
        String jobLock = "SELECT * FROM artifact_build_jobs WHERE tenant_id=%FOR UPDATE";
        String specs = "SELECT * FROM artifact_build_dispatch_specs%";
        String roles = "SELECT assignment.role_name FROM pangrova_user_roles assignment%";
        long lockedJobsBefore = statementReads(jobLock);
        long specsBefore = statementReads(specs);
        long rolesBefore = statementReads(roles);
        var delivery = org.pangrova.protocol.http.TaskStatusDelivery.newBuilder()
                .setRole(org.pangrova.protocol.http.TaskRole.TASK_ROLE_COMPUTE)
                .setWorkerId("worker-1").setSequence(1).setTimestampMs(System.currentTimeMillis())
                .setTerminalReport(com.google.protobuf.ByteString.copyFrom(original)).build();
        var received = org.pangrova.protocol.rest.task.TaskStatusWire.decode(delivery.toByteArray());
        assertThat(received.terminalEvidence().bytes()).isEqualTo(delivery.getTerminalReport());
        assertThat(receiver.onTaskStatus(received)).isTrue();
        assertThat(executionRouteReads() - before).as("durable execution route SELECTs").isEqualTo(3);
        assertThat(exchangeReads().since(exchangeBefore)).as("batch exchange evidence SELECTs")
                .isEqualTo(new ExchangeReads(0, 0, 0, 0, 0));
        assertThat(statementReads(exactRegistration) - exactRegistrationBefore)
                .as("producer allocation reads").isOne();
        assertThat(statementReads(jobLock) - lockedJobsBefore).as("locked job reads").isOne();
        assertThat(statementReads(specs) - specsBefore).as("dispatch contract reads").isOne();
        assertThat(statementReads(roles) - rolesBefore).as("current direct role reads").isOne();
        assertThat(committedAtStageFinish).isTrue();
        assertThat(tracker.getTaskState(KEY.executionId(), TASK))
                .isEqualTo(org.pangrova.coordinator.enums.ScheduledTaskState.FINISHED);
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
    void runningDurableProducerDoesNotNeedAnExecutionRouteRead() throws Exception {
        installStatementCounters();
        var attempt = request.taskRequest().getAttempt();
        byte[] bytes = TaskReport.newBuilder().setAttempt(attempt).setState(TaskState.TASK_STATE_RUNNING)
                .setCoverage(CoverageReport.newBuilder().setExpectedWorkUnits(1))
                .setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE).build().toByteArray();
        var report = new TaskStatusReport(TASK, KEY.executionId(), 1, 0, "COMPUTE", "worker-1", "RUNNING",
                null, null, null, 1, System.currentTimeMillis(), null, null, TaskAttemptDescriptor.from(attempt), bytes);
        long before = executionRouteReads();
        assertThat(receiver.onTaskStatus(report)).isTrue();
        assertThat(executionRouteReads() - before).as("durable execution route SELECTs").isZero();
        assertThat(tracker.getTaskState(KEY.executionId(), TASK))
                .isEqualTo(org.pangrova.coordinator.enums.ScheduledTaskState.RUNNING);
    }

    @Test
    void inspectsExactStageCohortWithoutPerPartitionTransactions() throws Exception {
        var key = new GenerationJobKey(KEY.tenantId(), "progress-windows");
        int partitions = 20;
        var owner = createWideJob(key, partitions);
        installStatementCounters();
        String pageLookup = "SELECT * FROM artifact_build_pages%stage_id=%partition_id%FOR UPDATE";
        long beforePage = statementReads(pageLookup);
        long beforeExchange = exchangeReads().combined();
        long beforeAuthority = executionRouteReads();
        String publicationLock = "SELECT pg_advisory_xact_lock_shared(%";
        long beforePublication = statementReads(publicationLock);
        String roleLookup = "SELECT assignment.role_name FROM pangrova_user_roles assignment%";
        long beforeRoles = statementReads(roleLookup);
        var progress = new java.util.ArrayList<GenerationStageExecutionStore.PartitionProgress>();
        int window = GenerationStageExecutionStore.PARTITION_INSPECTION_WINDOW;
        for (int first = 0; first < partitions; first += window) {
            progress.addAll(stages.inspectPartitions(key, owner, 1, first, Math.min(window, partitions - first)));
        }
        assertThat(progress).extracting(GenerationStageExecutionStore.PartitionProgress::pageOrdinal)
                .containsExactlyElementsOf(java.util.stream.IntStream.range(0, partitions).boxed().toList());
        assertThat(progress).allSatisfy(page -> {
            assertThat(page.kind()).isEqualTo(GenerationPageKind.RELATIONAL_COMPUTE);
            assertThat(page.winner()).isEmpty();
        });
        assertThat(statementReads(pageLookup) - beforePage).as("exact page reads").isEqualTo(3);
        assertThat(exchangeReads().combined() - beforeExchange).as("barrier evidence reads").isZero();
        assertThat(executionRouteReads() - beforeAuthority).as("execution route reads").isEqualTo(3);
        assertThat(statementReads(publicationLock) - beforePublication)
                .as("publication lock requests across three windows").isEqualTo(6);
        assertThat(statementReads(roleLookup) - beforeRoles)
                .as("direct policy role reads across three windows").isEqualTo(3);
        assertThatThrownBy(() -> stages.inspectPartitions(key, owner, 1, 0, window + 1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("inspection window");
        assertThatThrownBy(() -> stages.inspectPartitions(key, owner, 1, partitions - 1, 2))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("immutable stage cohort");
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "DELETE FROM artifact_build_pages WHERE tenant_id=? AND job_id=? AND stage_id=1 AND partition_id=6")) {
            statement.setString(1, key.tenantId());
            statement.setString(2, key.jobId());
            assertThat(statement.executeUpdate()).isOne();
        }
        assertThatThrownBy(() -> stages.inspectPartitions(key, owner, 1, 0, window))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("exact stage partitions");
    }

    @Test
    void claimsEightPartitionsWithOneAuthorityBarrierAndExactPageRead() throws Exception {
        int window = GenerationStageExecutionStore.PARTITION_INSPECTION_WINDOW;
        var key = new GenerationJobKey(KEY.tenantId(), "batched-page-claims");
        var owner = createWideJob(key, window * 2);
        installStatementCounters();

        var selectionSql = "WITH requested(partition_id, page_ordinal) AS (%artifact_build_pages%";
        var startedSql = "UPDATE artifact_build_jobs SET state=$% WHERE tenant_id=%AND state=$%";
        var splitSql = "SELECT page_id,source_object_id,split_bytes,split_sha256%FROM artifact_build_page_sources%";
        var dependenciesSql = "SELECT page_id,dependency_page_id%FROM artifact_build_page_dependencies%";
        var former = partitionClaims(key, owner, 0, window);
        long exchangeBefore = exchangeReads().combined();
        long authorityBefore = executionRouteReads();
        long selectionBefore = statementReads(selectionSql);
        long startedBefore = statementReads(startedSql);
        long splitBefore = statementReads(splitSql);
        long dependenciesBefore = statementReads(dependenciesSql);
        for (var claim : former) stages.preparePartition(claim, prepared -> prepared.lease()).orElseThrow();
        assertThat(exchangeReads().combined() - exchangeBefore).isEqualTo(window);
        assertThat(executionRouteReads() - authorityBefore).isEqualTo(window);
        assertThat(statementReads(selectionSql) - selectionBefore).isEqualTo(window);
        assertThat(statementReads(startedSql) - startedBefore).as("initial job transition").isOne();
        assertThat(statementReads(splitSql) - splitBefore).isEqualTo(window);
        assertThat(statementReads(dependenciesSql) - dependenciesBefore).isEqualTo(window);

        var claims = partitionClaims(key, owner, window, window);
        exchangeBefore = exchangeReads().combined();
        authorityBefore = executionRouteReads();
        selectionBefore = statementReads(selectionSql);
        startedBefore = statementReads(startedSql);
        splitBefore = statementReads(splitSql);
        dependenciesBefore = statementReads(dependenciesSql);
        var leased = stages.preparePartitions(claims, prepared -> prepared.lease());
        assertThat(leased).extracting(GenerationLease::partitionId)
                .containsExactlyElementsOf(claims.stream().map(GenerationStageExecutionStore.PartitionClaim::partitionId).toList());
        assertThat(leased).allSatisfy(lease -> {
            assertThat(lease.attemptId()).isZero();
            assertThat(lease.executionAnchor()).isEqualTo(owner);
        });
        assertThat(exchangeReads().combined() - exchangeBefore).as("locked exchange evidence reads").isOne();
        assertThat(executionRouteReads() - authorityBefore).as("execution authority reads").isOne();
        assertThat(statementReads(selectionSql) - selectionBefore).as("exact page cohort reads").isOne();
        assertThat(statementReads(startedSql) - startedBefore).as("updates for an already running job").isZero();
        assertThat(statementReads(splitSql) - splitBefore).as("page source reads").isOne();
        assertThat(statementReads(dependenciesSql) - dependenciesBefore).as("page dependency reads").isOne();

        try (var connection = dataSource.getConnection()) {
            var inputs = new org.pangrova.persistence.generation.postgresql.query.PostgresGenerationQueries()
                    .loadPageInputs(connection, key.tenantId(), key.jobId(), List.of("terminal-0", "producer-0"));
            assertThat(inputs.get(0).dependencies()).containsExactlyElementsOf(
                    java.util.stream.IntStream.range(0, window * 2).mapToObj(i -> "producer-" + i).toList());
            assertThat(inputs.get(1).dependencies()).isEmpty();
        }
    }

    @Test
    void durablePlacementAccountsForFrozenScanBytesBeforeIssuingAWholeWindow() throws Exception {
        int window = GenerationStageExecutionStore.PARTITION_INSPECTION_WINDOW;
        int partitions = window + 2;
        var key = new GenerationJobKey(KEY.tenantId(), "weighted-durable-placement");
        var placementSpec = specification(key, partitions, placementProducerPlan());
        var pages = new java.util.ArrayList<GenerationPage>();
        for (int partition = 0; partition < partitions; partition++) {
            var split = placementSplit(partition, (partition == 0 ? 128L : 1L) * 1024 * 1024);
            pages.add(new GenerationPage(key.jobId(), "producer-" + partition, 1, partition, partition,
                    GenerationPageKind.RELATIONAL_COMPUTE, List.of(split), List.of(), 3));
        }
        pages.add(new GenerationPage(key.jobId(), "terminal-0", 0, 0, partitions,
                GenerationPageKind.RELATIONAL_PARTITION, List.of(),
                pages.stream().map(GenerationPage::pageId).toList(), 3));
        builds.createJob(job(key, pages.size()), placementSpec, pages);
        var owner = executions.takeOwnership(key.tenantId(), key.jobId(), "report-coordinator", 8);
        var access = placementExchange(key, placementSpec, owner, partitions);

        var workers = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        var preferred = TestWorkerInfoBuilder.active("placement-preferred", "localhost", 8080, 8081);
        var alternate = TestWorkerInfoBuilder.withMetrics("placement-alternate", "localhost", 8082, 8083,
                0.25, 0, 1024L * 1024 * 1024, 1);
        workers.registerWorker(preferred);
        workers.registerWorker(alternate);
        workers.registerWorker(TestWorkerInfoBuilder.withMetrics("placement-too-small", "localhost", 8084, 8085,
                0, 0, 1, 1));
        var runtime = runtime(key, owner, workers);
        var plan = new DurablePartitionRequest.StageRequestPlan(placementSpec.stageGraph().requireStage(1), List.of());
        var tasks = java.util.stream.IntStream.range(0, window)
                .mapToObj(partition -> new DurableStageRuntime.PartitionTask(partition, partition, access)).toList();
        installStatementCounters();
        long before = executionRouteReads();

        var bound = runtime.bindStageTasks(1, plan);
        var prepared = bound.prepare(tasks, Instant.now());

        assertThat(executionRouteReads() - before).as("authority reads for the whole cohort").isOne();
        assertThat(prepared).extracting(value -> value.lease().partitionId())
                .containsExactlyElementsOf(java.util.stream.IntStream.range(0, window).boxed().toList());
        var workersByPartition = new java.util.ArrayList<String>();
        workersByPartition.add(preferred.workerId());
        workersByPartition.addAll(java.util.Collections.nCopies(window - 1, alternate.workerId()));
        assertThat(prepared).extracting(value -> value.worker().workerId())
                .containsExactlyElementsOf(workersByPartition);

        var first = prepared.get(0);
        tracker.prepareTaskDispatch(first.request(), first.worker());
        try {
            assertThat(tracker.placementLoad(preferred)).isOne();
            assertThat(bound.prepare(List.of(new DurableStageRuntime.PartitionTask(window, window, access)),
                    Instant.now())).singleElement().satisfies(task -> assertThat(task.worker()).isEqualTo(alternate));
        } finally {
            tracker.unregisterUndispatchedTask(key.executionId(), first.request().taskRequest().getAttempt().getTaskId());
        }
        assertThat(tracker.placementLoad(preferred)).isZero();
        assertThat(bound.prepare(List.of(new DurableStageRuntime.PartitionTask(window + 1, window + 1, access)),
                Instant.now())).singleElement().satisfies(task -> assertThat(task.worker()).isEqualTo(preferred));
    }

    private static GenerationSplit placementSplit(int partition, long bytes) {
        String object = "object-" + partition;
        var file = FileSplitDetail.newBuilder().setFileFormat(FileFormat.FILE_FORMAT_PARQUET).build();
        var split = DataSplitProtoCodec.buildFileSplit(new DataSplitProtoCodec.FileSplitInput(
                "iceberg", "lake.raw.items", "41", object, "s3://placement/" + object + ".parquet",
                0, bytes, java.util.Map.of(), file));
        return GenerationSplit.of(object, split.toByteArray());
    }

    private DurablePartitionRequest.ExchangeAccess placementExchange(GenerationJobKey key,
            GenerationDispatchSpec placementSpec, ExecutionAnchor owner, int partitions) {
        long epoch = 7;
        var checksum = ShuffleProducerSetChecksum.forContiguousPartitions(key.executionId(), 1, epoch, partitions);
        var registration = new ShuffleRegistration(key.executionId(), 1, 1, 0, false, epoch, partitions, checksum);
        var evidence = new DurableStageShuffleEvidence(ModelAuthorityFixtures.artifacts().exchanges(dataSource),
                placementSpec, "41", () -> GenerationExecutionOwner.from(owner));
        evidence.registered(registration, allocation(), shuffleOwner(owner));
        var route = new ShuffleServiceAddress("shuffle", 9443, "shuffle");
        var endpoint = new ShuffleServerEndpoint("shuffle", 9443, 0, 1, "shuffle");
        var grant = new ShuffleAccess(new byte[] {1}, System.currentTimeMillis() + 60_000L);
        var handle = new ShuffleHandle(allocation().shuffleId(), List.of(endpoint), route,
                grant, grant, grant, grant, partitions, checksum);
        var config = new ShuffleConfig("ARROW_BATCH", "none", 10_000, "MEMORY_ONLY", 1, List.of());
        return new DurablePartitionRequest.ExchangeAccess(epoch, List.of(), config, handle,
                (id, address, endpoints, count) -> allocation().descriptor());
    }

    private static byte[] placementProducerPlan() {
        var schema = NamedStruct.newBuilder().addNames("id").setStruct(Type.Struct.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)
                .addTypes(Type.newBuilder().setI64(Type.I64.newBuilder()
                        .setNullability(Type.Nullability.NULLABILITY_NULLABLE))));
        var source = Rel.newBuilder().setRead(ReadRel.newBuilder().setBaseSchema(schema)
                .setNamedTable(ReadRel.NamedTable.newBuilder().addAllNames(List.of("lake", "raw", "items"))));
        var exchange = Rel.newBuilder().setExchange(ExchangeRel.newBuilder().setInput(source)
                .setPartitionCount(1).setRoundRobin(ExchangeRel.RoundRobin.newBuilder().setExact(true)));
        var plan = Plan.newBuilder().addRelations(PlanRel.newBuilder().setRoot(RelRoot.newBuilder()
                .setInput(exchange).addNames("id")));
        SubstraitProtocolVersion.stamp(plan);
        return plan.build().toByteArray();
    }

    @Test
    void idempotentAdmissionReadsEachPageInputTableOnceAndKeepsExactOrdinals() throws Exception {
        var key = new GenerationJobKey(KEY.tenantId(), "admission-replay");
        int partitions = 8;
        var dispatch = specification(key, partitions);
        var pages = new java.util.ArrayList<GenerationPage>();
        var dependencyOrder = new java.util.ArrayList<String>();
        for (int partition = 0; partition < partitions; partition++) {
            String pageId = "producer-" + (partitions - partition);
            dependencyOrder.add(0, pageId);
            pages.add(new GenerationPage(key.jobId(), pageId, 1, partition, partition,
                    GenerationPageKind.RELATIONAL_COMPUTE,
                    List.of(GenerationSplit.of("source-" + partition, new byte[]{(byte) partition, 1})),
                    List.of(), 3));
        }
        var terminal = new GenerationPage(key.jobId(), "terminal-0", 0, 0, partitions,
                GenerationPageKind.RELATIONAL_PARTITION, List.of(), dependencyOrder, 3);
        pages.add(terminal);
        var job = job(key, pages.size());
        builds.createJob(job, dispatch, pages);
        installStatementCounters();

        String pageSql = "SELECT%FROM artifact_build_pages WHERE tenant_id=%ORDER BY page_ordinal%";
        String splitSql = "SELECT%FROM artifact_build_page_sources%";
        String dependencySql = "SELECT%FROM artifact_build_page_dependencies%";
        long pageBefore = statementReads(pageSql);
        long splitBefore = statementReads(splitSql);
        long dependencyBefore = statementReads(dependencySql);

        builds.createJob(job, dispatch, pages);

        assertThat(statementReads(pageSql) - pageBefore).isOne();
        assertThat(statementReads(splitSql) - splitBefore).isOne();
        assertThat(statementReads(dependencySql) - dependencyBefore).isOne();
        try (var connection = dataSource.getConnection()) {
            assertThat(new PostgresGenerationQueries().loadPages(connection, key.tenantId(), key.jobId()))
                    .containsExactlyElementsOf(pages);
        }

        java.util.Collections.reverse(dependencyOrder);
        pages.set(partitions, new GenerationPage(key.jobId(), terminal.pageId(), 0, 0, partitions,
                terminal.kind(), List.of(), dependencyOrder, terminal.maxAttempts()));
        assertThatThrownBy(() -> builds.createJob(job, dispatch, pages))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("different work pages");
    }

    @Test
    void aBlockedOrFailedCohortDoesNotConsumeAnyNewAttempt() {
        var key = new GenerationJobKey(KEY.tenantId(), "atomic-page-claims");
        var owner = createWideJob(key, 4);
        var claims = partitionClaims(key, owner, 0, 4);
        stages.preparePartition(claims.get(3), prepared -> prepared.lease()).orElseThrow();

        assertThat(stages.preparePartitions(claims, prepared -> {
            throw new AssertionError("A partly available cohort must not prepare a request");
        })).isEmpty();
        var failure = new IllegalStateException("request preparation failed");
        assertThatThrownBy(() -> stages.preparePartitions(claims.subList(0, 3), prepared -> {
            if (prepared.lease().partitionId() == 2) throw failure;
            return prepared.lease();
        })).isSameAs(failure);

        assertThat(stages.preparePartitions(claims.subList(0, 3), prepared -> prepared.lease()))
                .hasSize(3).allSatisfy(lease -> assertThat(lease.attemptId()).isZero());
    }

    private ExecutionAnchor createWideJob(GenerationJobKey key, int partitions) {
        var wideSpec = specification(key, partitions);
        var pages = new java.util.ArrayList<GenerationPage>();
        var upstream = new java.util.ArrayList<String>();
        for (int partition = 0; partition < partitions; partition++) {
            String pageId = "producer-" + partition;
            upstream.add(pageId);
            pages.add(new GenerationPage(key.jobId(), pageId, 1, partition, partition,
                    GenerationPageKind.RELATIONAL_COMPUTE, List.of(), List.of(), 3));
        }
        pages.add(new GenerationPage(key.jobId(), "terminal-0", 0, 0, partitions,
                GenerationPageKind.RELATIONAL_PARTITION, List.of(), upstream, 3));
        builds.createJob(job(key, pages.size()), wideSpec, pages);
        return executions.takeOwnership(key.tenantId(), key.jobId(), "report-coordinator", 8);
    }

    private static List<GenerationStageExecutionStore.PartitionClaim> partitionClaims(
            GenerationJobKey key, ExecutionAnchor owner, int first, int count) {
        Instant now = Instant.now();
        return java.util.stream.IntStream.range(first, first + count)
                .mapToObj(partition -> new GenerationStageExecutionStore.PartitionClaim(
                        key, owner, 1, partition, partition, 8, Duration.ofMinutes(1), now)).toList();
    }

    private void installStatementCounters() throws java.sql.SQLException {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public");
        }
    }

    private long executionRouteReads() throws java.sql.SQLException {
        return statementReads("SELECT tenant_id,job_id FROM artifact_execution_routes WHERE execution_id=%");
    }

    private ExchangeReads exchangeReads() throws java.sql.SQLException {
        return new ExchangeReads(
                statementReads("SELECT exchange_id,producer_stage_id,consumer_stage_id,%FROM artifact_stage_exchanges WHERE tenant_id=%"),
                statementReads("SELECT registration_bytes,registration_sha256 FROM artifact_exchange_registrations WHERE tenant_id=%"),
                statementReads("SELECT finalization_bytes,finalization_sha256 FROM artifact_exchange_finalizations WHERE tenant_id=%"),
                statementReads("SELECT source_snapshot FROM artifact_build_jobs WHERE tenant_id=%"),
                statementReads("WITH locked_edges AS MATERIALIZED%"));
    }

    private record ExchangeReads(long graph, long registration, long finalization, long snapshot, long combined) {
        ExchangeReads since(ExchangeReads before) {
            return new ExchangeReads(graph - before.graph, registration - before.registration,
                    finalization - before.finalization, snapshot - before.snapshot, combined - before.combined);
        }
    }

    private long statementReads(String pattern) throws java.sql.SQLException {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement("""
                SELECT COALESCE(SUM(calls), 0) FROM public.pg_stat_statements
                WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                  AND query LIKE ?
                """)) {
            statement.setString(1, pattern);
            try (var rows = statement.executeQuery()) {
                rows.next();
                return rows.getLong(1);
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
                .isEqualTo(org.pangrova.coordinator.enums.ScheduledTaskState.PENDING);
        tracker.markScheduled(KEY.executionId(), TASK);
        assertThat(receiver.onTaskStatus(finished)).isTrue();
        assertThat(tracker.getTaskState(KEY.executionId(), TASK))
                .isEqualTo(org.pangrova.coordinator.enums.ScheduledTaskState.FINISHED);
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
        var progress = runtime.inspect(1, 0, 1).get(0);
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
        assertThatThrownBy(() -> runtime.winner(1, 0, runtime.inspect(1, 0, 1).get(0)))
                .hasMessageContaining("original allocation epoch");
    }

    private DurableStageRuntime runtime(ExecutionAnchor current) {
        return runtime(KEY, current, new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()));
    }

    private DurableStageRuntime runtime(GenerationJobKey key, ExecutionAnchor current, WorkerManager workers) {
        return new DurableStageRuntime(stages.prepareExecution(key, current), new DurableStageRuntime.Dependencies(
                stages, workers,
                new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty()), () -> 9, Duration.ofMinutes(2),
                (task, acknowledgement) -> { throw new AssertionError("Winner replay must not dispatch a Worker"); },
                ModelAuthorityFixtures.artifacts().exchanges(dataSource),
                new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory(), tracker::placementLoad));
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
        journal(anchor).registered(shuffleRequest(7), allocation(), shuffleOwner(anchor));
    }

    @Test
    void shuffleJournalRequiresExactStageAndCurrentAuthorityBeforeRecovery() throws Exception {
        var original = journal(anchor);
        var registered = shuffleRequest(7);
        var replacement = executions.takeOwnership(KEY.tenantId(), KEY.jobId(), "replacement", 9);
        installStatementCounters();
        long allEdgesBefore = exchangeReads().combined();
        String exactRegistration = "WITH exact_registration_edge AS MATERIALIZED%";
        long exactRegistrationBefore = statementReads(exactRegistration);
        assertThatThrownBy(() -> original.restoredAllocation(registered))
                .isInstanceOf(GenerationExecutionOwnerLostException.class);
        var recovered = journal(replacement);
        assertThat(recovered.restoredAllocation(registered)).contains(allocation());
        assertThatThrownBy(() -> recovered.restoredAllocation(shuffleRequest(8)))
                .hasMessageContaining("requested stage cohort");
        assertThatThrownBy(() -> recovered.registered(registered, allocation(), shuffleOwner(anchor)))
                .hasMessageContaining("stale owner");
        recovered.registered(registered, allocation(), shuffleOwner(replacement));
        assertThat(exchangeReads().combined() - allEdgesBefore).isZero();
        assertThat(statementReads(exactRegistration) - exactRegistrationBefore).isEqualTo(2);
        assertThat(ModelAuthorityFixtures.artifacts().exchanges(dataSource)
                .load(KEY, GenerationExecutionOwner.from(replacement))).singleElement().satisfies(evidence ->
                        assertThat(evidence.registration().registeredOwner()).isEqualTo(GenerationExecutionOwner.from(anchor)));
    }

    private DurableStageShuffleEvidence journal(ExecutionAnchor owner) {
        return new DurableStageShuffleEvidence(ModelAuthorityFixtures.artifacts().exchanges(dataSource), spec, "41",
                () -> GenerationExecutionOwner.from(owner));
    }

    private static ShuffleRegistration shuffleRequest(long epoch) {
        return new ShuffleRegistration(KEY.executionId(), 1, 1, 0, false, epoch, 1,
                ShuffleProducerSetChecksum.forContiguousPartitions(KEY.executionId(), 1, epoch, 1));
    }

    private static ShuffleExecutionOwner shuffleOwner(ExecutionAnchor owner) {
        return new ShuffleExecutionOwner(owner.ownerId(), owner.ownerIncarnation(), owner.fencingToken());
    }

    private static Allocation allocation() {
        return new Allocation("allocation-1", ShuffleProviderDescriptor.of("test", 1, new byte[]{1, 2, 3}));
    }

    private static GenerationDispatchSpec specification() {
        return specification(KEY, 1);
    }

    private static GenerationDispatchSpec specification(GenerationJobKey key, int parallelism) {
        return specification(key, parallelism, new byte[] {1});
    }

    private static GenerationDispatchSpec specification(GenerationJobKey key, int parallelism, byte[] producerPlan) {
        var graph = new GenerationStageGraph(GenerationStageGraph.FORMAT_VERSION, GenerationGraphPurpose.RELATIONAL_EXPORT,
                List.of(GenerationStageGraph.StagePlan.of(1, parallelism, GenerationPageKind.RELATIONAL_COMPUTE,
                                producerPlan, GenerationStageTemplate.of(new byte[0], List.of(), List.of()),
                                List.of("lake.raw.items"), List.of("id"), false),
                        GenerationStageGraph.StagePlan.of(0, 1, GenerationPageKind.RELATIONAL_PARTITION,
                                new byte[]{2}, GenerationStageTemplate.of(new byte[]{3}, List.of(), List.of()),
                                List.of(), List.of("id"), true)),
                List.of(new GenerationStageGraph.Exchange("exchange-1-0", 1, 0,
                        GenerationExchangeDistribution.SINGLETON, List.of(), 1, "BATCH_SERDE_ARROW_BATCH", true)), List.of());
        return GenerationDispatchSpecFactory.create(key.jobId(), graph, key.tenantId(), "report-principal",
                DeferredAuthorizationScope.raw(new QualifiedTableName("lake", "raw", "items"), Set.of("id"),
                        new QualifiedTableName("lake", "features", "output")),
                new GenerationDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000));
    }

    private static GenerationJob job() {
        return job(KEY, 2);
    }

    private static GenerationJob job(GenerationJobKey key, int expectedPages) {
        var now = Instant.now();
        return new GenerationJob(key.tenantId(), key.jobId(), "feature-view/output", GenerationOutputTypeIds.TABULAR_EXPORT,
                key.jobId(), "lake.raw.items", "41", GenerationExecutionKind.EXPORT,
                GenerationDependencySet.sourceSnapshot("lake.raw.items", "41"), GenerationEmptyPartitionPolicy.ALLOW_ZERO_ROWS,
                "s3://warehouse/" + key.jobId(), "pangrova.tabular-export.arrow-stream", 1, 0, "a".repeat(64),
                "native", "", key.jobId(), expectedPages, 0, GenerationJobState.QUEUED, 0, now, now, now);
    }

    private static QueryManager queryManager(WorkerManager workers, CoordinatorTaskTracker tracker) {
        var scheduler = new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty());
        var remote = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());
        var dispatcher = new QueryDispatcher(QueryDispatcherContext.withDefaults(new SqlCompiler(), null, null, null,
                remote, scheduler, workers, tracker, null, null, null, 0L), null, null);
        return new QueryManager(QueryManagerTestContexts.minimal(dispatcher, new NoOpTracingProvider()));
    }
}
