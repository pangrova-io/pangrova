package org.pangrova.coordinator.generation;

import org.pangrova.coordinator.testinfra.ModelAuthorityFixtures;

import org.pangrova.query.store.jdbc.transaction.JdbcWriteCommitJournal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.ai.embedding.DurableGeneratedValueBuildService;
import org.pangrova.coordinator.ai.embedding.GeneratedValueMaterializationCoordinator;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactory;
import org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.generation.api.job.GenerationJobState;
import org.pangrova.generation.api.job.GenerationJobKey;
import org.pangrova.generation.api.GenerationOutputTypeIds;
import org.pangrova.generation.store.postgresql.job.PostgresGenerationJobStore;
import org.pangrova.metadata.generation.GeneratedValueBuildSubmissionStore;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.core.catalog.handle.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.planner.generation.GeneratedValueGenerationPlanBuilder;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.AllColumns;
import org.pangrova.parser.ast.Query;
import org.pangrova.parser.ast.SetQuery;
import org.pangrova.parser.ast.TableRef;
import org.pangrova.parser.ast.statement.dml.InsertSelectStatement;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.client.core.protocol.ResultEndpoint;
import org.pangrova.protocol.metadata.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.GeneratedColumnDefinition;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorBindingSet;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.connector.spi.index.CommitEvent;
import org.pangrova.connector.spi.index.CommitEventType;
import org.pangrova.security.api.UserIdentity;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.pangrova.coordinator.generation.GeneratedBuildTestConnectors.SnapshotConnector;
import org.pangrova.coordinator.generation.GeneratedBuildTestConnectors.SnapshotTableHandle;
import org.pangrova.coordinator.generation.GeneratedBuildTestConnectors.FixedCatalogRegistry;
import org.pangrova.coordinator.generation.GeneratedBuildTestWorkers.InventoryWorkerSelector;

import static org.pangrova.coordinator.generation.GeneratedBuildTestWorkers.worker;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
class DurableGeneratedValueBuildServiceTest extends org.pangrova.testing.testkit.ContainerTestBase {

    private org.pangrova.coordinator.testinfra.CoordinatorGenerationExecutionFixtures executions;

    @org.junit.jupiter.api.AfterEach
    void closeExecutionProcesses() {
        if (executions != null) executions.close();
    }
    private static final String SNAPSHOT = "41";
    private static final String TENANT = "tenant-a";
    private static final QualifiedTableName TABLE =
            new QualifiedTableName("lake", "sales", "documents");

    private PostgresGenerationJobStore store;
    private SnapshotGenerationSplitPlanner splitPlanner;
    private GeneratedValueBuildSubmissionStore submissions;
    private CatalogHandle catalogHandle;
    private javax.sql.DataSource dataSource;
    private org.pangrova.connector.spi.coordination.WriteCommitJournal writeJournal;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource(
                "durable_generated_values");
        store = ModelAuthorityFixtures.artifacts().builds(dataSource);
        writeJournal = new org.pangrova.query.store.jdbc.transaction.JdbcWriteCommitJournal(dataSource);
        executions = new org.pangrova.coordinator.testinfra.CoordinatorGenerationExecutionFixtures(dataSource);
        submissions = ModelAuthorityFixtures.artifacts().generatedSubmissions(dataSource);

        var connector = new SnapshotConnector(
                GeneratedColumnMetadataCodec.encode(definitions()));
        catalogHandle = new ConnectorCatalogHandle(
                new CatalogDescriptor(
                        "lake", CatalogType.CONNECTOR, "test", CatalogState.ACTIVE,
                        Map.of(), "", Set.of()),
                connector);
        splitPlanner = new SnapshotGenerationSplitPlanner(name -> catalogHandle);
        GeneratedAuthorityTestFixtures.provisionModel(dataSource);
        org.pangrova.coordinator.testinfra.CoordinatorGenerationAuthorityFixtures.provision(
                dataSource, TENANT, "alice", authorizationScope());
    }

    @Test
    void persistsReplayablePlanExactModelOutputArtifactAndSourcePages() throws Exception {
        WorkerInfo worker = worker(true);
        var service = new DurableGeneratedValueBuildService(
                store, submissions, splitPlanner,
                new InventoryWorkerSelector(worker),
                "s3://warehouse/system", new FixedCatalogRegistry(catalogHandle));

        bindCompiler(service);
        String fingerprint = GeneratedAuthorityTestFixtures.recordCommit(writeJournal, authorizationScope());
        var write = writeJournal.findByFingerprint(fingerprint).orElseThrow();
        List<String> jobIds = service.submitCommitted(write, definitions(), TABLE, SNAPSHOT, session());
        List<String> replayedJobIds = service.submitCommitted(write, definitions(), TABLE, SNAPSHOT, session());
        assertThat(writeJournal.isDeferredWorkAcknowledged(fingerprint)).isTrue();

        assertThat(jobIds).hasSize(1);
        assertThat(replayedJobIds).isEqualTo(jobIds);
        String jobId = jobIds.get(0);
        assertThat(store.findJob(TENANT, jobId)).hasValueSatisfying(job -> {
            assertThat(job.artifactType())
                    .isEqualTo(GenerationOutputTypeIds.GENERATED_COLUMN_VALUES);
            assertThat(job.sourceSnapshot()).isEqualTo(SNAPSHOT);
            assertThat(job.expectedPages()).isEqualTo(2);
            assertThat(job.state()).isEqualTo(GenerationJobState.QUEUED);
            assertThat(job.formatId())
                    .isEqualTo(BuiltInCapabilityCatalog
                            .GENERATED_VALUES_ARTIFACT_FORMAT_ID);
        });

        var dispatch = store.findDispatchSpec(TENANT, jobId).orElseThrow();
        assertThat(dispatch.tenantId()).isEqualTo(TENANT);
        assertThat(dispatch.principal()).isEqualTo("alice");
        OperatorBindingSet bindings = OperatorBindingSet.parseFrom(
                dispatch.stageGraph().requireStage(0).bindingSetBytes());
        assertThat(bindings.getBindingsList()).hasSize(2);
        OperatorBinding model = bindings.getBindings(0);
        assertThat(model.getPlanNodeId())
                .isEqualTo(GeneratedValueGenerationPlanBuilder.MODEL_INVOCATION_ID);
        assertThat(model.getModel().getModel().getVersion()).isEqualTo("model-v7");
        assertThat(model.hasExecutionPolicy()).isFalse();
        OperatorBinding sink = bindings.getBindings(1);
        assertThat(sink.getOutputArtifact().getArtifactType())
                .isEqualTo(GenerationOutputTypeIds.GENERATED_COLUMN_VALUES);
        assertThat(sink.getOutputArtifact().getPropertiesList())
                .anySatisfy(property -> {
                    assertThat(property.getName()).isEqualTo("vector_dimension");
                    assertThat(property.getValue().getInt64Value()).isEqualTo(768);
                })
                .anySatisfy(property -> {
                    assertThat(property.getName()).isEqualTo("output_type");
                    assertThat(property.getValue().getStringValue()).isEqualTo("VECTOR(768)");
                });

        assertFirstPinnedPage(jobId);
    }

    private void assertFirstPinnedPage(String jobId) {
        var firstPage = store.claimNextPage(
                TENANT, jobId, executions.takeOwnership(TENANT, jobId, "coordinator-a", 3), 3,
                Duration.ofMinutes(1), Instant.now())
                .orElseThrow();
        assertThat(firstPage.sourceObjectIds()).containsExactly("object-0");
        assertThat(firstPage.splits()).singleElement().satisfies(split ->
                assertThat(split.canonicalSplitBytes()).isNotEmpty());
    }

    @Test
    void missingNativeMaterializerFailsBeforeDurableJobCreation() {
        var service = new DurableGeneratedValueBuildService(
                store, submissions, splitPlanner,
                new InventoryWorkerSelector(worker(false)),
                "s3://warehouse/system", new FixedCatalogRegistry(catalogHandle));

        bindCompiler(service);
        String fingerprint = GeneratedAuthorityTestFixtures.recordCommit(writeJournal, authorizationScope());
        assertThatThrownBy(() -> service.submitCommitted(writeJournal.findByFingerprint(fingerprint).orElseThrow(),
                definitions(), TABLE, SNAPSHOT, session()))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("jointly advertises");
        assertThat(store.listRunnableJobs(10, Instant.now())).isEmpty();
        assertThat(writeJournal.isDeferredWorkAcknowledged(fingerprint)).isFalse();
    }

    @Test
    void productionCommitBridgeAdmitsAsyncAndPersistsExactSnapshotWork() {
        var service = new DurableGeneratedValueBuildService(
                store, submissions, splitPlanner,
                new InventoryWorkerSelector(worker(true)),
                "s3://warehouse/system", new FixedCatalogRegistry(catalogHandle));
        var coordinator = new GeneratedValueMaterializationCoordinator(
                new FixedCatalogRegistry(catalogHandle), service, dataSource, writeJournal,
                        ModelAuthorityFixtures.models());
        coordinator.activateCatalogs(Set.of("lake"));
        bindCompiler(service);

        coordinator.requireWriteAdmission(
                TABLE, definitions(),
                new UserIdentity(
                        "alice", Set.of(), Map.of("tenant_id", TENANT), Instant.EPOCH),
                "INSERT");
        String fingerprint = GeneratedAuthorityTestFixtures.recordCommit(writeJournal, authorizationScope());
        coordinator.onCommit(new CommitEvent(
                TENANT, "lake", "sales", "documents",
                List.of("s3://bucket/object-0.parquet"), List.of(), Instant.now(),
                SNAPSHOT, CommitEventType.APPEND, fingerprint));

        assertThat(store.listRunnableJobs(10, Instant.now()))
                .singleElement()
                .satisfies(key -> {
                    assertThat(key).isEqualTo(new GenerationJobKey(
                            TENANT, key.jobId()));
                    assertThat(store.findJob(key.tenantId(), key.jobId()).orElseThrow()
                            .sourceSnapshot()).isEqualTo(SNAPSHOT);
                });
    }

    @Test
    void descriptorFailureRollsBackEveryJobAndAcknowledgementThenAllowsExactRetry() throws Exception {
        var service = new DurableGeneratedValueBuildService(store, submissions, splitPlanner,
                new InventoryWorkerSelector(worker(true)), "s3://warehouse/system",
                new FixedCatalogRegistry(catalogHandle));
        bindCompiler(service);
        String fingerprint = GeneratedAuthorityTestFixtures.recordCommit(writeJournal, authorizationScope());
        var write = writeJournal.findByFingerprint(fingerprint).orElseThrow();
        sql("CREATE FUNCTION reject_generated_descriptor() RETURNS trigger LANGUAGE plpgsql "
                + "AS $$ BEGIN RAISE EXCEPTION 'descriptor failure'; END $$");
        sql("CREATE TRIGGER reject_generated_descriptor BEFORE INSERT ON generated_value_artifacts "
                + "FOR EACH ROW EXECUTE FUNCTION reject_generated_descriptor()");
        assertThatThrownBy(() -> service.submitCommitted(write, definitions(), TABLE, SNAPSHOT, session()))
                .isInstanceOf(org.pangrova.metadata.exception.MetastoreException.class);
        assertThat(store.listRunnableJobs(10, Instant.now())).isEmpty();
        assertThat(writeJournal.isDeferredWorkAcknowledged(fingerprint)).isFalse();
        sql("DROP TRIGGER reject_generated_descriptor ON generated_value_artifacts");
        assertThat(service.submitCommitted(write, definitions(), TABLE, SNAPSHOT, session())).hasSize(1);
        assertThat(writeJournal.isDeferredWorkAcknowledged(fingerprint)).isTrue();
    }

    @Test
    void alreadyAcknowledgedCommitDoesNotRecompileOrReauthorizeAfterRevocation() throws Exception {
        var service = new DurableGeneratedValueBuildService(store, submissions, splitPlanner,
                new InventoryWorkerSelector(worker(true)), "s3://warehouse/system",
                new FixedCatalogRegistry(catalogHandle));
        var coordinator = new GeneratedValueMaterializationCoordinator(
                new FixedCatalogRegistry(catalogHandle), service, dataSource, writeJournal,
                        ModelAuthorityFixtures.models());
        coordinator.activateCatalogs(Set.of("lake"));
        var compilations = new AtomicInteger();
        service.bindSourceCompiler((sql, actor) -> {
            compilations.incrementAndGet();
            return GeneratedProtectedInputFixtures.compiled();
        });
        String fingerprint = GeneratedAuthorityTestFixtures.recordCommit(writeJournal, authorizationScope());
        var event = new CommitEvent(TENANT, "lake", "sales", "documents", List.of(), List.of(),
                Instant.now(), SNAPSHOT, CommitEventType.APPEND, fingerprint);
        coordinator.onCommit(event);
        int beforeReplay = compilations.get();
        sql("DELETE FROM security_grants");
        coordinator.onCommit(event);
        assertThat(compilations).hasValue(beforeReplay);
        assertThat(writeJournal.isDeferredWorkAcknowledged(fingerprint)).isTrue();
        assertThat(store.listRunnableJobs(10, Instant.now())).hasSize(1);
    }

    private void sql(String statement) throws java.sql.SQLException {
        try (var connection = dataSource.getConnection(); var command = connection.createStatement()) {
            command.execute(statement);
        }
    }

    @Test
    void generatedInsertUsesAuthenticatedUserAndResolvedTenantBeforeNativeDispatch() {
        var selector = new InventoryWorkerSelector(worker(true));
        var service = new DurableGeneratedValueBuildService(
                store, submissions, splitPlanner, selector,
                "s3://warehouse/system", new FixedCatalogRegistry(catalogHandle));
        var registry = new FixedCatalogRegistry(catalogHandle);
        var coordinator = new GeneratedValueMaterializationCoordinator(registry, service, dataSource, writeJournal,
                ModelAuthorityFixtures.models());
        coordinator.activateCatalogs(Set.of("lake"));
        AtomicInteger dispatches = new AtomicInteger();
        var factory = new PlanExecutionFactory(new PlanExecutionFactoryContext(
                (state, session, statement) -> {
                    dispatches.incrementAndGet();
                    return new DispatchResult("worker.invalid", "task-1",
                            new ResultEndpoint("worker.invalid", 1, "ticket", "grpc"));
                }, null, (sql, session) -> sql, registry, coordinator));
        var statement = new InsertSelectStatement(
                Identifier.of("lake", "sales", "documents"), Optional.empty(),
                false, new SetQuery(List.of(new Query(false,
                        List.of(new AllColumns()),
                        List.of(new TableRef(Identifier.of("lake", "sales", "documents"))),
                        Optional.empty(), List.of(), Optional.empty(), List.of(),
                        Optional.empty()))));
        Instant authenticatedAt = Instant.parse("2026-01-01T00:00:00Z");
        var user = new UserIdentity("alice", Set.of("model-users"),
                Map.of("tenant_id", "untrusted-tenant", "department", "search"),
                authenticatedAt);
        var session = new CoordinatorSessionContext(
                "generated-insert", Map.of(), user, TENANT);

        factory.execute(statement, new StatementExecutionContext(
                new QueryStateMachine(), session.queryId(), session, null, null));

        assertThat(dispatches).hasValue(1);
        assertThat(selector.resolvedUser).isNotNull();
        assertThat(selector.resolvedUser.principal()).isEqualTo("alice");
        assertThat(selector.resolvedUser.groups()).containsExactly("model-users");
        assertThat(selector.resolvedUser.attributes())
                .containsEntry("tenant_id", TENANT)
                .containsEntry("department", "search");
        assertThat(selector.resolvedUser.authenticatedAt()).isEqualTo(authenticatedAt);

        var unauthenticated = new CoordinatorSessionContext(
                "generated-denied", Map.of(), null, TENANT);
        assertThatThrownBy(() -> factory.execute(statement, new StatementExecutionContext(
                new QueryStateMachine(), unauthenticated.queryId(),
                unauthenticated, null, null)))
                .hasMessageContaining("authenticated user and resolved tenant");
        assertThat(dispatches).hasValue(1);
    }

    @Test
    void syncGeneratedWriteRequiresSameAtomicNativeWriteRelation() {
        var service = new DurableGeneratedValueBuildService(
                store, submissions, splitPlanner,
                new InventoryWorkerSelector(worker(true)),
                "s3://warehouse/system", new FixedCatalogRegistry(catalogHandle));
        var coordinator = new GeneratedValueMaterializationCoordinator(
                new FixedCatalogRegistry(catalogHandle), service, dataSource, writeJournal,
                        ModelAuthorityFixtures.models());
        coordinator.activateCatalogs(Set.of("lake"));
        GeneratedColumnSet sync = definitions().toBuilder()
                .setDefinitions(0, definitions().getDefinitions(0).toBuilder()
                        .setExecutionMode(ExecutionMode.EXECUTION_MODE_SYNC))
                .build();

        assertThatThrownBy(() -> coordinator.requireWriteAdmission(
                TABLE, sync, null, "UPDATE"))
                .hasMessageContaining("same atomic commit");
    }

    @Test
    void writeAdmissionFailsClosedWhenCatalogCommitDeliveryIsNotWired() {
        var service = new DurableGeneratedValueBuildService(
                store, submissions, splitPlanner,
                new InventoryWorkerSelector(worker(true)),
                "s3://warehouse/system", new FixedCatalogRegistry(catalogHandle));
        var coordinator = new GeneratedValueMaterializationCoordinator(
                new FixedCatalogRegistry(catalogHandle), service, dataSource, writeJournal,
                        ModelAuthorityFixtures.models());

        assertThatThrownBy(() -> coordinator.requireWriteAdmission(
                TABLE, definitions(), null, "MERGE"))
                .hasMessageContaining("no durable connector commit listener")
                .hasMessageContaining("lake");
    }

    private org.pangrova.security.api.authz.DeferredAuthorizationScope authorizationScope() {
        return GeneratedAuthorityTestFixtures.scope(dataSource, TABLE,
                catalogHandle.getMetadata(session()).getTableMetadata(session(), new SnapshotTableHandle(-1)), definitions());
    }

    private static void bindCompiler(DurableGeneratedValueBuildService service) {
        service.bindSourceCompiler((sql, actor) -> GeneratedProtectedInputFixtures.compiled());
    }

    private static GeneratedColumnSet definitions() {
        return GeneratedColumnSet.newBuilder()
                .setSchemaVersion(1)
                .addDefinitions(GeneratedColumnDefinition.newBuilder()
                        .setTargetColumn("embedding")
                        .setCapability(BuiltInCapabilityCatalog.aiEmbeddingPlanRef())
                        .setLogicalModelName("catalog.embed")
                        .addSourceColumns("title")
                        .addSourceColumns("body")
                        .setExecutionMode(ExecutionMode.EXECUTION_MODE_ASYNC)
                        .setOutputType(VectorLogicalType.create(768)))
                .build();
    }

    private static ConnectorSession session() {
        return new ConnectorSession() {
            @Override
            public java.util.Optional<org.pangrova.connector.spi.ConnectorQueryAuthority> getQueryAuthority() {
                return java.util.Optional.empty();
            }

            @Override
            public String getUser() {
                return "alice";
            }

            @Override
            public Optional<String> getCatalog() {
                return Optional.of("lake");
            }

            @Override
            public Optional<String> getSchema() {
                return Optional.of("sales");
            }

            @Override
            public Map<String, String> getProperties() {
                return Map.of("tenant_id", TENANT);
            }
        };
    }

}
