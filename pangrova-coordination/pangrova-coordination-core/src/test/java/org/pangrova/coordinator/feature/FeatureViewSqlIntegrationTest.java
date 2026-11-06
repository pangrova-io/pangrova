package org.pangrova.coordinator.feature;

import org.pangrova.coordinator.testinfra.QueryManagerTestContexts;
import org.pangrova.query.api.lifecycle.QueryState;
import org.pangrova.coordinator.testinfra.ModelAuthorityFixtures;
import org.pangrova.generation.codec.execution.GenerationStageGraphCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.generation.SnapshotGenerationSplitPlanner;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.coordinator.metadata.PluginConnectorHandleFactory;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.query.core.lifecycle.QueryStateMachine;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.planner.core.analysis.AuthorizedRelation;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.security.AccessControlEnforcer;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.scheduler.builtin.placement.LoadBalancedStrategy;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.ai.control.assembly.AiInfrastructureStores;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.ai.control.feature.FeatureCatalogStore.FeatureViewId;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.planner.core.analysis.PangrovaPlanner;
import org.pangrova.planner.core.analysis.SqlCompiler;
import org.pangrova.planner.core.security.ColumnMaskApplier;
import org.pangrova.planner.core.security.RowFilterApplier;
import org.pangrova.security.policy.builtin.authorization.DynamicRbacAccessControl;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.core.mask.ColumnMaskingProcessor;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.security.core.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.core.secret.SecretResolverRegistry;
import org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore;
import org.pangrova.connector.spi.plugin.ConnectorFactory;
import org.pangrova.connector.api.query.ConnectorQueryAuthority;
import org.pangrova.types.PangrovaType;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.security.api.identity.UserIdentity;
import javax.sql.DataSource;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresTableProjectionRepository;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresMetadataProjectionTransactions;
import org.pangrova.persistence.catalog.postgresql.synchronization.PostgresMetadataSyncRepository;

/** Actual PostgreSQL governance and real connector/planner/policy chain; no client execution plans. */
class FeatureViewSqlIntegrationTest {
    private static final String TENANT = "tenant-a";
    private static final UserIdentity USER = new UserIdentity("feature-owner", Set.of(), Map.of(), Instant.EPOCH);
    @TempDir Path warehouse;
    private DataSource dataSource;
    private MetadataBootstrap metadata;
    private PluginConnectorHandleFactory connectorFactory;
    private SqlCompiler compiler;
    private RemoteTaskDispatcher transport;
    private QueryDispatcher queryDispatcher;
    private QueryManager queryManager;
    private CoordinatorTaskTracker taskTracker;
    private int statementNumber;
    private AiInfrastructureStores infrastructure;
    private org.pangrova.persistence.security.api.rowfilter.RowPolicyStore policies;
    private org.pangrova.persistence.security.api.mask.EnrichedColumnMaskStore masks;
    private final AtomicReference<StatementResult> completed = new AtomicReference<>();
    private final AtomicReference<AuthorizedRelation> observedCompile = new AtomicReference<>();
    private final AtomicReference<ConnectorQueryAuthority> observedLifetime = new AtomicReference<>();

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("feature_secure_sql");
        infrastructure = AiInfrastructureStores.jdbc(dataSource,
                ModelAuthorityFixtures.artifacts().builds(dataSource));
        var store = new PostgresCatalogDeploymentRepository(dataSource);
        initializeSourceCatalog(store);
        var roles = new JdbcRoleStore(dataSource);
        roles.createRole(Role.of("feature-role"));
        roles.grantRoleToUser(UserRoleAssignment.of(USER.principal(), "feature-role"));
        var grants = new JdbcGrantStore(dataSource);
        grants.addGrant(PrivilegeGrant.of("feature-role", Privilege.ALL, "SCHEMA", "lake", "sales", null, null));
        grants.addGrant(PrivilegeGrant.of("feature-role", Privilege.SELECT, "TABLE", "lake", "raw", "customers", null));
        policies = new org.pangrova.metadata.service.TableControlRowPolicyStore(
                new org.pangrova.metadata.service.RegistryTablePolicyAuthority(
                        metadata.catalogRegistry()),
                new JdbcRowPolicyStore(dataSource));
        var persistedMasks = new JdbcColumnMaskStore(dataSource);
        masks = new org.pangrova.metadata.adapter.security.TableControlColumnMaskStore(
                new org.pangrova.metadata.service.RegistryTablePolicyAuthority(
                        metadata.catalogRegistry()),
                persistedMasks);
        policies.savePolicy(new RowPolicy(0, "source_rows", "lake", "raw", "customers",
                "customer_id >= 100", List.of("feature-role"), true, 1, null));
        masks.saveColumnMask(new ColumnMask(0, "source_secret", "lake", "raw", "customers", "secret",
                "'hidden'", List.of("feature-role"), List.of(), true, null));
        var access = new DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(policies), new ColumnMaskingProcessor(masks));
        compiler = new SqlCompiler().withSessionPlanner(scope -> new PangrovaPlanner(
                metadata.buildCalciteSchema(scope), List.of(scope.getCatalog().orElseThrow(), scope.getSchema().orElseThrow())));
        transport = new RemoteTaskDispatcher(org.pangrova.common.resilience.CircuitBreakerPolicy.noOp(),
                org.pangrova.testing.testkit.TestObservability.TRACING);
        taskTracker = new CoordinatorTaskTracker();
        var dispatcherContext = QueryDispatcherContext.withDefaults(compiler,
                new AccessControlEnforcer(access), new ColumnMaskApplier(access), new RowFilterApplier(access),
                transport, new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty()),
                new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()),
                taskTracker, metadata.catalogRegistry(), null, null, 0);
        var exports = new DurableFeatureExportService(infrastructure.catalogExports(),
                new SnapshotGenerationSplitPlanner(metadata.catalogRegistry()), warehouse.resolve("exports").toString(),
                metadata.catalogRegistry());
        var service = new FeatureViewStatementService(infrastructure.features(), exports, access, (sql, actor) -> {
            observedLifetime.set(actor.queryAuthority().orElseThrow());
            AuthorizedRelation compiled = queryDispatcher.compileAuthorizedRelation(sql, actor);
            observedCompile.set(compiled);
            return compiled;
        });
        var tasks = new HashMap<Class<? extends Statement>, org.pangrova.coordinator.query.execution.dispatch.MetadataTask<?>>();
        var factory = new MetadataExecutionFactory(tasks);
        FeatureViewMetadataTasks.register(factory, service);
        var statements = new StatementDispatcher(new StatementDispatcherContext(compiler, null, List.of(factory),
                List.of(event -> completed.set(event.result())), access));
        queryDispatcher = new QueryDispatcher(dispatcherContext, statements);
        queryManager = new QueryManager(QueryManagerTestContexts.minimal(
                queryDispatcher, org.pangrova.testing.testkit.TestObservability.TRACING));
    }

    private void initializeSourceCatalog(PostgresCatalogDeploymentRepository store) {
        store.saveCatalogDescriptor(new CatalogDescriptor("lake", CatalogType.CONNECTOR, "paimon",
                CatalogState.ACTIVE, Map.of("warehouse", warehouse.toUri().toString()), "", Set.of()));
        connectorFactory = new PluginConnectorHandleFactory(ConnectorFactory.class.getClassLoader(),
                new SecretResolverRegistry(), ignored -> Optional.empty());
        metadata = MetadataBootstrap.initialize(store, new PostgresTableProjectionRepository(dataSource), new PostgresMetadataProjectionTransactions(dataSource), new PostgresMetadataSyncRepository(dataSource), connectorFactory);
        var sourceSession = SessionContextConverter.toConnectorSession(session(), "lake", "raw");
        var connectorMetadata = metadata.catalogRegistry().getCatalog("lake").getMetadata(sourceSession);
        connectorMetadata.createSchema(sourceSession, "raw");
        connectorMetadata.createTable(sourceSession, new ConnectorTableMetadata(
                new SchemaTableName("raw", "customers"), List.of("customer_id", "event_ts", "secret"),
                List.of(new ConnectorTableMetadata.Column("customer_id", PangrovaType.BIGINT),
                        new ConnectorTableMetadata.Column("event_ts", PangrovaType.TIMESTAMP),
                        new ConnectorTableMetadata.Column("secret", PangrovaType.VARCHAR)), Map.of()));
        var registry = metadata.catalogRegistry();
        registry.setTableControlService(new org.pangrova.table.core.ownership.TableControlService(
                new org.pangrova.persistence.table.postgresql.control.JdbcTableControlStore(dataSource),
                registry::requireCurrentCatalogIdentity));
        registry.setGovernanceBindingStore(new org.pangrova.persistence.table.postgresql.governancebinding.JdbcTableControlGovernanceBindingStore(
                dataSource));
        var source = connectorMetadata.getTableHandle(sourceSession, new SchemaTableName("raw", "customers"));
        registry.tableControl().adopt("lake", "raw", "customers", USER.principal(),
                connectorMetadata.getTableIdentity(sourceSession, source),
                connectorMetadata.observeVersion(sourceSession, source));
    }

    @AfterEach
    void close() throws Exception {
        Throwable failure = null;
        for (var owner : new AutoCloseable[]{
                queryManager == null ? null : () -> assertThat(queryManager.awaitAllQueries(5000)).isTrue(),
                transport,
                compiler == null || compiler.queryResultCache() == null ? null : compiler.queryResultCache(),
                metadata == null ? null : metadata::shutdown,
                connectorFactory == null ? null : connectorFactory::shutdown}) {
            if (owner == null) continue;
            try {
                owner.close();
            } catch (Exception | Error cleanup) {
                if (failure == null) failure = cleanup;
                else if (failure != cleanup) failure.addSuppressed(cleanup);
            }
        }
        if (failure instanceof Error error) throw error;
        if (failure instanceof Exception exception) throw exception;
    }

    @Test
    void createCompilesRealSourceSqlWithRlsAndMaskRatherThanTrustingAnExecutionPlan() {
        dispatch("CREATE FEATURE VIEW customer_features VERSION 1 AS '" + definition("customers") + "'");
        var stored = infrastructure.features().findFeatureView(id("customer_features"), 1).orElseThrow();
        assertThat(stored.sourceQuery()).isEqualTo("SELECT customer_id,event_ts,secret FROM customers");
        assertThat(stored.definitionJson()).doesNotContain("source_plan");
        assertThat(stored.sourcePlan()).isEqualTo(observedCompile.get().planBytes());
        var observed = observedCompile.get().observedPolicies().get("lake.raw.customers");
        assertThat(observed.rowFilter()).hasValueSatisfying(filter -> assertThat(filter).contains("100"));
        assertThat(observed.columnMasks().get("secret")).hasValue("'hidden'");
        assertThat(observedCompile.get().sources().get(0).qualifiedName()).isEqualTo("lake.raw.customers");
    }

    @Test
    void materializeRecompilesCurrentPoliciesAndIgnoresCorruptPersistedAuditPlan() throws Exception {
        dispatch("CREATE FEATURE VIEW customer_features VERSION 1 AS '" + definition("customers") + "'");
        try (var connection = dataSource.getConnection(); var update = connection.prepareStatement(
                "UPDATE feature_view_versions SET source_plan=? WHERE tenant_id=? AND feature_view_name=?")) {
            update.setBytes(1, new byte[] {1, 2, 3});
            update.setString(2, TENANT);
            update.setString(3, "customer_features");
            update.executeUpdate();
        }
        for (var policy : policies.getPolicies("lake", "raw", "customers")) policies.deletePolicy(policy.policyId());
        policies.savePolicy(new RowPolicy(0, "changed_rows", "lake", "raw", "customers",
                "customer_id >= 200", List.of("feature-role"), true, 1, null));
        observedCompile.set(null);
        // The real empty table has no snapshot 41. Recompilation must happen first;
        // snapshot lookup then fails instead of executing unpinned current data.
        Throwable failure = catchThrowable(() -> dispatch(
                "MATERIALIZE FEATURE VIEW customer_features VERSION 1 SNAPSHOT '41'"));
        assertThat(failure).isInstanceOf(PangrovaException.class)
                .hasMessageContaining("Snapshot not found: snapshot-id=41");
        assertThat(((PangrovaException) failure).getErrorCode())
                .isEqualTo(PangrovaErrorCode.CONNECTOR_SNAPSHOT_NOT_FOUND);
        assertThat(observedCompile.get()).as("Current secure compilation must precede snapshot failure: %s", failure)
                .isNotNull();
        assertThat(observedCompile.get().observedPolicies().get("lake.raw.customers").rowFilter())
                .hasValueSatisfying(filter -> assertThat(filter).contains("200"));
    }

    @Test
    void publishUsesMonotonicEpochFenceAndRejectsAba() {
        dispatch("CREATE FEATURE VIEW customer_features VERSION 1 AS '" + definition("customers") + "'");
        dispatch("CREATE FEATURE VIEW customer_features VERSION 2 AS '" + definition("customers").replace("v1", "v2") + "'");
        dispatch("ALTER FEATURE VIEW customer_features PUBLISH VERSION 1");
        dispatch("ALTER FEATURE VIEW customer_features PUBLISH VERSION 2 EXPECTING EPOCH 1");
        dispatch("ALTER FEATURE VIEW customer_features PUBLISH VERSION 1 EXPECTING EPOCH 2");
        assertThat(completed.get().rows().get(0).getLong("publication_epoch")).isEqualTo(3);
        assertThatThrownBy(() -> dispatch("ALTER FEATURE VIEW customer_features PUBLISH VERSION 2 EXPECTING EPOCH 1"))
                .hasMessageContaining("publication epoch changed");
    }

    @Test
    void rejectsClientPlansMismatchedOutputEntityEventAndTypes() {
        String base = definition("customers");
        assertThatThrownBy(() -> dispatch("CREATE FEATURE VIEW forged VERSION 1 AS '"
                + base.replace("\"source_query\":", "\"source_plan_base64\":\"AQID\",\"source_query\":") + "'"))
                .hasMessageContaining("source_plan_base64");
        assertThatThrownBy(() -> dispatch("CREATE FEATURE VIEW bad_entity VERSION 1 AS '"
                + base.replace("\"entity_keys\":[\"customer_id\"]", "\"entity_keys\":[\"missing\"]") + "'"))
                .hasMessageContaining("entity_keys");
        assertThatThrownBy(() -> dispatch("CREATE FEATURE VIEW bad_event VERSION 1 AS '"
                + base.replace("\"event_time\":\"event_ts\"", "\"event_time\":\"secret\"") + "'"))
                .hasMessageContaining("event_time");
        assertThatThrownBy(() -> dispatch("CREATE FEATURE VIEW bad_type VERSION 1 AS '"
                + base.replace("\"customer_id\":\"BIGINT\"", "\"customer_id\":\"VARCHAR\"") + "'"))
                .hasMessageContaining("compiled output type");
    }

    @Test
    void globalRelationFreezesTheProtectedDagExactPinnedPagesAndClosedConsumerBarrier() throws Exception {
        long snapshot = commitSourceRows();
        var target = new org.pangrova.connector.api.metadata.table.QualifiedTableName("lake", "sales", "global_limit");
        var scope = org.pangrova.security.api.authorization.DeferredAuthorizationScope.raw(
                new org.pangrova.connector.api.metadata.table.QualifiedTableName("lake", "raw", "customers"),
                Set.of("customer_id", "event_ts", "secret"), target);
        org.pangrova.coordinator.testinfra.CoordinatorGenerationAuthorityFixtures.provision(
                dataSource, TENANT, USER.principal(), scope);
        dispatch("CREATE FEATURE VIEW global_limit VERSION 1 AS '"
                + definition("customers").replace("FROM customers", "FROM customers ORDER BY customer_id DESC LIMIT 1")
                + "'");
        dispatch("MATERIALIZE FEATURE VIEW global_limit VERSION 1 SNAPSHOT '" + snapshot + "'");
        var compiled = observedCompile.get();
        var builds = ModelAuthorityFixtures.artifacts().builds(dataSource);
        String jobId = completed.get().rows().get(0).getString("job_id");
        var spec = builds.findDispatchSpec(TENANT, jobId).orElseThrow();
        assertThat(compiled.splitIndependent()).isFalse();
        assertThat(compiled.observedPolicies().get("lake.raw.customers").rowFilter()).isPresent();
        assertThat(spec.stageGraph().exchanges()).isNotEmpty();
        for (var stage : compiled.stageGraph().stages()) {
            var persisted = spec.stageGraph().requireStage(stage.stageId());
            assertThat(persisted.parallelism()).isEqualTo(stage.parallelism());
            assertThat(persisted.outputNames()).isEqualTo(stage.outputNames());
            assertThat(builds.findJob(TENANT, jobId).orElseThrow().expectedPages())
                    .isEqualTo(spec.stageGraph().stages().stream().mapToInt(value -> value.parallelism()).sum());
        }
        try (var ownership = new org.pangrova.coordinator.testinfra.CoordinatorGenerationExecutionFixtures(dataSource)) {
            var anchor = ownership.takeOwnership(TENANT, jobId, "global-coordinator", 4);
            var stages = ModelAuthorityFixtures.artifacts().stages(dataSource);
            var terminal = spec.stageGraph().stages().stream().filter(value -> value.publicationOutput())
                    .findFirst().orElseThrow();
            assertThat(terminal.parallelism()).isEqualTo(1);
            var key = new org.pangrova.generation.api.identity.GenerationJobKey(TENANT, jobId);
            var progress = stages.inspectPartition(key, anchor, terminal.stageId(), 0);
            assertThat(progress.pageOrdinal()).isPositive();
            assertThat(stages.preparePartition(new org.pangrova.persistence.generation.api.execution
                    .GenerationStageExecutionStore.PartitionClaim(
                            key, anchor, terminal.stageId(), 0, progress.pageOrdinal(),
                            4, java.time.Duration.ofMinutes(1), Instant.now()),
                    evidence -> evidence.lease())).isEmpty();
            var frozen = stages.prepareExecution(
                    new org.pangrova.generation.api.identity.GenerationJobKey(TENANT, jobId), anchor);
            assertThat(GenerationStageGraphCodec.sha256(frozen.authority().dispatchSpec().stageGraph())).isEqualTo(GenerationStageGraphCodec.sha256(spec.stageGraph()));
        }
        assertPinnedPageCoverage(jobId, snapshot);
        assertTopologicalPageOrdinals(jobId);
    }

    private void assertTopologicalPageOrdinals(String jobId) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement("""
                SELECT producer.page_ordinal AS upstream, consumer.page_ordinal AS downstream
                FROM artifact_build_page_dependencies dependency
                JOIN artifact_build_pages producer ON producer.tenant_id=dependency.tenant_id
                  AND producer.job_id=dependency.job_id AND producer.page_id=dependency.dependency_page_id
                JOIN artifact_build_pages consumer ON consumer.tenant_id=dependency.tenant_id
                  AND consumer.job_id=dependency.job_id AND consumer.page_id=dependency.page_id
                WHERE dependency.tenant_id=? AND dependency.job_id=?
                """)) {
            statement.setString(1, TENANT);
            statement.setString(2, jobId);
            try (var rows = statement.executeQuery()) {
                int count = 0;
                while (rows.next()) {
                    assertThat(rows.getInt("upstream")).isLessThan(rows.getInt("downstream"));
                    count++;
                }
                assertThat(count).isPositive();
            }
        }
    }

    private long commitSourceRows() throws Exception {
        var options = new org.apache.paimon.options.Options();
        options.setString("warehouse", warehouse.toUri().toString());
        try (var catalog = org.apache.paimon.catalog.CatalogFactory.createCatalog(
                org.apache.paimon.catalog.CatalogContext.create(options))) {
            var table = (org.apache.paimon.table.FileStoreTable) catalog.getTable(
                    org.apache.paimon.catalog.Identifier.create("raw", "customers"));
            var builder = table.newBatchWriteBuilder();
            try (var writer = builder.newWrite(); var commit = builder.newCommit()) {
                for (long id : new long[] {50, 150, 250}) {
                    writer.write(org.apache.paimon.data.GenericRow.of(id,
                            org.apache.paimon.data.Timestamp.fromInstant(Instant.parse("2026-01-01T00:00:00Z")),
                            org.apache.paimon.data.BinaryString.fromString("private")));
                }
                commit.commit(writer.prepareCommit());
            }
            return table.snapshotManager().latestSnapshotId();
        }
    }

    private void assertPinnedPageCoverage(String jobId, long snapshot) throws Exception {
        var pinned = enumeratePinnedCoverage(snapshot);
        assertThat(pinned).isNotEmpty();
        try (var connection = dataSource.getConnection(); var query = connection.prepareStatement(
                "SELECT source_object_id FROM artifact_build_page_sources WHERE tenant_id=? AND job_id=?")) {
            query.setString(1, TENANT);
            query.setString(2, jobId);
            var ids = new java.util.ArrayList<String>();
            try (var rows = query.executeQuery()) {
                while (rows.next()) ids.add(rows.getString(1));
            }
            assertThat(ids).containsExactlyInAnyOrderElementsOf(pinned.stream().map(value -> value.dataObjectId()).toList());
            assertThat(ids).doesNotHaveDuplicates();
        }
    }

    /** Independent component oracle with a real engine-issued, state-bound and explicitly closed lifetime. */
    private List<org.pangrova.generation.api.page.GenerationSplit> enumeratePinnedCoverage(long snapshot) {
        String queryId = "feature-coverage";
        var state = new QueryStateMachine();
        var authority = state.bindConnectorAuthority(taskTracker.openQueryIssuance(queryId));
        var actor = SessionContextConverter.toConnectorSession(
                session().withQueryId(queryId).withQueryAuthority(authority), "lake", "raw");
        try {
            authority.beginMetadata("lake", () -> metadata.catalogRegistry().getCatalog("lake").getMetadata(actor));
            return new SnapshotGenerationSplitPlanner(metadata.catalogRegistry()).enumerate(
                    new org.pangrova.connector.api.metadata.table.QualifiedTableName("lake", "raw", "customers"),
                    Long.toString(snapshot), actor);
        } finally {
            state.tryTransition(QueryState.CANCELLED);
            taskTracker.sealQueryIssuance(queryId);
            authority.requireDrained();
            authority.cleanupMetadata().requireSuccess();
            taskTracker.retireQueryIssuance(queryId);
            assertThat(authority.isClosed()).isTrue();
        }
    }

    private void dispatch(String sql) {
        String queryId = "feature-statement-" + ++statementNumber;
        try {
            queryManager.submitQuery(queryId, sql, session());
            assertThat(queryManager.getQueryState(queryId)).contains(QueryState.FINISHED);
        } finally {
            assertThat(queryManager.getActiveQueryCount()).isZero();
            assertThat(queryManager.getActiveQueryIds()).isEmpty();
            var lifetime = observedLifetime.get();
            if (lifetime != null && queryId.equals(lifetime.queryId())) {
                assertThat(lifetime.isClosed()).isTrue();
                lifetime.requireDrained();
            }
        }
    }
    private static FeatureViewId id(String name) { return new FeatureViewId(TENANT, "lake", "sales", name); }
    private static QuerySession session() {
        return new QuerySession("feature-query", Map.of("catalog", "lake", "schema", "sales"), USER, TENANT, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
    }
    private static String definition(String sourceQueryTable) {
        return "{\"entity_keys\":[\"customer_id\"],\"event_time\":\"event_ts\","
                + "\"source_table\":\"lake.raw.customers\","
                + "\"source_query\":\"SELECT customer_id,event_ts,secret FROM " + sourceQueryTable + "\","
                + "\"transform_version\":\"v1\",\"schema\":{\"customer_id\":\"BIGINT\",\"event_ts\":\"TIMESTAMP\",\"secret\":\"VARCHAR\"},"
                + "\"freshness_target_ms\":60000,\"lineage\":[\"lake.raw.customers\"]}";
    }
}
