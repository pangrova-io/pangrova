package org.pangrova.coordinator.query;
import org.pangrova.query.runtime.QueryManager;

import org.pangrova.planner.metadata.catalog.MetadataSchemaBuilder;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.connector.iceberg.IcebergMetadata;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.coordinator.metadata.PluginConnectorHandleFactory;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.security.AccessControlEnforcer;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.scheduler.api.exception.NoAvailableWorkerException;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.strategies.placement.LoadBalancedStrategy;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.parser.api.exception.ParseException;
import org.pangrova.planner.analysis.PangrovaPlanner;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.planner.security.ColumnMaskApplier;
import org.pangrova.planner.security.RowFilterApplier;
import org.pangrova.security.policy.authorization.DynamicRbacAccessControl;
import org.pangrova.security.api.policy.GroupRoleMapping;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.secret.resolution.SecretResolverRegistry;
import org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.testing.testkit.constants.TestObservability;
import org.pangrova.testing.testkit.ContainerTestBase;
import javax.sql.DataSource;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresTableProjectionRepository;
import org.pangrova.persistence.workflow.postgresql.projection.PostgresMetadataProjectionTransactions;
import org.pangrova.persistence.catalog.postgresql.synchronization.PostgresMetadataSyncRepository;

/**
 * Real PostgreSQL directory, durable authority and Iceberg snapshot lifecycle component test.
 * No Worker executes rows: valid SQL deliberately reaches the no-Worker dispatch failure.
 * Pins are actual connector resources seeded before dispatch, not invocation counters or Native E2E proof.
 */
class QueryDispatcherCleanupOnFailureTest extends ContainerTestBase {
    private static final String CATALOG = "lake";
    private static final String UNREFERENCED_CATALOG = "unreferenced";
    private static final String SCHEMA = "db";
    private static final String TABLE = "records";
    private static final String READER = "cleanup_reader";
    private static final String SQL = "SELECT id FROM lake.db.records";

    @TempDir Path warehouse;
    private HadoopCatalog sdkCatalog;
    private PluginConnectorHandleFactory connectors;
    private MetadataBootstrap metadata;
    private RemoteTaskDispatcher remoteDispatcher;
    private QueryDispatcher dispatcher;
    private long snapshotId;

    @BeforeEach
    void setUp() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("query_cleanup");
        createSdkTable();
        var store = new PostgresCatalogDeploymentRepository(dataSource);
        for (String name : List.of(CATALOG, UNREFERENCED_CATALOG)) {
            store.saveCatalogDescriptor(new CatalogDescriptor(name, CatalogType.CONNECTOR, "iceberg",
                    CatalogState.ACTIVE, Map.of("warehouse", warehouse.toString(), "type", "hadoop"),
                    "", Set.of()));
        }
        connectors = new PluginConnectorHandleFactory(getClass().getClassLoader(),
                new SecretResolverRegistry(), ignored -> Optional.empty());
        metadata = MetadataBootstrap.initialize(memoryTableStores(), store, new PostgresTableProjectionRepository(dataSource), new PostgresMetadataProjectionTransactions(dataSource), new PostgresMetadataSyncRepository(dataSource), connectors);
        var compiler = new SqlCompiler().withSessionPlanner((session, revision) ->
                new PangrovaPlanner(new MetadataSchemaBuilder(
                        metadata.metadataService(), metadata.catalogRegistry())
                        .buildRootSchema(session), List.of(CATALOG, SCHEMA)));
        remoteDispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        var authority = createAuthority(dataSource);
        var context = QueryDispatcherContext.withDefaults(compiler,
                new AccessControlEnforcer(authority), new ColumnMaskApplier(authority), new RowFilterApplier(authority),
                remoteDispatcher, new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty()),
                new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()),
                new CoordinatorTaskTracker(), metadata.catalogRegistry(), null, null, 0L);
        dispatcher = new QueryDispatcher(context, null, null);
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            if (remoteDispatcher != null) remoteDispatcher.close();
        } finally {
            try {
                if (metadata != null) metadata.shutdown();
            } finally {
                try {
                    if (connectors != null) connectors.shutdown();
                } finally {
                    if (sdkCatalog != null) sdkCatalog.close();
                }
            }
        }
    }

    @Test
    void executeFailure_releasesOnlyActuallyRegisteredConnectorSnapshot() {
        var session = session("q-cleanup-1");
        var state = new QueryStateMachine();
        var authority = dispatcher.bindConnectorAuthority(state, session.queryId());
        session = session.withQueryAuthority(authority);
        var registered = (IcebergMetadata) metadata.catalogRegistry()
                .queryCatalog(CATALOG, SessionContextConverter.toConnectorSession(
                        session, CATALOG, SCHEMA)).metadata();
        var unrelated = connectorMetadata(UNREFERENCED_CATALOG, session);
        pin(registered, session.queryId());
        var unrelatedOwner = new org.pangrova.coordinator.task.TestConnectorQuery(session.queryId());
        beginAndPin(unrelated, unrelatedOwner.authority());
        var admittedSession = session;

        assertThrows(NoAvailableWorkerException.class, () -> dispatcher.execute(state, SQL, admittedSession));

        // QueryManager publishes the failure, then claims the physical cleanup.
        assertThat(state.getState()).isEqualTo(QueryState.SCHEDULING);
        assertThat(dispatcher.tryCleanupQuery(session.queryId(), null)).isTrue();
        assertThat(registered.getPinnedSnapshot(session.queryId(), SCHEMA, TABLE)).isEmpty();
        assertThat(unrelated.getPinnedSnapshot(session.queryId(), SCHEMA, TABLE)).hasValue(snapshotId);
        unrelatedOwner.seal();
        unrelated.cleanupQuery(unrelatedOwner.authority());
        unrelatedOwner.retire();
    }

    @Test
    void dispatchFailure_closesPinScopeWithoutCleaningAnotherQuery() {
        var failing = session("q-cleanup-2");
        var other = session("q-independent");
        var state = new QueryStateMachine();
        var authority = dispatcher.bindConnectorAuthority(state, failing.queryId());
        failing = failing.withQueryAuthority(authority);
        var connector = (IcebergMetadata) metadata.catalogRegistry()
                .queryCatalog(CATALOG, SessionContextConverter.toConnectorSession(
                        failing, CATALOG, SCHEMA)).metadata();
        pin(connector, failing.queryId());
        var otherOwner = new org.pangrova.coordinator.task.TestConnectorQuery(other.queryId());
        beginAndPin(connector, otherOwner.authority());
        var admittedSession = failing;

        assertThrows(NoAvailableWorkerException.class,
                () -> dispatcher.execute(state, SQL, admittedSession));

        assertThat(dispatcher.tryCleanupQuery(failing.queryId(), null)).isTrue();
        assertThat(connector.getPinnedSnapshot(failing.queryId(), SCHEMA, TABLE)).isEmpty();
        connector.pinSnapshotForQuery(failing.queryId(), SCHEMA, TABLE, snapshotId);
        assertThat(connector.getPinnedSnapshot(failing.queryId(), SCHEMA, TABLE)).isEmpty();
        assertThat(connector.getPinnedSnapshot(other.queryId(), SCHEMA, TABLE)).hasValue(snapshotId);
        otherOwner.seal();
        connector.cleanupQuery(otherOwner.authority());
        otherOwner.retire();
    }

    @Test
    void compileFailure_doesNotCleanUnregisteredCatalogResources() {
        var session = session("q-compile-fail");
        var first = connectorMetadata(CATALOG, session);
        var second = connectorMetadata(UNREFERENCED_CATALOG, session);
        // These scopes belong to another owner; invalid SQL registers neither with this dispatcher.
        var externalOwner = new org.pangrova.coordinator.task.TestConnectorQuery(session.queryId());
        beginAndPin(first, externalOwner.authority());
        beginAndPin(second, externalOwner.authority());
        var state = new QueryStateMachine();

        assertThrows(ParseException.class,
                () -> dispatcher.execute(state, "THIS IS NOT VALID SQL !!!", session));

        assertThat(state.getState()).isEqualTo(QueryState.PLANNING);
        assertThat(first.getPinnedSnapshot(session.queryId(), SCHEMA, TABLE)).hasValue(snapshotId);
        assertThat(second.getPinnedSnapshot(session.queryId(), SCHEMA, TABLE)).hasValue(snapshotId);
        assertThat(dispatcher.tryCleanupQuery(session.queryId(), null)).isTrue();
        assertThat(first.getPinnedSnapshot(session.queryId(), SCHEMA, TABLE)).hasValue(snapshotId);
        assertThat(second.getPinnedSnapshot(session.queryId(), SCHEMA, TABLE)).hasValue(snapshotId);
        externalOwner.seal();
        first.cleanupQuery(externalOwner.authority());
        second.cleanupQuery(externalOwner.authority());
        externalOwner.retire();
    }

    private void createSdkTable() throws Exception {
        var configuration = new Configuration();
        configuration.set("fs.file.impl", org.apache.hadoop.fs.RawLocalFileSystem.class.getName());
        sdkCatalog = new HadoopCatalog(configuration, warehouse.toString());
        sdkCatalog.createNamespace(Namespace.of(SCHEMA));
        Table table = sdkCatalog.createTable(TableIdentifier.of(SCHEMA, TABLE),
                new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
        var output = table.io().newOutputFile(table.location() + "/data/record.parquet");
        var writer = Parquet.writeData(output).schema(table.schema()).withSpec(table.spec())
                .createWriterFunc(GenericParquetWriter::create).build();
        try (writer) {
            var row = GenericRecord.create(table.schema());
            row.setField("id", 1L);
            writer.write(row);
        }
        table.newAppend().appendFile(writer.toDataFile()).commit();
        assertThat(table.currentSnapshot()).isNotNull();
        snapshotId = table.currentSnapshot().snapshotId();
    }

    private static DynamicRbacAccessControl createAuthority(DataSource dataSource) {
        var roles = new JdbcRoleStore(dataSource);
        roles.createRole(Role.of(READER));
        roles.mapGroupToRole(GroupRoleMapping.of(READER, READER));
        var grants = new JdbcGrantStore(dataSource);
        grants.addGrant(PrivilegeGrant.of(READER, Privilege.SELECT, "TABLE", CATALOG, SCHEMA, TABLE, null));
        return new DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(new JdbcRowPolicyStore(dataSource)),
                new ColumnMaskingProcessor(new JdbcColumnMaskStore(dataSource)));
    }

    private IcebergMetadata connectorMetadata(String catalog, QuerySession session) {
        var scope = SessionContextConverter.toConnectorSession(session, catalog, SCHEMA);
        return (IcebergMetadata) metadata.catalogRegistry().getCatalog(catalog).getMetadata(scope);
    }

    private void beginAndPin(IcebergMetadata connector,
            org.pangrova.connector.api.query.ConnectorQueryAuthority authority) {
        connector.beginQuery(authority);
        pin(connector, authority.queryId());
    }

    private void pin(IcebergMetadata connector, String queryId) {
        connector.pinSnapshotForQuery(queryId, SCHEMA, TABLE, snapshotId);
        assertThat(connector.getPinnedSnapshot(queryId, SCHEMA, TABLE)).hasValue(snapshotId);
    }

    private static QuerySession session(String queryId) {
        return new QuerySession(queryId, Map.of("catalog", CATALOG, "schema", SCHEMA),
                new UserIdentity("cleanup-user", Set.of(READER), Map.of(), Instant.EPOCH), "cleanup-test-tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
    }
}
