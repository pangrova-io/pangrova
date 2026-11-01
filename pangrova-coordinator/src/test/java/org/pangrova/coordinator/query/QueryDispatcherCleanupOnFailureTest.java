package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
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
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.scheduler.failure.NoAvailableWorkerException;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.store.jdbc.JdbcMetadataStore;
import org.pangrova.parser.ParseException;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.security.ColumnMaskApplier;
import org.pangrova.planner.security.RowFilterApplier;
import org.pangrova.security.authz.DynamicRbacAccessControl;
import org.pangrova.security.authz.GroupRoleMapping;
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.Role;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.secret.SecretResolver;
import org.pangrova.security.store.PersistentColumnMaskStore;
import org.pangrova.security.store.PersistentGrantStore;
import org.pangrova.security.store.PersistentRoleStore;
import org.pangrova.security.store.PersistentRowPolicyStore;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.testing.TestObservability;
import org.pangrova.testing.ContainerTestBase;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        var store = new JdbcMetadataStore(dataSource);
        for (String name : List.of(CATALOG, UNREFERENCED_CATALOG)) {
            store.saveCatalogDescriptor(new CatalogDescriptor(name, CatalogType.CONNECTOR, "iceberg",
                    CatalogState.ACTIVE, Map.of("warehouse", warehouse.toString(), "type", "hadoop"),
                    "", Set.of()));
        }
        connectors = new PluginConnectorHandleFactory(getClass().getClassLoader(),
                new SecretResolver(), ignored -> Optional.empty());
        metadata = MetadataBootstrap.initialize(store, connectors);
        var compiler = new SqlCompiler().withSessionPlanner(session ->
                new PangrovaPlanner(metadata.buildCalciteSchema(session), List.of(CATALOG, SCHEMA)));
        remoteDispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        var authority = createAuthority(dataSource);
        var context = QueryDispatcherContext.withDefaults(compiler,
                new AccessControlEnforcer(authority), new ColumnMaskApplier(authority), new RowFilterApplier(authority),
                remoteDispatcher, new TaskScheduler(new WorkerPerformanceScorer()),
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
        var registered = connectorMetadata(CATALOG, session);
        var unrelated = connectorMetadata(UNREFERENCED_CATALOG, session);
        authority.beginMetadata(CATALOG, () -> registered);
        pin(registered, session.queryId());
        var unrelatedOwner = new org.pangrova.coordinator.task.TestConnectorQuery(session.queryId());
        beginAndPin(unrelated, unrelatedOwner.authority());
        var admittedSession = session;

        assertThrows(NoAvailableWorkerException.class, () -> dispatcher.execute(state, SQL, admittedSession));

        // QueryManager publishes the failure, then claims the physical cleanup.
        assertThat(state.getState()).isEqualTo(QueryStateMachine.State.SCHEDULING);
        dispatcher.cleanupQuery(session.queryId());
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
        var connector = connectorMetadata(CATALOG, failing);
        authority.beginMetadata(CATALOG, () -> connector);
        pin(connector, failing.queryId());
        var otherOwner = new org.pangrova.coordinator.task.TestConnectorQuery(other.queryId());
        beginAndPin(connector, otherOwner.authority());
        var admittedSession = failing;

        assertThrows(NoAvailableWorkerException.class,
                () -> dispatcher.execute(state, SQL, admittedSession));

        dispatcher.cleanupQuery(failing.queryId());
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

        assertThat(state.getState()).isEqualTo(QueryStateMachine.State.PLANNING);
        assertThat(first.getPinnedSnapshot(session.queryId(), SCHEMA, TABLE)).hasValue(snapshotId);
        assertThat(second.getPinnedSnapshot(session.queryId(), SCHEMA, TABLE)).hasValue(snapshotId);
        dispatcher.cleanupQuery(session.queryId());
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
        var roles = new PersistentRoleStore(dataSource);
        roles.initSchema();
        roles.createRole(Role.of(READER));
        roles.mapGroupToRole(GroupRoleMapping.of(READER, READER));
        var grants = new PersistentGrantStore(dataSource);
        grants.addGrant(PrivilegeGrant.of(READER, Privilege.SELECT, "TABLE", CATALOG, SCHEMA, TABLE, null));
        return new DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(new PersistentRowPolicyStore(dataSource)),
                new ColumnMaskingProcessor(new PersistentColumnMaskStore(dataSource)));
    }

    private IcebergMetadata connectorMetadata(String catalog, CoordinatorSessionContext session) {
        var scope = SessionContextConverter.toConnectorSession(session, catalog, SCHEMA);
        return (IcebergMetadata) metadata.catalogRegistry().getCatalog(catalog).getMetadata(scope);
    }

    private void beginAndPin(IcebergMetadata connector,
            org.pangrova.spi.connector.ConnectorQueryAuthority authority) {
        connector.beginQuery(authority);
        pin(connector, authority.queryId());
    }

    private void pin(IcebergMetadata connector, String queryId) {
        connector.pinSnapshotForQuery(queryId, SCHEMA, TABLE, snapshotId);
        assertThat(connector.getPinnedSnapshot(queryId, SCHEMA, TABLE)).hasValue(snapshotId);
    }

    private static CoordinatorSessionContext session(String queryId) {
        return new CoordinatorSessionContext(queryId, Map.of("catalog", CATALOG, "schema", SCHEMA),
                new UserIdentity("cleanup-user", Set.of(READER), Map.of(), Instant.EPOCH), "cleanup-test-tenant");
    }
}
