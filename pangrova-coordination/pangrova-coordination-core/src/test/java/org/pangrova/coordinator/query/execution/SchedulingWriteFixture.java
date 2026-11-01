package org.pangrova.coordinator.query.execution;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.pangrova.connector.iceberg.IcebergMetadata;
import org.pangrova.connector.iceberg.handle.IcebergOutputHandleCodec;
import org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.coordinator.query.execution.security.SecurityPolicyApplier;
import org.pangrova.coordinator.query.security.AccessControlEnforcer;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.catalog.handle.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.store.jdbc.catalog.JdbcMetadataStore;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.security.ColumnMaskApplier;
import org.pangrova.planner.security.RowFilterApplier;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.metadata.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.GeneratedColumnDefinition;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.spi.security.authz.DeferredAuthorizationScope;
import org.pangrova.persistence.jdbc.security.codec.DeferredAuthorizationScopeCodec;
import org.pangrova.security.authz.DynamicRbacAccessControl;
import org.pangrova.spi.security.policy.Privilege;
import org.pangrova.spi.security.policy.PrivilegeGrant;
import org.pangrova.spi.security.policy.Role;
import org.pangrova.spi.security.policy.UserRoleAssignment;
import org.pangrova.spi.security.mask.ColumnMask;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.spi.security.rowfilter.RowPolicy;
import org.pangrova.persistence.jdbc.security.mask.JdbcColumnMaskStore;
import org.pangrova.persistence.jdbc.security.policy.JdbcGrantStore;
import org.pangrova.persistence.jdbc.security.policy.JdbcRoleStore;
import org.pangrova.persistence.jdbc.security.rowfilter.JdbcRowPolicyStore;
import org.pangrova.spi.connector.plugin.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputHandleCodec;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.coordination.WriteAuthorizationReceipt;
import org.pangrova.spi.connector.coordination.WriteDeclarationSnapshot;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Real SDK allocation and serialization; the leaf catalog router is not an HTTP or Native fixture. */
final class SchedulingWriteFixture implements AutoCloseable {
    static final QualifiedTableName TABLE = new QualifiedTableName("catalog", "schema", "documents");
    final NativeWriteCommitTracker tracker = new NativeWriteCommitTracker();
    final CountingMetadata metadata;
    final QuerySchedulingEngine engine;
    final CatalogRegistry registry;
    final List<GeneratedWriteAuthoritySupplier.WriteAuthorityRequest> observations =
            new java.util.concurrent.CopyOnWriteArrayList<>();
    private final HadoopCatalog catalog;
    private final CatalogHandle catalogHandle;
    private final boolean generated;

    SchedulingWriteFixture(Path warehouse, boolean generated) {
        this.generated = generated;
        catalog = new HadoopCatalog(new Configuration(), warehouse.toString());
        catalog.createNamespace(Namespace.of("schema"));
        var columns = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "title", Types.StringType.get()),
                Types.NestedField.optional(3, "embedding", Types.ListType.ofOptional(4, Types.FloatType.get())));
        catalog.createTable(TableIdentifier.of("schema", "documents"), columns);
        catalog.createTable(TableIdentifier.of("schema", "other"), columns);
        if (generated) {
            var update = catalog.loadTable(TableIdentifier.of("schema", "documents")).updateProperties();
            GeneratedColumnMetadataCodec.encode(generatedDefinition()).forEach(update::set);
            update.commit();
        }
        metadata = new CountingMetadata(catalog);
        var descriptor = new CatalogDescriptor("catalog", CatalogType.CONNECTOR, "iceberg", CatalogState.ACTIVE,
                Map.of("warehouse", warehouse.toString()), "", Set.of("tenant-1"));
        var handle = new ConnectorCatalogHandle(descriptor, new MetadataConnector(metadata));
        catalogHandle = handle;
        registry = new SingleCatalogRegistry(handle);
        engine = new QuerySchedulingEngine(new QuerySchedulingEngine.SchedulingDependencies(
                null, null, null, null, registry, null, null, 0));
        engine.setWriteCommitTracker(tracker);
    }

    void enforceOwnership() {
        registry.setTableControlService(new org.pangrova.table.control.TableControlService(
                new org.pangrova.table.store.memory.control.InMemoryTableControlStore(), registry::requireCurrentCatalogIdentity,
                new org.pangrova.table.control.TableControlSettings(true, true, Set.of())));
    }

    void installOrdinaryAuthority() {
        engine.setGeneratedWriteAuthoritySupplier(request -> {
            observations.add(request);
            return receipt(request);
        });
    }

    void evolveActualSchema() {
        catalog.loadTable(TableIdentifier.of("schema", "documents")).updateSchema()
                .addColumn("new_column", Types.StringType.get()).commit();
    }

    WritePolicyObservation compileProtectedInsert(CoordinatorSessionContext actor) throws Exception {
        String sql = generated
                ? "INSERT INTO catalog.schema.documents (id, title) SELECT id, title FROM catalog.schema.other"
                : "INSERT INTO catalog.schema.documents SELECT id, title, embedding FROM catalog.schema.other";
        return compileProtectedWrite(actor, sql);
    }

    WritePolicyObservation compileProtectedWrite(CoordinatorSessionContext actor, String sql) throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("write_read_binding");
        var roles = new JdbcRoleStore(dataSource);
        roles.initSchema();
        roles.createRole(Role.of("reader"));
        roles.grantRoleToUser(UserRoleAssignment.of(actor.userIdentity().principal(), "reader"));
        var grants = new JdbcGrantStore(dataSource);
        grants.addGrant(PrivilegeGrant.of("reader", Privilege.SELECT, "TABLE", "catalog", "schema", "other", null));
        var policies = new JdbcRowPolicyStore(dataSource);
        policies.savePolicy(new RowPolicy(0, "positive-id", "catalog", "schema", "other",
                "id > 0", List.of("reader"), true, 1, null));
        var masks = new JdbcColumnMaskStore(dataSource);
        masks.saveColumnMask(new ColumnMask(0, "hide-title", "catalog", "schema", "other", "title",
                "'masked'", List.of("reader"), List.of(), true, null));
        var authority = new DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(policies), new ColumnMaskingProcessor(masks));
        var directory = new JdbcMetadataStore(dataSource);
        directory.saveCatalogDescriptor(catalogHandle.getDescriptor());
        var bootstrap = MetadataBootstrap.initialize(directory, ignored -> catalogHandle);
        try {
            var schema = bootstrap.buildCalciteSchema(SessionContextConverter.toConnectorSession(actor));
            var compiler = new SqlCompiler(new PangrovaPlanner(schema, List.of("catalog", "schema")),
                    new SubstraitConverter(), new SubstraitPlanSerializer());
            var security = new SecurityPolicyApplier(compiler, new AccessControlEnforcer(authority),
                    new ColumnMaskApplier(authority), new RowFilterApplier(authority));
            var observed = security.applyPoliciesObserved(compiler.compileToPlannedQuery(sql), actor);
            return WritePolicyObservation.capture(observed, compiler, compiler.parseToAst(sql));
        } finally {
            bootstrap.shutdown();
        }
    }

    WriteDeclarationSnapshot declaration() {
        var session = SessionContextConverter.toConnectorSession(new CoordinatorSessionContext(
                "fixture-compilation", Map.of(), new org.pangrova.spi.security.UserIdentity(
                        "alice", Set.of(), Map.of(), java.time.Instant.EPOCH), "tenant-1"));
        var handle = metadata.getTableHandle(session, new SchemaTableName(TABLE.schema(), TABLE.table()));
        var actual = metadata.getTableMetadata(session, handle);
        var definitions = GeneratedColumnMetadataCodec.decode(actual.properties());
        return WriteDeclarationSnapshot.capture(TABLE, actual, definitions.toByteArray(),
                definitions.getDefinitionsCount() != 0);
    }

    DmlWriteContext context() {
        return new DmlWriteContext(TABLE, "INSERT", List.of(),
                Optional.of(new WritePolicyObservation(List.of(), Set.of(Privilege.INSERT), declaration())));
    }

    void evolveActualDeclaration() {
        var update = catalog.loadTable(TableIdentifier.of("schema", "documents")).updateProperties();
        GeneratedColumnMetadataCodec.encode(generatedDefinition()).forEach(update::set);
        update.commit();
    }

    WriteAuthorizationReceipt receipt(GeneratedWriteAuthoritySupplier.WriteAuthorityRequest request) {
        var table = metadata.getTableHandle(request.session(),
                new SchemaTableName(request.table().schema(), request.table().table()));
        var actual = metadata.getTableMetadata(request.session(), table);
        var definitions = GeneratedColumnMetadataCodec.decode(actual.properties());
        if (definitions.getDefinitionsCount() != 0) {
            throw new IllegalArgumentException("This ordinary allocation fixture does not authorize generated models");
        }
        var declaration = WriteDeclarationSnapshot.capture(request.table(), actual, definitions.toByteArray(), false);
        var permissions = new ArrayList<>(request.observedReadPermissions());
        request.requiredWritePrivileges().forEach(privilege -> permissions.add(
                new DeferredAuthorizationScope.TablePermission(request.table(), Set.of(), privilege,
                        DeferredAuthorizationScope.ReadMode.NONE, DeferredAuthorizationScope.PolicySnapshot.empty())));
        return DeferredAuthorizationScopeCodec.encodeWriteReceipt(new DeferredAuthorizationScope(permissions,
                Optional.of(new DeferredAuthorizationScope.DefinitionSnapshot(declaration, List.of()))));
    }

    private static GeneratedColumnSet generatedDefinition() {
        return GeneratedColumnSet.newBuilder().setSchemaVersion(GeneratedColumnMetadataCodec.SCHEMA_VERSION)
                .addDefinitions(GeneratedColumnDefinition.newBuilder().setTargetColumn("embedding")
                        .setCapability(BuiltInCapabilityCatalog.aiEmbeddingPlanRef()).setLogicalModelName("model")
                        .addSourceColumns("title").setExecutionMode(ExecutionMode.EXECUTION_MODE_ASYNC)
                        .setOutputType(VectorLogicalType.create(768))).build();
    }

    @Override public void close() throws IOException { catalog.close(); }

    static final class CountingMetadata extends IcebergMetadata {
        int inserts;
        int overwrites;
        int partitionOverwrites;
        Map<String, String> partition = Map.of();
        boolean failOverwrite;
        Runnable beforeInsert = () -> { };
        org.pangrova.spi.connector.metadata.table.TableCapabilities capabilities =
                org.pangrova.connector.iceberg.handle.IcebergTableHandle.CAPABILITIES;

        CountingMetadata(HadoopCatalog catalog) { super(catalog, "catalog"); }

        @Override
        public org.pangrova.spi.connector.metadata.table.TableCapabilities getTableCapabilities(
                ConnectorSession session, ConnectorTableHandle table) {
            return capabilities;
        }

        @Override
        public ConnectorOutputTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle table) {
            inserts++;
            beforeInsert.run();
            return super.beginInsert(session, table);
        }

        @Override
        public ConnectorOutputTableHandle beginOverwrite(ConnectorSession session, ConnectorTableHandle table) {
            overwrites++;
            if (failOverwrite) throw new IllegalStateException("Actual allocation boundary rejected overwrite");
            return super.beginOverwrite(session, table);
        }

        @Override
        public ConnectorOutputTableHandle beginOverwrite(ConnectorSession session, ConnectorTableHandle table,
                Map<String, String> staticPartition) {
            partitionOverwrites++;
            partition = Map.copyOf(staticPartition);
            return beginOverwrite(session, table);
        }
    }

    private record MetadataConnector(CountingMetadata metadata) implements Connector {
        @Override public ConnectorMetadata getMetadata(ConnectorSession session) { return metadata; }
        @Override public ConnectorOutputHandleCodec getOutputHandleCodec() { return new IcebergOutputHandleCodec(); }
        @Override public ConnectorSplitManager getSplitManager() { throw new AssertionError("No dispatch in this unit"); }
        @Override public ConnectorPageSourceProvider getPageSourceProvider() {
            throw new AssertionError("Coordinator allocation must not read rows");
        }
    }

    private static final class SingleCatalogRegistry extends CatalogRegistry {
        private final CatalogHandle handle;

        SingleCatalogRegistry(CatalogHandle handle) {
            super(null);
            this.handle = handle;
        }
        @Override public CatalogHandle getCatalog(String name) {
            return name.equals("catalog") ? handle : null;
        }
        @Override public org.pangrova.table.api.control.CatalogDeploymentIdentity requireCurrentCatalogIdentity(String name) {
            var deployment = getCatalog(name).getDescriptor();
            return new org.pangrova.table.api.control.CatalogDeploymentIdentity(
                    deployment.incarnationId(), deployment.revision());
        }
    }
}
