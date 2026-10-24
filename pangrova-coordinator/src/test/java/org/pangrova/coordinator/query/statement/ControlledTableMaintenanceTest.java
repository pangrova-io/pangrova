package org.pangrova.coordinator.query.statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.audit.TableControlAuditEmitter;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.metadata.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.control.InMemoryTableControlMaintenanceStore;
import org.pangrova.metadata.control.InMemoryTableControlStore;
import org.pangrova.metadata.control.MaintenanceOperation;
import org.pangrova.metadata.control.MaintenanceRunState;
import org.pangrova.metadata.control.TableControlMaintenanceService;
import org.pangrova.metadata.control.TableControlRecord;
import org.pangrova.metadata.control.TableControlService;
import org.pangrova.metadata.control.TableControlSettings;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.sync.SyncResult;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.security.audit.AuditEvent;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.security.audit.AuditEventType;
import org.pangrova.security.audit.AuditEventWriter;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableIdentity;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.spi.connector.metadata.table.TableVersionToken;
import org.pangrova.spi.coordination.DistributedLease;
import org.pangrova.spi.coordination.LeaseEntry;
import org.pangrova.spi.security.AccessDeniedException;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.coordinator.query.statement.StatementTestSessions.context;

class ControlledTableMaintenanceTest {

    private static final Instant NOW = Instant.parse("2026-05-01T00:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    private TestMetadata metadata;
    private CatalogRegistry registry;
    private TableControlMaintenanceService policies;
    private TestLease lease;
    private ControlledTableMaintenance maintenance;
    private AuditEventEmitter auditEmitter;

    @BeforeEach
    void setUp() {
        metadata = new TestMetadata();
        metadata.create("default", "orders", "uuid-orders");
        metadata.create("default", "external", "uuid-external");
        ConnectorHandleFactory factory = descriptor ->
                new ConnectorCatalogHandle(descriptor, new TestConnector(metadata));
        registry = new CatalogRegistry(new EmptyMetadataStore(), factory, null, null);
        registry.register(new CatalogDescriptor(
                "test_cat", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));
        registry.setTableControlService(new TableControlService(
                new InMemoryTableControlStore(),
                new TableControlSettings(true, true, Set.of())));
        policies = new TableControlMaintenanceService(
                new InMemoryTableControlMaintenanceStore(), CLOCK);
        lease = new TestLease();
        maintenance = new ControlledTableMaintenance(
                registry, policies, lease, "coordinator-1", Duration.ofMinutes(5), CLOCK);
    }

    @AfterEach
    void tearDown() {
        if (auditEmitter != null) {
            auditEmitter.close();
        }
    }

    @Test
    void observedTablesAreRejectedBeforeTheConnectorRuns() {
        assertThatThrownBy(() -> maintenance.execute(MaintenanceOperation.COMPACT,
                Identifier.of("test_cat", "default", "orders"), context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("OBSERVED");
        assertThat(metadata.procedureCalls).isEmpty();
    }

    @Test
    void linkedTablesNeedAnExplicitOperationPolicy() {
        adopt("external");

        assertThatThrownBy(() -> maintenance.execute(MaintenanceOperation.COMPACT,
                Identifier.of("test_cat", "default", "external"), context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("not enabled");
        assertThat(metadata.procedureCalls).isEmpty();
    }

    @Test
    void linkedTableRunsAnAuthorizedOperationAndRecordsItByTableId() {
        TableControlRecord record = adopt("external");
        policies.applyAdoptionProperties(record,
                Map.of("maintenance_operations", "COMPACT"), "alice");

        List<TypedRow> rows = maintenance.execute(MaintenanceOperation.COMPACT,
                Identifier.of("test_cat", "default", "external"), context());

        assertThat(metadata.procedureCalls)
                .containsExactly("compact|default.external");
        assertThat(rows).hasSize(1);
        var runs = policies.listRuns(record.tableId(), 10);
        assertThat(runs).hasSize(1);
        assertThat(runs.get(0).state()).isEqualTo(MaintenanceRunState.SUCCEEDED);
        assertThat(runs.get(0).versionBefore()).isEqualTo("uuid:v1");
        assertThat(runs.get(0).actor()).isEqualTo("statement-user");
        assertThat(lease.released).isTrue();
    }

    @Test
    void anActiveExternalWriterAbandonsTheRun() {
        TableControlRecord record = adopt("external");
        policies.applyAdoptionProperties(record,
                Map.of("maintenance_operations", "COMPACT"), "alice");
        metadata.version = sequence("v1", "v2");

        assertThatThrownBy(() -> maintenance.execute(MaintenanceOperation.COMPACT,
                Identifier.of("test_cat", "default", "external"), context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("external writer");
        assertThat(metadata.procedureCalls).isEmpty();
        var runs = policies.listRuns(record.tableId(), 10);
        assertThat(runs.get(0).state()).isEqualTo(MaintenanceRunState.SKIPPED);
        assertThat(lease.released).isTrue();
    }

    @Test
    void aBusyLeaseRejectsMaintenance() {
        TableControlRecord record = adopt("external");
        policies.applyAdoptionProperties(record,
                Map.of("maintenance_operations", "COMPACT"), "alice");
        lease.busy = true;

        assertThatThrownBy(() -> maintenance.execute(MaintenanceOperation.COMPACT,
                Identifier.of("test_cat", "default", "external"), context()))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("already in progress");
        assertThat(metadata.procedureCalls).isEmpty();
        assertThat(policies.listRuns(record.tableId(), 10).get(0).state())
                .isEqualTo(MaintenanceRunState.SKIPPED);
    }

    @Test
    void aCompletedRunIsAuditedWithItsOutcomeAndVersion() {
        TableControlRecord record = adopt("external");
        policies.applyAdoptionProperties(record,
                Map.of("maintenance_operations", "COMPACT"), "alice");
        RecordingAuditWriter writer = new RecordingAuditWriter();
        auditEmitter = new AuditEventEmitter(writer);
        maintenance.setTableControlAudit(new TableControlAuditEmitter(auditEmitter));

        maintenance.execute(MaintenanceOperation.COMPACT,
                Identifier.of("test_cat", "default", "external"), context());

        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> writer.events.stream().anyMatch(event ->
                        event.eventType() == AuditEventType.TABLE_MAINTENANCE));
        AuditEvent event = writer.events.stream()
                .filter(e -> e.eventType() == AuditEventType.TABLE_MAINTENANCE)
                .findFirst().orElseThrow();
        assertThat(event.success()).isTrue();
        assertThat(event.properties())
                .containsEntry("table_id", record.tableId().toString())
                .containsEntry("operation", "COMPACT")
                .containsEntry("state", "SUCCEEDED")
                .containsEntry("version_before", "uuid:v1");
    }

    @Test
    void managedVacuumUsesThePolicyOrphanAge() {
        registry.tableControl().registerCreated("test_cat", "default", "orders", "alice",
                Optional.of(identity("orders")), Optional.of(version()));

        maintenance.execute(MaintenanceOperation.VACUUM,
                Identifier.of("test_cat", "default", "orders"), context());

        assertThat(metadata.procedureCalls)
                .containsExactly("remove_orphan_files|default.orders|"
                        + NOW.minus(Duration.ofDays(3)).toEpochMilli());
    }

    private TableControlRecord adopt(String table) {
        return registry.tableControl().adopt("test_cat", "default", table, "alice",
                Optional.of(identity(table)), Optional.of(version()));
    }

    private static TableIdentity identity(String table) {
        return new TableIdentity("ICEBERG", "uuid-" + table, "s3://bucket/" + table, "schema");
    }

    private static TableVersionToken version() {
        return TableVersionToken.of("ICEBERG", "uuid", "v1");
    }

    private static Supplier<Optional<TableVersionToken>> sequence(String first, String second) {
        AtomicInteger calls = new AtomicInteger();
        return () -> Optional.of(TableVersionToken.of("ICEBERG", "uuid",
                calls.getAndIncrement() == 0 ? first : second));
    }

    private static final class TestConnector implements Connector {
        private final ConnectorMetadata metadata;

        TestConnector(ConnectorMetadata metadata) {
            this.metadata = metadata;
        }

        @Override public ConnectorMetadata getMetadata(ConnectorSession s) { return metadata; }
        @Override public ConnectorSplitManager getSplitManager() { return null; }
        @Override public ConnectorPageSourceProvider getPageSourceProvider() { return null; }
    }

    private static final class TestMetadata implements ConnectorMetadata {

        private final Map<String, ConnectorTableMetadata> tables = new LinkedHashMap<>();
        private final Map<String, String> uuids = new LinkedHashMap<>();
        private final List<String> procedureCalls = new ArrayList<>();
        private Supplier<Optional<TableVersionToken>> version =
                () -> Optional.of(TableVersionToken.of("ICEBERG", "uuid", "v1"));

        void create(String schema, String table, String uuid) {
            SchemaTableName name = new SchemaTableName(schema, table);
            tables.put(key(name), new ConnectorTableMetadata(name, List.of("id"), Map.of()));
            uuids.put(key(name), uuid);
        }

        private static String key(SchemaTableName name) {
            return name.schema() + "." + name.table();
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName name) {
            return tables.containsKey(key(name)) ? new Handle(key(name)) : null;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session,
                ConnectorTableHandle handle) {
            return tables.get(((Handle) handle).key);
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
                ConnectorTableHandle handle) {
            return Map.of();
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session,
                ConnectorTableHandle handle) {
            return TableStatistics.UNKNOWN;
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of("default");
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schema) {
            return tables.values().stream().map(ConnectorTableMetadata::table).toList();
        }

        @Override
        public Optional<TableIdentity> getTableIdentity(ConnectorSession session,
                ConnectorTableHandle handle) {
            String key = ((Handle) handle).key;
            return Optional.of(new TableIdentity(
                    "ICEBERG", uuids.get(key), "s3://bucket/" + key, "schema"));
        }

        @Override
        public Optional<TableVersionToken> observeVersion(ConnectorSession session,
                ConnectorTableHandle handle) {
            return version.get();
        }

        @Override
        public List<TypedRow> executeProcedure(ConnectorSession session, String procedureName,
                Map<String, String> arguments) {
            procedureCalls.add(procedureName + "|" + arguments.get("arg0")
                    + (arguments.containsKey("arg1") ? "|" + arguments.get("arg1") : ""));
            return List.of(TypedRow.builder()
                    .putString("result", procedureName + " succeeded")
                    .putString("table", arguments.get("arg0"))
                    .build());
        }
    }

    private record Handle(String key) implements ConnectorTableHandle { }

    private static final class RecordingAuditWriter implements AuditEventWriter {
        private final List<AuditEvent> events =
                new java.util.concurrent.CopyOnWriteArrayList<>();

        @Override
        public void write(AuditEvent event) {
            events.add(event);
        }
    }

    private static final class TestLease implements DistributedLease {
        private final Map<String, LeaseEntry> active = new HashMap<>();
        private boolean busy;
        private boolean released;
        private long epoch = 1;

        @Override
        public Optional<LeaseEntry> tryAcquire(String leaseKey, String holderId, Duration duration) {
            if (busy) {
                return Optional.empty();
            }
            LeaseEntry entry = new LeaseEntry(leaseKey, holderId, epoch++, NOW, NOW.plus(duration));
            active.put(leaseKey, entry);
            return Optional.of(entry);
        }

        @Override
        public boolean tryRenew(String leaseKey, String holderId, long epoch, Duration duration) {
            return false;
        }

        @Override
        public boolean release(String leaseKey, String holderId, long epoch) {
            LeaseEntry entry = active.remove(leaseKey);
            released = entry != null;
            return released;
        }

        @Override
        public Optional<LeaseEntry> currentHolder(String leaseKey) {
            return Optional.ofNullable(active.get(leaseKey));
        }
    }

    private static final class EmptyMetadataStore implements MetadataStore {
        @Override public void saveTable(TableMetadata m) { }
        @Override public Optional<TableMetadata> loadTable(String c, String s, String t) { return Optional.empty(); }
        @Override public void deleteTable(String c, String s, String t) { }
        @Override public java.util.List<QualifiedTableRef> searchTables(String p) { return java.util.List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) { }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) { return Optional.empty(); }
        @Override public void deleteCatalogDescriptor(String n) { }
        @Override public java.util.List<CatalogDescriptor> listAllCatalogDescriptors() { return java.util.List.of(); }
        @Override public Optional<Instant> getLastSyncTime(String c) { return Optional.empty(); }
        @Override public void updateSyncTime(String c, Instant t) { }
        @Override public void recordSyncHistory(SyncResult r) { }
        @Override public void saveSchemas(String c, java.util.List<String> s) { }
    }
}
