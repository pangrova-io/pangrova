package org.pangrova.coordinator.query.statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.audit.TableControlAuditEmitter;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.core.catalog.handle.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.core.catalog.handle.connector.ConnectorHandleFactory;
import org.pangrova.table.store.memory.control.InMemoryTableControlMaintenanceStore;
import org.pangrova.table.store.memory.control.InMemoryTableControlStore;
import org.pangrova.table.api.maintenance.MaintenanceOperation;
import org.pangrova.table.api.maintenance.MaintenanceRunState;
import org.pangrova.table.core.maintenance.TableControlMaintenanceService;
import org.pangrova.table.api.control.TableControlRecord;
import org.pangrova.table.core.control.TableControlService;
import org.pangrova.table.core.control.TableControlSettings;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.catalog.MetadataStore;
import org.pangrova.metadata.api.catalog.SyncResult;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.spi.security.audit.AuditEvent;
import org.pangrova.security.core.audit.AuditEventEmitter;
import org.pangrova.spi.security.audit.AuditEventType;
import org.pangrova.spi.security.audit.AuditEventWriter;
import org.pangrova.spi.connector.plugin.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableIdentity;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.spi.connector.metadata.table.TableVersionToken;
import org.pangrova.persistence.api.coordination.DistributedLease;
import org.pangrova.spi.coordination.LeaseEntry;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.table.api.maintenance.TableMaintenanceRun;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
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
                new InMemoryTableControlStore(), registry::requireCurrentCatalogIdentity,
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
    void replacedTableCannotInheritMaintenanceAuthorizationEvenWithTheSameVersion() {
        TableControlRecord record = adopt("external");
        policies.applyAdoptionProperties(record,
                Map.of("maintenance_operations", "COMPACT"), "alice");
        metadata.create("default", "external", "replacement");

        assertThatThrownBy(() -> maintenance.execute(MaintenanceOperation.COMPACT,
                Identifier.of("test_cat", "default", "external"), context()))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("identity");
        assertThat(metadata.procedureCalls).isEmpty();
        assertThat(policies.listRuns(record.tableId(), 10).get(0).state())
                .isEqualTo(MaintenanceRunState.SKIPPED);
        assertThat(lease.released).isTrue();
    }

    @Test
    void identityChangeDuringExecutionIsAFailedRunRatherThanASkippedOrSuccessfulRun() {
        TableControlRecord record = adopt("external");
        policies.applyAdoptionProperties(record,
                Map.of("maintenance_operations", "COMPACT"), "alice");
        metadata.onExecute = () -> metadata.create("default", "external", "replacement");

        assertThatThrownBy(() -> maintenance.execute(MaintenanceOperation.COMPACT,
                Identifier.of("test_cat", "default", "external"), context()))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("identity");
        assertThat(metadata.procedureCalls).hasSize(1);
        assertThat(policies.listRuns(record.tableId(), 10).get(0).state())
                .isEqualTo(MaintenanceRunState.FAILED);
        assertThat(registry.tableControl().find("test_cat", "default", "external").orElseThrow().state())
                .isEqualTo(org.pangrova.table.api.control.TableControlState.QUARANTINED);
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
    void leaseLossBeforeExecutionSkipsTheConnector() {
        var record = registry.tableControl().registerCreated("test_cat", "default", "orders", "alice",
                Optional.of(identity("orders")), Optional.of(version())).orElseThrow();
        lease.renewalAllowed = false;
        assertThatThrownBy(() -> maintenance.execute(MaintenanceOperation.COMPACT,
                Identifier.of("test_cat", "default", "orders"), context()))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("lease lost");
        assertThat(metadata.procedureCalls).isEmpty();
        assertThat(policies.listRuns(record.tableId(), 10)).singleElement()
                .satisfies(run -> assertThat(run.state()).isEqualTo(MaintenanceRunState.SKIPPED));
    }

    @Test
    void leaseLossDuringExecutionCannotBeReportedAsSuccess() {
        var record = registry.tableControl().registerCreated("test_cat", "default", "orders", "alice",
                Optional.of(identity("orders")), Optional.of(version())).orElseThrow();
        metadata.onExecute = () -> lease.renewalAllowed = false;
        assertThatThrownBy(() -> maintenance.execute(MaintenanceOperation.COMPACT,
                Identifier.of("test_cat", "default", "orders"), context()))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("lease lost");
        assertThat(metadata.procedureCalls).hasSize(1);
        assertThat(policies.listRuns(record.tableId(), 10)).singleElement()
                .satisfies(run -> assertThat(run.state()).isEqualTo(MaintenanceRunState.FAILED));
    }

    @Test
    void leaseIsRenewedWhileTheConnectorIsStillExecuting() {
        registry.tableControl().registerCreated("test_cat", "default", "orders", "alice",
                Optional.of(identity("orders")), Optional.of(version()));
        metadata.onExecute = () -> {
            int before = lease.renewals.get();
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5))
                    .until(() -> lease.renewals.get() >= before + 2);
        };
        var executor = new ControlledTableMaintenance(
                registry, policies, lease, "renewing", Duration.ofMillis(30), CLOCK);
        executor.execute(MaintenanceOperation.COMPACT,
                Identifier.of("test_cat", "default", "orders"), context());
        assertThat(lease.released).isTrue();
    }

    @Test
    void requestsOnTheSameCoordinatorCannotReenterAnotherAttemptsLease() {
        var record = registry.tableControl().registerCreated("test_cat", "default", "orders", "alice",
                Optional.of(identity("orders")), Optional.of(version())).orElseThrow();
        var realLease = new org.pangrova.persistence.memory.coordination.InMemoryDistributedLease(CLOCK);
        var executor = new ControlledTableMaintenance(
                registry, policies, realLease, "same-coordinator", Duration.ofMinutes(5), CLOCK);
        var nested = new java.util.concurrent.atomic.AtomicBoolean();
        metadata.onExecute = () -> {
            if (nested.compareAndSet(false, true)) {
                assertThatThrownBy(() -> executor.execute(MaintenanceOperation.COMPACT,
                        Identifier.of("test_cat", "default", "orders"), context()))
                        .hasMessageContaining("already in progress");
            }
        };

        executor.execute(MaintenanceOperation.COMPACT,
                Identifier.of("test_cat", "default", "orders"), context());

        assertThat(metadata.procedureCalls).hasSize(1);
        assertThat(policies.listRuns(record.tableId(), 10)).extracting(TableMaintenanceRun::state)
                .containsExactlyInAnyOrder(MaintenanceRunState.SUCCEEDED, MaintenanceRunState.SKIPPED);
        assertThat(realLease.currentHolder("table-maintenance:" + record.tableId())).isEmpty();
    }

    @Test
    void procedureCannotWeakenRetentionOrPassUnrecognizedArguments() {
        registry.tableControl().registerCreated("test_cat", "default", "orders", "alice",
                Optional.of(identity("orders")), Optional.of(version()));
        var ref = new org.pangrova.spi.connector.metadata.table.QualifiedTableName(
                "test_cat", "default", "orders");
        assertThatThrownBy(() -> maintenance.executeProcedure("expire_snapshots", ref,
                Map.of("arg0", "default.orders", "arg1", "1"), context()))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("retention policy");
        assertThatThrownBy(() -> maintenance.executeProcedure("remove_orphan_files", ref,
                Map.of("arg0", "default.orders", "arg1", String.valueOf(NOW.toEpochMilli())), context()))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("retention policy");
        assertThatThrownBy(() -> maintenance.executeProcedure("compact", ref,
                Map.of("arg0", "default.orders", "arg1", "ignored"), context()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("at most 1");
        assertThat(metadata.procedureCalls).isEmpty();
    }

    @Test
    void procedureWithStrongerRetentionExecutesUnderThePolicyAndLease() {
        var record = registry.tableControl().registerCreated("test_cat", "default", "orders", "alice",
                Optional.of(identity("orders")), Optional.of(version())).orElseThrow();
        maintenance.executeProcedure("expire_snapshots",
                new org.pangrova.spi.connector.metadata.table.QualifiedTableName("test_cat", "default", "orders"),
                Map.of("arg0", "default.orders", "arg1", "20",
                        "arg2", String.valueOf(NOW.minus(Duration.ofDays(30)).toEpochMilli()), "arg3", "true"), context());
        assertThat(metadata.procedureCalls).containsExactly("expire_snapshots|default.orders|20");
        assertThat(policies.listRuns(record.tableId(), 10)).singleElement()
                .satisfies(run -> assertThat(run.state()).isEqualTo(MaintenanceRunState.SUCCEEDED));
        assertThat(lease.released).isTrue();
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
        return new TableIdentity("ICEBERG", "uuid-" + table, "s3://bucket/default." + table, "schema");
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
        private Runnable onExecute = () -> { };
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
            onExecute.run();
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
        private final Map<String, LeaseEntry> active = new java.util.concurrent.ConcurrentHashMap<>();
        private final AtomicInteger renewals = new AtomicInteger();
        private volatile boolean renewalAllowed = true;
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
            renewals.incrementAndGet();
            LeaseEntry entry = active.get(leaseKey);
            return renewalAllowed && entry != null && entry.holderId().equals(holderId) && entry.epoch() == epoch;
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
        private final Map<String, CatalogDescriptor> descriptors = new LinkedHashMap<>();
        @Override public void saveTable(TableMetadata m) { }
        @Override public Optional<TableMetadata> loadTable(String c, String s, String t) { return Optional.empty(); }
        @Override public void deleteTable(String c, String s, String t) { }
        @Override public java.util.List<QualifiedTableRef> searchTables(String p) { return java.util.List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) { descriptors.put(d.name(), d); }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) { return Optional.ofNullable(descriptors.get(n)); }
        @Override public void deleteCatalogDescriptor(String n) { descriptors.remove(n); }
        @Override public java.util.List<CatalogDescriptor> listAllCatalogDescriptors() { return java.util.List.of(); }
        @Override public Optional<Instant> getLastSyncTime(String c) { return Optional.empty(); }
        @Override public void updateSyncTime(String c, Instant t) { }
        @Override public void recordSyncHistory(SyncResult r) { }
        @Override public void saveSchemas(String c, java.util.List<String> s) { }
    }
}
