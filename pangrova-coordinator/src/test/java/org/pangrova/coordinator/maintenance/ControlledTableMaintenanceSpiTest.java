package org.pangrova.coordinator.maintenance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.statement.ControlledTableMaintenance;
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
import org.pangrova.metadata.control.TableMaintenanceRun;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.sync.SyncResult;
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
import org.pangrova.streaming.maintenance.CompactionResult;
import org.pangrova.streaming.maintenance.MaintenanceConfig;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ControlledTableMaintenanceSpiTest {

    private static final Instant NOW = Instant.parse("2026-05-01T00:00:00Z");

    private TestMetadata metadata;
    private CatalogRegistry registry;
    private TableControlService control;
    private TableControlMaintenanceService policies;
    private TestLease lease;
    private MutableClock clock;
    private ControlledTableMaintenanceSpi spi;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(NOW);
        metadata = new TestMetadata();
        metadata.create("default", "orders", "uuid-orders", "ICEBERG");
        metadata.create("default", "paimon_tbl", "uuid-paimon", "PAIMON");
        ConnectorHandleFactory factory = descriptor ->
                new ConnectorCatalogHandle(descriptor, new TestConnector(metadata));
        control = new TableControlService(new InMemoryTableControlStore(),
                new TableControlSettings(true, true, Set.of()), clock);
        registry = new CatalogRegistry(new EmptyMetadataStore(), factory, null, null);
        registry.register(new CatalogDescriptor(
                "test_cat", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));
        registry.setTableControlService(control);
        policies = new TableControlMaintenanceService(
                new InMemoryTableControlMaintenanceStore(), clock);
        lease = new TestLease();
        var executor = new ControlledTableMaintenance(
                registry, policies, lease, "coordinator-1", Duration.ofMinutes(5), clock);
        spi = new ControlledTableMaintenanceSpi(
                registry, policies, executor, MaintenanceConfig.defaults(), clock);
    }

    @Test
    void managedTableRunsEveryPolicyOperationAutomatically() {
        TableControlRecord record = managed("orders");

        spi.runScheduledMaintenanceAsync("test_cat.default.orders").join();

        List<TableMaintenanceRun> runs = policies.listRuns(record.tableId(), 10);
        assertThat(runs).hasSize(3);
        assertThat(runs).allSatisfy(run -> {
            assertThat(run.mode()).isEqualTo("AUTOMATIC");
            assertThat(run.actor()).isEqualTo("system:maintenance");
            assertThat(run.state()).isEqualTo(MaintenanceRunState.SUCCEEDED);
        });
        assertThat(runs).extracting(TableMaintenanceRun::operation)
                .containsExactlyInAnyOrder(MaintenanceOperation.COMPACT,
                        MaintenanceOperation.EXPIRE_SNAPSHOTS,
                        MaintenanceOperation.REMOVE_ORPHAN_FILES);
        assertThat(metadata.procedures()).containsExactlyInAnyOrder(
                "compact", "expire_snapshots", "remove_orphan_files");
        assertThat(lease.released).isTrue();
    }

    @Test
    void secondScanIsNotDueUntilThePolicyIntervalElapses() {
        managed("orders");
        String name = "test_cat.default.orders";

        assertThat(spi.isScheduledMaintenanceDue(name)).isTrue();
        spi.runScheduledMaintenanceAsync(name).join();
        assertThat(spi.isScheduledMaintenanceDue(name)).isFalse();

        clock.advance(Duration.ofHours(2));
        assertThat(spi.isScheduledMaintenanceDue(name)).isTrue();
    }

    @Test
    void uncontrolledTableIsInvisibleToTheScheduler() {
        assertThat(spi.isScheduledMaintenanceDue("test_cat.default.orders")).isFalse();

        spi.runScheduledMaintenanceAsync("test_cat.default.orders").join();

        assertThat(metadata.procedures()).isEmpty();
        assertThat(lease.released).isFalse();
    }

    @Test
    void writeTriggeredCompactionForUncontrolledTableReturnsZeroWithoutFailing() {
        CompactionResult result = spi.compactAsync("test_cat.default.orders").join();

        assertThat(result.filesCompacted()).isZero();
        assertThat(metadata.procedures()).isEmpty();
    }

    @Test
    void linkedTableRunsOnlyExplicitlyEnabledOperations() {
        TableControlRecord record = adopt("paimon_tbl");
        policies.applyAdoptionProperties(record,
                Map.of("maintenance_operations", "COMPACT"), "alice");

        spi.runScheduledMaintenanceAsync("test_cat.default.paimon_tbl").join();

        assertThat(metadata.procedures()).containsExactly("compact");
        assertThat(policies.listRuns(record.tableId(), 10)).hasSize(1);
    }

    @Test
    void icebergExpirationUsesRetainCountAndTimeHorizon() {
        managed("orders");

        spi.runScheduledMaintenanceAsync("test_cat.default.orders").join();

        Map<String, String> args = metadata.arguments("expire_snapshots");
        assertThat(args).containsEntry("arg1", "10");
        assertThat(args).containsEntry("arg2",
                String.valueOf(NOW.minus(Duration.ofDays(7)).toEpochMilli()));
        assertThat(args).containsEntry("arg3", "false");
    }

    @Test
    void paimonExpirationUsesRetainWindowOnly() {
        TableControlRecord record = adopt("paimon_tbl");
        policies.applyAdoptionProperties(record,
                Map.of("maintenance_operations", "EXPIRE_SNAPSHOTS"), "alice");

        spi.runScheduledMaintenanceAsync("test_cat.default.paimon_tbl").join();

        Map<String, String> args = metadata.arguments("expire_snapshots");
        assertThat(args).containsEntry("arg1", "10");
        assertThat(args).containsEntry("arg2", "10");
        assertThat(args).containsEntry("arg3", "false");
    }

    @Test
    void leaseContentionSurfacesToTheSchedulerAndSkipsTheCycle() {
        TableControlRecord record = managed("orders");
        lease.busy = true;

        assertThatThrownBy(() -> spi.runScheduledMaintenanceAsync(
                "test_cat.default.orders").join())
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("already in progress");

        assertThat(metadata.procedures()).isEmpty();
        assertThat(policies.listRuns(record.tableId(), 10))
                .extracting(TableMaintenanceRun::state)
                .containsExactly(MaintenanceRunState.SKIPPED);
    }

    private TableControlRecord managed(String table) {
        return control.registerCreated("test_cat", "default", table, "alice",
                Optional.of(identity(table)), Optional.of(version())).orElseThrow();
    }

    private TableControlRecord adopt(String table) {
        return control.adopt("test_cat", "default", table, "alice",
                Optional.of(identity(table)), Optional.of(version()));
    }

    private TableIdentity identity(String table) {
        return new TableIdentity(metadata.format(table), "uuid-" + table,
                "s3://bucket/" + table, "schema");
    }

    private static TableVersionToken version() {
        return TableVersionToken.of("ICEBERG", "uuid", "v1");
    }

    private static final class MutableClock extends Clock {
        private final AtomicReference<Instant> now;

        MutableClock(Instant now) {
            this.now = new AtomicReference<>(now);
        }

        void advance(Duration duration) {
            now.updateAndGet(current -> current.plus(duration));
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now.get();
        }
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
        private final Map<String, String> formats = new LinkedHashMap<>();
        private final List<ServiceCall> calls = new ArrayList<>();

        void create(String schema, String table, String uuid, String format) {
            SchemaTableName name = new SchemaTableName(schema, table);
            tables.put(key(name), new ConnectorTableMetadata(name, List.of("id"), Map.of()));
            uuids.put(key(name), uuid);
            formats.put(key(name), format);
        }

        String format(String table) {
            return formats.get("default." + table);
        }

        List<String> procedures() {
            return calls.stream().map(ServiceCall::procedure).toList();
        }

        Map<String, String> arguments(String procedure) {
            return calls.stream()
                    .filter(call -> call.procedure().equals(procedure))
                    .findFirst()
                    .map(ServiceCall::arguments)
                    .orElseThrow();
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
                    formats.get(key), uuids.get(key), "s3://bucket/" + key, "schema"));
        }

        @Override
        public Optional<TableVersionToken> observeVersion(ConnectorSession session,
                ConnectorTableHandle handle) {
            return Optional.of(TableVersionToken.of(
                    "ICEBERG", uuids.get(((Handle) handle).key), "v1"));
        }

        @Override
        public List<TypedRow> executeProcedure(ConnectorSession session, String procedureName,
                Map<String, String> arguments) {
            calls.add(new ServiceCall(procedureName, Map.copyOf(arguments)));
            return List.of(TypedRow.builder().putString("result", procedureName + " succeeded")
                    .build());
        }
    }

    private record ServiceCall(String procedure, Map<String, String> arguments) { }

    private record Handle(String key) implements ConnectorTableHandle { }

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
        @Override public List<QualifiedTableRef> searchTables(String p) { return List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) { }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) { return Optional.empty(); }
        @Override public void deleteCatalogDescriptor(String n) { }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }
        @Override public Optional<Instant> getLastSyncTime(String c) { return Optional.empty(); }
        @Override public void updateSyncTime(String c, Instant t) { }
        @Override public void recordSyncHistory(SyncResult r) { }
        @Override public void saveSchemas(String c, List<String> s) { }
    }
}
