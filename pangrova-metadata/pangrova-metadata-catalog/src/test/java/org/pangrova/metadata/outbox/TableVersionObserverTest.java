package org.pangrova.metadata.outbox;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.api.catalog.ConnectorHandleFactory;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.api.enums.TableControlState;
import org.pangrova.metadata.api.event.MetadataEvent;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.event.MetadataEventListener;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.connector.api.metadata.table.TableVersionToken;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class TableVersionObserverTest {

    private static final Clock CLOCK = Clock.fixed(
            Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);

    private MutableConnectorMetadata connectorMetadata;
    private MetadataEventDispatcher dispatcher;
    private CatalogRegistry registry;
    private TableVersionObserver observer;

    @BeforeEach
    void setUp() {
        connectorMetadata = new MutableConnectorMetadata();
        dispatcher = new MetadataEventDispatcher();
        ConnectorHandleFactory factory = descriptor ->
                new ConnectorCatalogHandle(descriptor, new TestConnector(connectorMetadata));
        registry = new CatalogRegistry(memoryTableStores(), new TestCatalogDeployments(), factory, null, dispatcher);
        registry.register(new CatalogDescriptor("cat", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));
        registry.initialize();
        observer = new TableVersionObserver(registry, dispatcher, CLOCK);
    }

    @AfterEach
    void tearDown() {
        registry.shutdown();
        dispatcher.shutdown();
    }

    @Test
    void observesExternalCommitAndDispatchesAuditEvent() throws Exception {
        registerManaged("orders");
        connectorMetadata.version("db.orders", "2");
        CountDownLatch delivered = new CountDownLatch(1);
        AtomicReference<MetadataEvent> observed = new AtomicReference<>();
        MetadataEventListener listener = event -> {
            observed.set(event);
            delivered.countDown();
        };
        dispatcher.addListener(listener);

        TableVersionObservationReport report = observer.observeCatalog("cat");

        assertThat(report.checked()).isEqualTo(1);
        assertThat(report.versionChanges()).isEqualTo(1);
        TableControlRecord record = managedRecord("orders");
        assertThat(record.versionToken()).isEqualTo("uuid-orders:2");
        assertThat(record.metadataRevision()).isEqualTo(2L);
        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(observed.get()).isInstanceOf(MetadataEvent.ExternalCommitObservedEvent.class);
        MetadataEvent.ExternalCommitObservedEvent event =
                (MetadataEvent.ExternalCommitObservedEvent) observed.get();
        assertThat(event.tableRef()).isEqualTo(new QualifiedTableRef("cat", "db", "orders"));
        assertThat(event.previousVersionToken()).isEqualTo("uuid-orders:1");
        assertThat(event.versionToken()).isEqualTo("uuid-orders:2");
        assertThat(event.observedAt()).isEqualTo(CLOCK.instant());

        // A second pass sees the recorded version and writes nothing new.
        assertThat(observer.observeCatalog("cat").versionChanges()).isZero();
        assertThat(managedRecord("orders").metadataRevision()).isEqualTo(2L);
    }

    @Test
    void missingTableBecomesStaleAndReappearingTableRecovers() {
        registerManaged("orders");
        connectorMetadata.remove("db.orders");

        TableVersionObservationReport missing = observer.observeCatalog("cat");
        assertThat(missing.missing()).isEqualTo(1);
        assertThat(managedRecord("orders").state()).isEqualTo(TableControlState.STALE);

        connectorMetadata.create(new SchemaTableName("db", "orders"), "uuid-orders");
        connectorMetadata.version("db.orders", "2");
        TableVersionObservationReport recovered = observer.observeCatalog("cat");

        assertThat(recovered.versionChanges()).isEqualTo(1);
        assertThat(managedRecord("orders").state()).isEqualTo(TableControlState.ACTIVE);
    }

    @Test
    void changedFormatIdentityQuarantinesTheRecord() {
        registerManaged("orders");
        connectorMetadata.replaceUuid("db.orders", "uuid-replaced");

        TableVersionObservationReport report = observer.observeCatalog("cat");

        assertThat(report.identityChanges()).isEqualTo(1);
        assertThat(managedRecord("orders").state()).isEqualTo(TableControlState.QUARANTINED);
    }

    @Test
    void observedTablesAreNotPersistedOrObserved() {
        connectorMetadata.create(new SchemaTableName("db", "external"), "uuid-external");

        TableVersionObservationReport report = observer.observeCatalog("cat");

        assertThat(report.checked()).isZero();
        assertThat(registry.tableControl().find("cat", "db", "external")).isEmpty();
    }

    @Test
    void unreachableCatalogDoesNotReclassifyControlledTables() {
        registerManaged("orders");
        registry.unregister("cat");

        TableVersionObservationReport report = observer.observeCatalog("cat");

        assertThat(report.checked()).isZero();
        assertThat(report.hasFailures()).isTrue();
        assertThat(managedRecord("orders").state()).isEqualTo(TableControlState.ACTIVE);
    }

    private void registerManaged(String table) {
        connectorMetadata.create(new SchemaTableName("db", table), "uuid-" + table);
        connectorMetadata.version("db." + table, "1");
        registry.tableControl().registrations().registerCreated("cat", "db", table, "alice",
                Optional.of(identity("uuid-" + table)),
                Optional.of(TableVersionToken.of("ICEBERG", "uuid-" + table, "1")));
    }

    private TableControlRecord managedRecord(String table) {
        return registry.tableControl().find("cat", "db", table).orElseThrow();
    }

    private static TableIdentity identity(String uuid) {
        return new TableIdentity("ICEBERG", uuid, "s3://bucket/db.orders", "schema");
    }

    static final class MutableConnectorMetadata implements ConnectorMetadata {

        private final Map<String, ConnectorTableMetadata> tables = new LinkedHashMap<>();
        private final Map<String, String> uuids = new LinkedHashMap<>();
        private final Map<String, String> versions = new LinkedHashMap<>();

        void create(SchemaTableName name, String uuid) {
            tables.put(key(name), new ConnectorTableMetadata(name, List.of("id"), Map.of()));
            uuids.put(key(name), uuid);
        }

        void remove(String key) {
            tables.remove(key);
            uuids.remove(key);
            versions.remove(key);
        }

        void replaceUuid(String key, String uuid) {
            uuids.put(key, uuid);
        }

        void version(String key, String version) {
            versions.put(key, version);
        }

        private static String key(SchemaTableName name) {
            return name.schema() + "." + name.table();
        }

        @Override
        public void beginQuery(org.pangrova.connector.api.query.ConnectorQueryAuthority authority) { }

        @Override
        public void cleanupQuery(org.pangrova.connector.api.query.ConnectorQueryAuthority authority) { }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of("db");
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schema) {
            return tables.values().stream().map(ConnectorTableMetadata::table).toList();
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName name) {
            return tables.containsKey(key(name)) ? new TestHandle(key(name)) : null;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle h) {
            return tables.get(((TestHandle) h).key);
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle h) {
            return Map.of();
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle h) {
            return TableStatistics.UNKNOWN;
        }

        @Override
        public Optional<TableIdentity> getTableIdentity(ConnectorSession session, ConnectorTableHandle handle) {
            String key = ((TestHandle) handle).key;
            return Optional.of(new TableIdentity(
                    "ICEBERG", uuids.get(key), "s3://bucket/" + key, "schema"));
        }

        @Override
        public Optional<TableVersionToken> observeVersion(ConnectorSession session, ConnectorTableHandle handle) {
            String key = ((TestHandle) handle).key;
            String version = versions.get(key);
            return version == null ? Optional.empty()
                    : Optional.of(TableVersionToken.of("ICEBERG", uuids.get(key), version));
        }
    }

    record TestHandle(String key) implements ConnectorTableHandle { }

    static final class TestConnector implements Connector {
        private final ConnectorMetadata metadata;

        TestConnector(ConnectorMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return metadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() { }
    }

    static final class TestCatalogDeployments implements CatalogDeploymentRepository {
        @Override
        public java.util.Optional<org.pangrova.metadata.api.catalog.CatalogDescriptor>
                findCatalogDescriptorRevision(String name, String incarnationId, long revision) {
            throw new UnsupportedOperationException("This fixture does not store deployment history");
        }

        private final Map<String, CatalogDescriptor> descriptors = new LinkedHashMap<>();

        @Override public void saveCatalogDescriptor(CatalogDescriptor d) { descriptors.put(d.name(), d); }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) {
            return Optional.ofNullable(descriptors.get(n));
        }
        @Override public boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) { return descriptors.entrySet().removeIf(entry -> entry.getValue().identity().equals(expected)); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }

    }
}
