package org.pangrova.metadata.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.governance.classification.ColumnTagService;
import org.pangrova.governance.classification.TagChangedEvent;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.metadata.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.control.OwnershipMode;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.model.ColumnMetadata;
import org.pangrova.metadata.model.ColumnType;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
import org.pangrova.metadata.sync.SyncResult;
import org.pangrova.metadata.tag.InMemoryColumnTagStore;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableIdentity;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.spi.connector.metadata.table.TableVersionToken;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultMetadataServiceControlTest {

    private DefaultMetadataService service;
    private CatalogRegistry registry;
    private MetadataEventDispatcher dispatcher;
    private MutableConnectorMetadata connectorMetadata;

    @BeforeEach
    void setUp() {
        connectorMetadata = new MutableConnectorMetadata();
        dispatcher = new MetadataEventDispatcher();
        ConnectorHandleFactory factory = descriptor ->
                new ConnectorCatalogHandle(descriptor, new TestConnector(connectorMetadata));
        registry = new CatalogRegistry(new TestMetadataStore(), factory, null, dispatcher);
        registry.register(new CatalogDescriptor(
                "cat", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));
        registry.initialize();
        service = new DefaultMetadataService(
                registry, new TieredMetadataCache(), dispatcher,
                new SchemaEvolutionValidator(), 1_000L, Duration.ofMinutes(5));
    }

    @AfterEach
    void tearDown() {
        registry.shutdown();
        dispatcher.shutdown();
    }

    @Test
    void catalogVisibleTableWithoutControlRecordIsObserved() {
        connectorMetadata.create(new SchemaTableName("db", "external"), "uuid-ext");

        ResolvedTable resolved = service.resolveTable(session(), ref("external"));

        assertThat(resolved.ownershipMode()).isEqualTo(OwnershipMode.OBSERVED);
        assertThat(resolved.tableId()).isEmpty();
        assertThat(resolved.versionToken()).contains("uuid-ext:snapshot-1");
    }

    @Test
    void controlledResolutionUsesTheRegistryVersionNotTheLiveConnectorVersion() {
        service.createTable(session(), ref("orders"), tableMetadata("orders"));
        connectorMetadata.setVersion("db.orders", "snapshot-9");

        assertThat(service.resolveTable(session(), ref("orders")).versionToken())
                .contains("uuid-orders:snapshot-1");
    }

    @Test
    void createTableRegistersManagedOwnershipAndVersion() {
        service.createTable(session(), ref("orders"), tableMetadata("orders"));

        ResolvedTable resolved = service.resolveTable(session(), ref("orders"));

        assertThat(resolved.ownershipMode()).isEqualTo(OwnershipMode.MANAGED);
        assertThat(resolved.tableId()).isPresent();
        assertThat(resolved.versionToken()).contains("uuid-orders:snapshot-1");
        assertThat(registry.tableControl().find("cat", "db", "orders")).get()
                .satisfies(record -> {
                    assertThat(record.ownerPrincipal()).isEqualTo("alice");
                    assertThat(record.formatTableUuid()).isEqualTo("uuid-orders");
                    assertThat(record.metadataRevision()).isEqualTo(1L);
                });
    }

    @Test
    void controlledResolutionCacheIsInvalidatedWhenTheObservedVersionChanges() {
        service.createTable(session(), ref("orders"), tableMetadata("orders"));
        assertThat(service.resolveTable(session(), ref("orders")).versionToken())
                .contains("uuid-orders:snapshot-1");

        registry.tableControl().registerCreated("cat", "db", "orders", "alice",
                Optional.of(new TableIdentity(
                        "ICEBERG", "uuid-orders", "s3://bucket/db.orders", "schema")),
                Optional.of(TableVersionToken.of("ICEBERG", "uuid-orders", "snapshot-2")));

        assertThat(service.resolveTable(session(), ref("orders")).versionToken())
                .contains("uuid-orders:snapshot-2");
    }

    @Test
    void dropTableRetiresTheControlRecord() {
        service.createTable(session(), ref("orders"), tableMetadata("orders"));

        service.dropTable(session(), ref("orders"));

        assertThat(registry.tableControl().find("cat", "db", "orders")).isEmpty();
        assertThat(connectorMetadata.tables).doesNotContainKey("db.orders");
    }

    @Test
    void persistedColumnTagsAreReplayedIntoGovernanceListeners() {
        InMemoryColumnTagStore tagStore = new InMemoryColumnTagStore();
        tagStore.put("cat|db|external|email", "pii_type", "PII_EMAIL");
        service.setColumnTagStore(tagStore);
        ColumnTagService tagService = new ColumnTagService();
        List<TagChangedEvent> events = new java.util.ArrayList<>();
        tagService.addListener(events::add);
        service.setColumnTagService(tagService);

        assertThat(service.replayColumnTags()).isEqualTo(1);

        assertThat(events).singleElement().satisfies(event -> {
            assertThat(event.catalog()).isEqualTo("cat");
            assertThat(event.schema()).isEqualTo("db");
            assertThat(event.table()).isEqualTo("external");
            assertThat(event.columnName()).isEqualTo("email");
            assertThat(event.tagKey()).isEqualTo("pii_type");
            assertThat(event.newValue()).isEqualTo("PII_EMAIL");
        });
        assertThat(tagService.getTags("cat", "db", "external", "email"))
                .containsEntry("pii_type", "PII_EMAIL");
    }

    @Test
    void clearingTableTagsRemovesOnlyThatTablesTags() {
        InMemoryColumnTagStore tagStore = new InMemoryColumnTagStore();
        tagStore.put("cat|db|orders|id", "pii_type", "PII_EMAIL");
        tagStore.put("cat|db|other|id", "pii_type", "PII_PHONE");
        service.setColumnTagStore(tagStore);

        assertThat(service.clearTableTags(session(), ref("orders"))).isEqualTo(1);

        assertThat(tagStore.list("cat|db|orders|id")).isEmpty();
        assertThat(tagStore.list("cat|db|other|id"))
                .containsEntry("pii_type", "PII_PHONE");
    }

    private static QualifiedTableRef ref(String table) {
        return new QualifiedTableRef("cat", "db", table);
    }

    private static MetadataSessionContext session() {
        return new MetadataSessionContext(
                "alice", "cat", "db", "UTC", "en",
                Map.of(), "default", "", "query-control");
    }

    private static TableMetadata tableMetadata(String table) {
        return new TableMetadata("cat", "db", table,
                List.of(new ColumnMetadata("id", ColumnType.of("BIGINT"), true, "", Map.of())),
                Map.of(), List.of(), Map.of(), null, null);
    }

    static final class MutableConnectorMetadata implements ConnectorMetadata {

        private final Map<String, ConnectorTableMetadata> tables = new LinkedHashMap<>();
        private final Map<String, String> uuids = new LinkedHashMap<>();
        private final Map<String, String> versions = new LinkedHashMap<>();

        void create(SchemaTableName name, String uuid) {
            tables.put(key(name), new ConnectorTableMetadata(name, List.of("id"), Map.of()));
            uuids.put(key(name), uuid);
        }

        void setVersion(String key, String version) {
            versions.put(key, version);
        }

        private static String key(SchemaTableName name) {
            return name.schema() + "." + name.table();
        }

        @Override
        public org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle createTable(
                ConnectorSession session, ConnectorTableMetadata table) {
            tables.put(key(table.table()), table);
            uuids.put(key(table.table()), "uuid-" + table.table().table());
            return null;
        }

        @Override
        public void dropTable(ConnectorSession session, ConnectorTableHandle handle) {
            String key = ((TestHandle) handle).key;
            tables.remove(key);
            uuids.remove(key);
        }

        @Override
        public void beginQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) { }

        @Override
        public void cleanupQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) { }

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
            return Optional.of(TableVersionToken.of("ICEBERG", uuids.get(key),
                    versions.getOrDefault(key, "snapshot-1")));
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

    static final class TestMetadataStore implements org.pangrova.metadata.store.MetadataStore {
        @Override public void saveTable(TableMetadata m) { }
        @Override public Optional<TableMetadata> loadTable(String c, String s, String t) { return Optional.empty(); }
        @Override public void deleteTable(String c, String s, String t) { }
        @Override public List<QualifiedTableRef> searchTables(String p) { return List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) { }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) { return Optional.empty(); }
        @Override public void deleteCatalogDescriptor(String n) { }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }
        @Override public Optional<Instant> getLastSyncTime(String n) { return Optional.empty(); }
        @Override public void updateSyncTime(String n, Instant t) { }
        @Override public void recordSyncHistory(SyncResult r) { }
        @Override public void saveSchemas(String n, List<String> s) { }
    }
}
