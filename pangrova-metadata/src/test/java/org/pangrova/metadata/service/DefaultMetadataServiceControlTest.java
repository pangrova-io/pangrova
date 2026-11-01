package org.pangrova.metadata.service;

import org.pangrova.table.api.control.TableControlState;
import org.pangrova.table.control.TableControlService;
import org.pangrova.table.control.TableControlSettings;
import org.pangrova.table.store.memory.control.InMemoryTableControlStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.governance.core.classification.ColumnTagService;
import org.pangrova.governance.api.classification.TagChangedEvent;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.catalog.handle.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.catalog.handle.connector.ConnectorHandleFactory;
import org.pangrova.table.api.control.OwnershipMode;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.object.ColumnMetadata;
import org.pangrova.metadata.api.object.ColumnType;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
import org.pangrova.metadata.api.catalog.SyncResult;
import org.pangrova.spi.connector.plugin.Connector;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void cachedReadsRejectARecreatedCatalogEvenWhenThePhysicalTableIsUnchanged() {
        service.createTable(session(), ref("orders"), tableMetadata("orders"));
        service.resolveTable(session(), ref("orders"));
        registry.unregister("cat");
        registry.register(new CatalogDescriptor("cat", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));

        assertThatThrownBy(() -> service.resolveTable(session(), ref("orders")))
                .isInstanceOf(org.pangrova.spi.security.AccessDeniedException.class)
                .hasMessageContaining("catalog deployment");
    }

    @Test
    void shadowDropRejectsARecreatedCatalogBeforeDeletingThePhysicalTable() {
        service.createTable(session(), ref("orders"), tableMetadata("orders"));
        var captured = registry.tableControl().find("cat", "db", "orders").orElseThrow();
        assertThat(registry.tableControl().enforcing()).isFalse();
        registry.unregister("cat");
        registry.register(new CatalogDescriptor("cat", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));

        assertThatThrownBy(() -> service.dropTable(session(), ref("orders")))
                .isInstanceOf(org.pangrova.spi.security.AccessDeniedException.class)
                .hasMessageContaining("catalog deployment");
        assertThat(connectorMetadata.tables).containsKey("db.orders");
        assertThat(registry.tableControl().find("cat", "db", "orders")).contains(captured);
    }

    @Test
    void cachedReadsCannotBypassAnIncompleteRename() {
        service.createTable(session(), ref("orders"), tableMetadata("orders"));
        service.resolveTable(session(), ref("orders"));
        var control = registry.tableControl();
        var expected = control.find("cat", "db", "orders").orElseThrow();
        var pending = control.rename(expected, "db", "orders");

        assertThatThrownBy(() -> service.resolveTable(session(), ref("orders")))
                .isInstanceOf(org.pangrova.spi.security.AccessDeniedException.class)
                .hasMessageContaining("rename is incomplete");
        var handle = connectorMetadata.getTableHandle(null, new SchemaTableName("db", "orders"));
        control.completeRename(pending, connectorMetadata.getTableIdentity(null, handle),
                connectorMetadata.observeVersion(null, handle));
        assertThat(service.resolveTable(session(), ref("orders")).tableId()).contains(expected.tableId());
    }

    @Test
    void dropCannotDeleteARecreatedTableWithTheOldOwnership() {
        registry.setTableControlService(new org.pangrova.table.control.TableControlService(
                new org.pangrova.table.store.memory.control.InMemoryTableControlStore(), registry::requireCurrentCatalogIdentity,
                new org.pangrova.table.control.TableControlSettings(true, true, Set.of())));
        service.createTable(session(), ref("orders"), tableMetadata("orders"));
        connectorMetadata.create(new SchemaTableName("db", "orders"), "replacement");

        assertThatThrownBy(() -> service.dropTable(session(), ref("orders")))
                .isInstanceOf(org.pangrova.spi.security.AccessDeniedException.class)
                .hasMessageContaining("identity");
        assertThat(connectorMetadata.tables).containsKey("db.orders");
        assertThat(registry.tableControl().find("cat", "db", "orders").orElseThrow().state())
                .isEqualTo(org.pangrova.table.api.control.TableControlState.QUARANTINED);
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
    void cachedObservedResolutionIsInvalidatedAfterAdopt() {
        connectorMetadata.create(new SchemaTableName("db", "external"), "uuid-ext");
        assertThat(service.resolveTable(session(), ref("external")).ownershipMode())
                .isEqualTo(OwnershipMode.OBSERVED);
        var adopted = registry.tableControl().adopt("cat", "db", "external", "alice",
                Optional.of(new TableIdentity("ICEBERG", "uuid-ext", "s3://bucket/db.external", "schema")),
                Optional.of(TableVersionToken.of("ICEBERG", "uuid-ext", "snapshot-1")));

        ResolvedTable resolved = service.resolveTable(session(), ref("external"));
        assertThat(resolved.ownershipMode()).isEqualTo(OwnershipMode.LINKED);
        assertThat(resolved.tableId()).contains(adopted.tableId());
    }

    @Test
    void cachedResolutionCannotSurviveRetirementAndReregistrationWithTheSameVersion() {
        service.createTable(session(), ref("orders"), tableMetadata("orders"));
        ResolvedTable previous = service.resolveTable(session(), ref("orders"));
        registry.tableControl().markDropped(registry.tableControl().find("cat", "db", "orders").orElseThrow());
        var replacement = registry.tableControl().registerCreated("cat", "db", "orders", "alice",
                Optional.of(new TableIdentity("ICEBERG", "uuid-orders", "s3://bucket/db.orders", "schema")),
                Optional.of(TableVersionToken.of("ICEBERG", "uuid-orders", "snapshot-1"))).orElseThrow();

        ResolvedTable resolved = service.resolveTable(session(), ref("orders"));
        assertThat(resolved.controlRevision()).isEqualTo(previous.controlRevision());
        assertThat(resolved.versionToken()).isEqualTo(previous.versionToken());
        assertThat(resolved.tableId()).contains(replacement.tableId()).isNotEqualTo(previous.tableId());
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
        service.createTable(session(), ref("external"), new TableMetadata("cat", "db", "external",
                List.of(new ColumnMetadata("email", ColumnType.of("VARCHAR"), true, "", Map.of())),
                Map.of(), List.of(), Map.of(), null, null));
        service.setTag(session(), ref("external"), "email", "pii_type", "PII_EMAIL");
        ColumnTagService tagService = new ColumnTagService(table -> registry.tableGovernance()
                .requireBindable(table.catalog(), table.schema(), table.table()).tableId());
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
        service.createTable(session(), ref("orders"), tableMetadata("orders"));
        service.createTable(session(), ref("other"), tableMetadata("other"));
        service.setTag(session(), ref("orders"), "id", "classification", "sensitive");
        service.setTag(session(), ref("other"), "id", "classification", "internal");
        var tableId = registry.tableControl().find("cat", "db", "orders").orElseThrow().tableId();
        assertThat(service.clearTableTags(session(), tableId)).isEqualTo(1);
        assertThat(service.listTags(session(), ref("orders"), "id")).isEmpty();
        assertThat(service.listTags(session(), ref("other"), "id"))
                .containsEntry("classification", "internal");
    }

    @Test
    void tagApiResolvesCanonicalColumnsAndChecksTypesWithoutDdlPrechecks() {
        service.createTable(session(), ref("orders"), tableMetadata("orders"));
        declareColumns("orders", List.of(new ConnectorTableMetadata.Column("email",
                org.pangrova.spi.connector.data.type.PangrovaType.VARCHAR)));
        service.setTag(session(), ref("orders"), "EMAIL", "pii_type", "PII_EMAIL");
        assertThat(service.listTags(session(), ref("orders"), "email"))
                .containsExactly(Map.entry("pii_type", "PII_EMAIL"));
        assertThat(service.listTags(session(), ref("orders"), "EMAIL"))
                .isEqualTo(service.listTags(session(), ref("orders"), "email"));
        assertThat(connectorMetadata.lastSession.getUser()).isEqualTo("alice");
        assertThat(connectorMetadata.lastSession.getProperties()).containsEntry("tenant_id", "default");
        service.unsetTag(session(), ref("orders"), "EMAIL", "pii_type");
        assertThat(service.listTags(session(), ref("orders"), "email")).isEmpty();

        declareColumns("orders", List.of(new ConnectorTableMetadata.Column("id",
                org.pangrova.spi.connector.data.type.PangrovaType.BIGINT)));
        assertThatThrownBy(() -> service.setTag(session(), ref("orders"), "ID", "pii_type", "PII_EMAIL"))
                .hasMessageContaining("VARCHAR");
        assertThatThrownBy(() -> service.setTag(session(), ref("orders"), "missing", "classification", "private"))
                .hasMessageContaining("Column not found");
        assertThat(registry.tableGovernance().bindingsFor("cat", "db", "orders")).isEmpty();
    }

    @Test
    void ambiguousCaseFoldedTagTargetIsRejectedWhileExactNamesRemainDistinct() {
        service.createTable(session(), ref("orders"), tableMetadata("orders"));
        declareColumns("orders", List.of(
                new ConnectorTableMetadata.Column("Email", org.pangrova.spi.connector.data.type.PangrovaType.VARCHAR),
                new ConnectorTableMetadata.Column("email", org.pangrova.spi.connector.data.type.PangrovaType.VARCHAR)));
        assertThatThrownBy(() -> service.setTag(session(), ref("orders"), "EMAIL", "pii_type", "PII_EMAIL"))
                .hasMessageContaining("ambiguous");
        service.setTag(session(), ref("orders"), "Email", "pii_type", "PII_EMAIL");
        assertThat(service.listTags(session(), ref("orders"), "email")).isEmpty();
        assertThat(service.listTags(session(), ref("orders"), "Email"))
                .containsEntry("pii_type", "PII_EMAIL");
    }

    @Test
    void tagApiRejectsUntypedPiiAndUnavailableSchemaEvidence() {
        service.createTable(session(), ref("orders"), tableMetadata("orders"));
        connectorMetadata.tables.put("db.orders", new ConnectorTableMetadata(
                new SchemaTableName("db", "orders"), List.of("id"), Map.of()));
        assertThatThrownBy(() -> service.setTag(session(), ref("orders"), "id", "pii_type", "PII_EMAIL"))
                .hasMessageContaining("declared type");
        connectorMetadata.schemaFingerprint = "";
        assertThatThrownBy(() -> service.setTag(session(), ref("orders"), "id", "classification", "private"))
                .hasMessageContaining("schema evidence");
        assertThat(registry.tableGovernance().bindingsFor("cat", "db", "orders")).isEmpty();
    }

    @Test
    void tagApiQuarantinesExternalReplacementDuringColumnResolution() {
        service.createTable(session(), ref("orders"), tableMetadata("orders"));
        var original = registry.tableControl().find("cat", "db", "orders").orElseThrow();
        connectorMetadata.afterDeclaration = () -> connectorMetadata.create(
                new SchemaTableName("db", "orders"), "replacement");
        assertThatThrownBy(() -> service.setTag(session(), ref("orders"), "id", "classification", "private"))
                .hasMessageContaining("identity");
        assertThat(registry.tableControl().findById(original.tableId()).orElseThrow().state())
                .isEqualTo(org.pangrova.table.api.control.TableControlState.QUARANTINED);
        assertThat(registry.tableGovernance().bindingsFor(original.tableId())).isEmpty();
    }

    @Test
    void tagApiCannotRetargetAControlOwnerReplacedDuringResolution() {
        service.createTable(session(), ref("orders"), tableMetadata("orders"));
        var control = registry.tableControl();
        var original = control.find("cat", "db", "orders").orElseThrow();
        connectorMetadata.afterDeclaration = () -> {
            control.markDropped(original);
            control.registerCreated("cat", "db", "orders", "alice", Optional.of(new TableIdentity(
                    "ICEBERG", "uuid-orders", "s3://bucket/db.orders", "schema")), Optional.empty());
        };
        assertThatThrownBy(() -> service.setTag(session(), ref("orders"), "id", "classification", "private"))
                .hasMessageContaining("ownership");
        var replacement = control.find("cat", "db", "orders").orElseThrow();
        assertThat(replacement.tableId()).isNotEqualTo(original.tableId());
        assertThat(registry.tableGovernance().bindingsFor(replacement.tableId())).isEmpty();
        assertThat(registry.tableGovernance().bindingsFor(original.tableId())).isEmpty();
    }

    @Test
    void tagApiRejectsSchemaChangeDuringColumnResolution() {
        service.createTable(session(), ref("orders"), tableMetadata("orders"));
        connectorMetadata.afterDeclaration = () -> connectorMetadata.schemaFingerprint = "another-schema";
        assertThatThrownBy(() -> service.setTag(session(), ref("orders"), "id", "classification", "private"))
                .hasMessageContaining("schema evidence");
        assertThat(registry.tableGovernance().bindingsFor("cat", "db", "orders")).isEmpty();
    }

    private void declareColumns(String table, List<ConnectorTableMetadata.Column> fields) {
        connectorMetadata.tables.put("db." + table, new ConnectorTableMetadata(new SchemaTableName("db", table),
                fields.stream().map(ConnectorTableMetadata.Column::name).toList(), fields, Map.of()));
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
        private String schemaFingerprint = "schema";
        private Runnable afterDeclaration = () -> { };
        private ConnectorSession lastSession;

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
            lastSession = session;
            var declaration = tables.get(((TestHandle) h).key);
            afterDeclaration.run();
            return declaration;
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
                    "ICEBERG", uuids.get(key), "s3://bucket/" + key, schemaFingerprint));
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

    static final class TestMetadataStore implements org.pangrova.metadata.api.catalog.MetadataStore {
        private final Map<String, CatalogDescriptor> descriptors = new LinkedHashMap<>();
        @Override public void saveTable(TableMetadata m) { }
        @Override public Optional<TableMetadata> loadTable(String c, String s, String t) { return Optional.empty(); }
        @Override public void deleteTable(String c, String s, String t) { }
        @Override public List<QualifiedTableRef> searchTables(String p) { return List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) { descriptors.put(d.name(), d); }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) { return Optional.ofNullable(descriptors.get(n)); }
        @Override public void deleteCatalogDescriptor(String n) { descriptors.remove(n); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }
        @Override public Optional<Instant> getLastSyncTime(String n) { return Optional.empty(); }
        @Override public void updateSyncTime(String n, Instant t) { }
        @Override public void recordSyncHistory(SyncResult r) { }
        @Override public void saveSchemas(String n, List<String> s) { }
    }
}
