package org.pangrova.coordinator.query.statement;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.metadata.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.control.InMemoryTableControlStore;
import org.pangrova.metadata.control.TableControlService;
import org.pangrova.metadata.control.TableControlSettings;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.sync.SyncResult;
import org.pangrova.parser.SqlParser;
import org.pangrova.parser.ast.statement.ShowTableControlsStatement;
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

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.coordinator.query.statement.StatementTestSessions.context;

class ShowTableControlsTest {

    private final SqlParser parser = new SqlParser();
    private TestMetadata metadata;
    private CatalogRegistry registry;
    private ShowDescribeExecutor executor;

    @BeforeEach
    void setUp() {
        metadata = new TestMetadata();
        metadata.create("default", "orders", "uuid-orders");
        metadata.create("default", "events", "uuid-events");
        ConnectorHandleFactory factory = descriptor ->
                new ConnectorCatalogHandle(descriptor, new TestConnector(metadata));
        registry = new CatalogRegistry(new EmptyMetadataStore(), factory, null, null);
        registry.register(new CatalogDescriptor(
                "test_cat", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));
        registry.setTableControlService(new TableControlService(
                new InMemoryTableControlStore(), registry::requireCurrentCatalogIdentity,
                new TableControlSettings(true, true, Set.of())));
        executor = new ShowDescribeExecutor(registry);
    }

    @Test
    void withoutScopeListsPersistedControlRecordsOnly() {
        registry.tableControl().registerCreated("test_cat", "default", "orders", "alice",
                Optional.of(identity("orders")), Optional.of(version("orders")));

        List<TypedRow> rows = executor.executeShowTableControls(
                statement("SHOW TABLE CONTROLS"), context());

        assertThat(rows).hasSize(1);
        TypedRow row = rows.get(0);
        assertThat(row.getString("catalog_name")).isEqualTo("test_cat");
        assertThat(row.getString("table_name")).isEqualTo("orders");
        assertThat(row.getString("ownership_mode")).isEqualTo("MANAGED");
        assertThat(row.getString("state")).isEqualTo("ACTIVE");
        assertThat(row.getString("table_id")).isNotBlank();
        assertThat(row.getString("format_table_uuid")).isEqualTo("uuid-orders");
        assertThat(row.getString("version_token")).isEqualTo("uuid-orders:snapshot-1");
    }

    @Test
    void controlledRowsExposeFreshAnalyzeStatistics() {
        registry.tableControl().registerCreated("test_cat", "default", "orders", "alice",
                Optional.of(identity("orders")), Optional.of(version("orders")));
        registry.tableStatistics().record("test_cat", "default", "orders",
                new TableStatistics(7, 700), "ANALYZE");

        TypedRow row = executor.executeShowTableControls(
                statement("SHOW TABLE CONTROLS IN test_cat.default.orders"), context()).get(0);

        assertThat(row.getRaw("stats_version")).isEqualTo(0L);
        assertThat(row.getRaw("stats_row_count")).isEqualTo(7L);
        assertThat(row.getRaw("stats_data_size")).isEqualTo(700L);
        assertThat(row.getString("stats_computed_at")).isNotBlank();
    }

    @Test
    void observedRowsReportNoCollectedStatistics() {
        TypedRow row = executor.executeShowTableControls(
                statement("SHOW TABLE CONTROLS IN test_cat.default.events"), context()).get(0);

        assertThat(row.getRaw("stats_version")).isEqualTo(-1L);
        assertThat(row.getRaw("stats_row_count")).isEqualTo(-1L);
        assertThat(row.getString("stats_computed_at")).isNull();
    }

    @Test
    void schemaScopeMergesLiveTablesAsObserved() {
        registry.tableControl().registerCreated("test_cat", "default", "orders", "alice",
                Optional.of(identity("orders")), Optional.of(version("orders")));

        List<TypedRow> rows = executor.executeShowTableControls(
                statement("SHOW TABLE CONTROLS IN test_cat.default"), context());

        assertThat(rows).extracting(row -> row.getString("table_name"))
                .containsExactly("events", "orders");
        TypedRow observed = rows.get(0);
        assertThat(observed.getString("ownership_mode")).isEqualTo("OBSERVED");
        assertThat(observed.getString("table_id")).isNull();
        assertThat(observed.getString("version_token")).isNull();
        assertThat(observed.getString("provenance")).isEqualTo("DISCOVERED");
        assertThat(rows.get(1).getString("ownership_mode")).isEqualTo("MANAGED");
    }

    @Test
    void singleTableScopeResolvesObservedIdentityAndVersionLive() {
        List<TypedRow> rows = executor.executeShowTableControls(
                statement("SHOW TABLE CONTROLS IN test_cat.default.events"), context());

        assertThat(rows).hasSize(1);
        TypedRow row = rows.get(0);
        assertThat(row.getString("ownership_mode")).isEqualTo("OBSERVED");
        assertThat(row.getString("format")).isEqualTo("ICEBERG");
        assertThat(row.getString("format_table_uuid")).isEqualTo("uuid-events");
        assertThat(row.getString("version_token")).isEqualTo("uuid-events:snapshot-1");
    }

    @Test
    void controlledRecordStaysVisibleWhenTheCatalogNoLongerHasTheTable() {
        registry.tableControl().registerCreated("test_cat", "default", "gone", "alice",
                Optional.of(new TableIdentity("ICEBERG", "uuid-gone", "s3://bucket/gone", "schema")),
                Optional.empty());

        List<TypedRow> rows = executor.executeShowTableControls(
                statement("SHOW TABLE CONTROLS IN test_cat.default"), context());

        assertThat(rows).extracting(row -> row.getString("table_name"))
                .containsExactly("events", "gone", "orders");
        TypedRow gone = rows.get(1);
        assertThat(gone.getString("ownership_mode")).isEqualTo("MANAGED");
        assertThat(gone.getString("state")).isEqualTo("ACTIVE");
    }

    private ShowTableControlsStatement statement(String sql) {
        return (ShowTableControlsStatement) parser.parse(sql);
    }

    private static TableIdentity identity(String table) {
        return new TableIdentity("ICEBERG", "uuid-" + table, "s3://bucket/" + table, "schema");
    }

    private static TableVersionToken version(String table) {
        return TableVersionToken.of("ICEBERG", "uuid-" + table, "snapshot-1");
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
            String key = ((Handle) handle).key;
            return Optional.of(TableVersionToken.of("ICEBERG", uuids.get(key), "snapshot-1"));
        }
    }

    private record Handle(String key) implements ConnectorTableHandle { }

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
