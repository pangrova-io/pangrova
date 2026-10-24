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
import org.pangrova.metadata.control.TableControlRecord;
import org.pangrova.metadata.control.TableControlService;
import org.pangrova.metadata.control.TableControlSettings;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.sync.SyncResult;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.statement.maintenance.AnalyzeStatement;
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
import org.pangrova.spi.security.AccessDeniedException;

import java.time.Instant;
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

class AnalyzeExecutorTest {

    private TestMetadata metadata;
    private CatalogRegistry registry;
    private AnalyzeExecutor executor;

    @BeforeEach
    void setUp() {
        metadata = new TestMetadata();
        metadata.create("default", "orders", "uuid-orders");
        ConnectorHandleFactory factory = descriptor ->
                new ConnectorCatalogHandle(descriptor, new TestConnector(metadata));
        registry = new CatalogRegistry(new EmptyMetadataStore(), factory, null, null);
        registry.register(new CatalogDescriptor(
                "test_cat", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));
        registry.setTableControlService(new TableControlService(
                new InMemoryTableControlStore(),
                new TableControlSettings(true, true, Set.of())));
        executor = new AnalyzeExecutor(registry);
    }

    @Test
    void observedTablesAreRejectedBeforeStatisticsAreRead() {
        assertThatThrownBy(() -> executor.execute(analyze("orders"), context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("OBSERVED");
        assertThat(metadata.statisticsReads).isZero();
    }

    @Test
    void collectedStatisticsAreBoundToTheStableTableAndObservedVersion() {
        TableControlRecord record = managed("orders", version("v1"));
        metadata.statistics = new TableStatistics(42, 840);

        List<TypedRow> rows = executor.execute(analyze("orders"), context());

        assertThat(metadata.statisticsReads).isEqualTo(1);
        assertThat(rows).singleElement().satisfies(row -> {
            assertThat(row.getString("table")).isEqualTo("default.orders");
            assertThat(row.getRaw("row_count")).isEqualTo(42L);
            assertThat(row.getRaw("data_size")).isEqualTo(840L);
            assertThat(row.getString("version_token")).isEqualTo("uuid:v1");
            assertThat(row.getRaw("stats_version")).isEqualTo(0L);
        });
        var collected = registry.tableStatistics().fresh("test_cat", "default", "orders")
                .orElseThrow();
        assertThat(collected.tableId()).isEqualTo(record.tableId());
        assertThat(collected.versionToken()).isEqualTo("uuid:v1");
    }

    @Test
    void statisticsThatAdvanceTheRecordedVersionAreNotBornStale() {
        managed("orders", version("v1"));
        metadata.version = () -> Optional.of(version("v2"));
        metadata.statistics = new TableStatistics(42, 840);

        executor.execute(analyze("orders"), context());

        assertThat(registry.tableControl().find("test_cat", "default", "orders")).get()
                .extracting(TableControlRecord::versionToken)
                .isEqualTo("uuid:v2");
        assertThat(registry.tableStatistics().fresh("test_cat", "default", "orders"))
                .hasValueSatisfying(stats ->
                        assertThat(stats.versionToken()).isEqualTo("uuid:v2"));
    }

    @Test
    void aConcurrentCommitDuringCollectionAbandonsTheRun() {
        managed("orders", version("v1"));
        metadata.version = sequence("v1", "v2");
        metadata.statistics = new TableStatistics(42, 840);

        assertThatThrownBy(() -> executor.execute(analyze("orders"), context()))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("changed during collection");
        assertThat(registry.tableStatistics().fresh("test_cat", "default", "orders")).isEmpty();
    }

    @Test
    void unknownConnectorStatisticsAreRejectedInsteadOfPersisted() {
        managed("orders", version("v1"));
        metadata.statistics = TableStatistics.UNKNOWN;

        assertThatThrownBy(() -> executor.execute(analyze("orders"), context()))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("no statistics");
        assertThat(registry.tableStatistics().fresh("test_cat", "default", "orders")).isEmpty();
    }

    @Test
    void statisticsAreRejectedWhenTheConnectorCannotObserveAVersion() {
        managed("orders", version("v1"));
        metadata.version = Optional::empty;
        metadata.statistics = new TableStatistics(42, 840);

        assertThatThrownBy(() -> executor.execute(analyze("orders"), context()))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("cannot observe a version");
        assertThat(registry.tableStatistics().fresh("test_cat", "default", "orders")).isEmpty();
    }

    private TableControlRecord managed(String table, TableVersionToken version) {
        return registry.tableControl().registerCreated("test_cat", "default", table, "alice",
                Optional.of(new TableIdentity("ICEBERG", "uuid-" + table,
                        "s3://bucket/" + table, "schema")),
                Optional.of(version)).orElseThrow();
    }

    private static AnalyzeStatement analyze(String table) {
        return new AnalyzeStatement(Identifier.of("test_cat", "default", table));
    }

    private static TableVersionToken version(String value) {
        return TableVersionToken.of("ICEBERG", "uuid", value);
    }

    private static Supplier<Optional<TableVersionToken>> sequence(String first, String second) {
        AtomicInteger calls = new AtomicInteger();
        return () -> Optional.of(version(calls.getAndIncrement() == 0 ? first : second));
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
        private Supplier<Optional<TableVersionToken>> version =
                () -> Optional.of(TableVersionToken.of("ICEBERG", "uuid", "v1"));
        private TableStatistics statistics = TableStatistics.UNKNOWN;
        private int statisticsReads;

        void create(String schema, String table, String uuid) {
            SchemaTableName name = new SchemaTableName(schema, table);
            tables.put(key(name), new ConnectorTableMetadata(name, List.of("id"), Map.of()));
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
            statisticsReads++;
            return statistics;
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
                    "ICEBERG", "uuid-" + key, "s3://bucket/" + key, "schema"));
        }

        @Override
        public Optional<TableVersionToken> observeVersion(ConnectorSession session,
                ConnectorTableHandle handle) {
            return version.get();
        }
    }

    private record Handle(String key) implements ConnectorTableHandle { }

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
