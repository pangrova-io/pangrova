package org.pangrova.coordinator.index.ddl;

import org.pangrova.indexing.build.DataFileInfo;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.metadata.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.sync.SyncResult;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.index.statistics.DataFileStatistics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link ConnectorAccessorResolver#resolveDataFileInfos} enriches
 * file paths with actual row count, file size, and inferred format from connector
 * file statistics.
 *
 * <p>Regression test for F-INDEX-08: all coordinator build paths previously
 * constructed {@code DataFileInfo} with hardcoded {@code rowCount=0} and
 * {@code sizeBytes=0}, losing connector-provided metadata.</p>
 */
@DisplayName("ConnectorAccessorResolver DataFileInfo enrichment (F-INDEX-08)")
class ConnectorAccessorResolverDataFileInfoTest {

    private static final String CATALOG = "test-catalog";
    private static final String SCHEMA = "db";
    private static final String TABLE = "orders";

    private ConnectorAccessorResolver resolver;

    @BeforeEach
    void setUp() {
        CatalogDescriptor descriptor = new CatalogDescriptor(
                CATALOG, CatalogType.CONNECTOR, "test-provider",
                CatalogState.ACTIVE, Map.of(), "", null);

        ConnectorHandleFactory factory = desc ->
                new ConnectorCatalogHandle(desc, new StubConnector());

        MetadataStore metadataStore = new StubMetadataStore(descriptor);
        CatalogRegistry registry = new CatalogRegistry(
                metadataStore, factory, null, null);
        registry.initialize();

        resolver = new ConnectorAccessorResolver(registry);
    }

    @Test
    @DisplayName("resolveDataFileInfos populates rowCount and sizeBytes from file statistics")
    void enrichesWithActualMetadata() {
        List<String> paths = List.of(
                "/data/orders_001.parquet",
                "/data/orders_002.parquet");

        List<DataFileInfo> result = resolver.resolveDataFileInfos(
                CATALOG, SCHEMA, TABLE, paths);

        assertThat(result).hasSize(2);

        DataFileInfo first = result.get(0);
        assertThat(first.filePath()).isEqualTo("/data/orders_001.parquet");
        assertThat(first.rowCount()).isEqualTo(50_000L);
        assertThat(first.sizeBytes()).isEqualTo(4_096_000L);
        assertThat(first.format()).isEqualTo("parquet");

        DataFileInfo second = result.get(1);
        assertThat(second.filePath()).isEqualTo("/data/orders_002.parquet");
        assertThat(second.rowCount()).isEqualTo(75_000L);
        assertThat(second.sizeBytes()).isEqualTo(6_144_000L);
        assertThat(second.format()).isEqualTo("parquet");
    }

    @Test
    @DisplayName("resolveDataFileInfos falls back to zero for files not in statistics")
    void fallsBackToZeroForMissingStatistics() {
        List<String> paths = List.of("/data/unknown_file.parquet");

        List<DataFileInfo> result = resolver.resolveDataFileInfos(
                CATALOG, SCHEMA, TABLE, paths);

        assertThat(result).hasSize(1);
        DataFileInfo info = result.get(0);
        assertThat(info.filePath()).isEqualTo("/data/unknown_file.parquet");
        assertThat(info.rowCount()).isZero();
        assertThat(info.sizeBytes()).isZero();
        assertThat(info.format()).isEqualTo("parquet");
    }

    @Test
    @DisplayName("resolveDataFileInfos infers format from file extension")
    void infersFormatFromExtension() {
        List<String> paths = List.of(
                "/data/file.parquet",
                "/data/file.orc",
                "/data/file.avro",
                "/data/file.csv");

        List<DataFileInfo> result = resolver.resolveDataFileInfos(
                CATALOG, SCHEMA, TABLE, paths);

        assertThat(result).extracting(DataFileInfo::format)
                .containsExactly("parquet", "orc", "avro", "unknown");
    }

    @Test
    @DisplayName("listDataFilesDetailed returns enriched DataFileInfo list")
    void listDataFilesDetailedReturnsEnrichedInfo() {
        List<DataFileInfo> result = resolver.listDataFilesDetailed(
                CATALOG, SCHEMA, TABLE);

        assertThat(result).isNotEmpty();
        // The stub connector lists the two parquet files with known stats
        assertThat(result).allSatisfy(info -> {
            assertThat(info.rowCount()).isGreaterThan(0);
            assertThat(info.sizeBytes()).isGreaterThan(0);
            assertThat(info.format()).isEqualTo("parquet");
        });
    }

    @Test
    @DisplayName("resolveDataFileInfos returns empty for empty input")
    void emptyInputReturnsEmpty() {
        List<DataFileInfo> result = resolver.resolveDataFileInfos(
                CATALOG, SCHEMA, TABLE, List.of());

        assertThat(result).isEmpty();
    }

    // ── Stubs ──

    /**
     * Stub Connector that returns a metadata implementation providing known file statistics.
     */
    static final class StubConnector implements Connector {

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return new StubConnectorMetadata();
        }

        @Override
        public ConnectorSplitManager getSplitManager() {
            return null;
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() {
            return null;
        }
    }

    /**
     * Stub ConnectorMetadata that returns known file statistics and data file list.
     */
    static final class StubConnectorMetadata implements ConnectorMetadata {

        private static final List<DataFileStatistics> KNOWN_STATS = List.of(
                new DataFileStatistics("/data/orders_001.parquet", "1",
                        50_000L, 4_096_000L, List.of()),
                new DataFileStatistics("/data/orders_002.parquet", "1",
                        75_000L, 6_144_000L, List.of()));

        @Override
        public List<DataFileStatistics> getFileStatistics(
                String catalogName, String schemaName, String tableName,
                List<String> dataFilePaths) {
            return KNOWN_STATS.stream()
                    .filter(s -> dataFilePaths.contains(s.dataFilePath()))
                    .toList();
        }

        @Override
        public List<String> listDataFiles(
                String catalogName, String schemaName, String tableName) {
            return List.of("/data/orders_001.parquet", "/data/orders_002.parquet");
        }

        // ── Required abstract methods with no-op implementations ──
        @Override
        public org.pangrova.spi.connector.metadata.table.ConnectorTableHandle getTableHandle(
                ConnectorSession session,
                org.pangrova.spi.connector.metadata.table.SchemaTableName tableName) {
            return null;
        }

        @Override
        public org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata getTableMetadata(
                ConnectorSession session,
                org.pangrova.spi.connector.metadata.table.ConnectorTableHandle table) {
            return null;
        }

        @Override
        public Map<String, org.pangrova.spi.connector.metadata.table.ColumnHandle> getColumnHandles(
                ConnectorSession session,
                org.pangrova.spi.connector.metadata.table.ConnectorTableHandle table) {
            return Map.of();
        }

        @Override
        public org.pangrova.spi.connector.metadata.table.TableStatistics getTableStatistics(
                ConnectorSession session,
                org.pangrova.spi.connector.metadata.table.ConnectorTableHandle table) {
            return null;
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of();
        }

        @Override
        public List<org.pangrova.spi.connector.metadata.table.SchemaTableName> listTables(
                ConnectorSession session, Optional<String> schemaName) {
            return List.of();
        }
    }

    /**
     * Minimal MetadataStore that provides the test catalog descriptor.
     */
    static final class StubMetadataStore implements MetadataStore {
        private final CatalogDescriptor catalogDescriptor;

        StubMetadataStore(CatalogDescriptor catalogDescriptor) {
            this.catalogDescriptor = catalogDescriptor;
        }

        @Override
        public List<CatalogDescriptor> listAllCatalogDescriptors() {
            return List.of(catalogDescriptor);
        }

        @Override public void saveTable(TableMetadata m) {}
        @Override public Optional<TableMetadata> loadTable(String c, String s, String t) { return Optional.empty(); }
        @Override public void deleteTable(String c, String s, String t) {}
        @Override public List<QualifiedTableRef> searchTables(String p) { return List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {}
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) { return Optional.empty(); }
        @Override public void deleteCatalogDescriptor(String n) {}
        @Override public Optional<Instant> getLastSyncTime(String c) { return Optional.empty(); }
        @Override public void updateSyncTime(String c, Instant t) {}
        @Override public void recordSyncHistory(SyncResult r) {}
        @Override public void saveSchemas(String c, List<String> s) {}
    }
}
