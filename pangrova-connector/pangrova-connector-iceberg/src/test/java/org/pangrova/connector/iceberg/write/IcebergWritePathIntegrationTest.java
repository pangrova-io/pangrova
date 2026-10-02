package org.pangrova.connector.iceberg.write;

import org.pangrova.connector.iceberg.IcebergMetadata;
import org.pangrova.connector.iceberg.IcebergPageSourceProvider;
import org.pangrova.connector.iceberg.IcebergSplitManager;
import org.pangrova.connector.iceberg.read.IcebergExpressionToSubstrait;
import org.pangrova.connector.iceberg.handle.IcebergColumnHandle;
import org.pangrova.connector.iceberg.handle.IcebergSplit;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.ConnectorPageSink;
import org.pangrova.spi.connector.data.ConnectorPageSource;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.ConnectorSplit;
import org.pangrova.spi.connector.metadata.ConnectorSplitSource;
import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.spi.connector.metadata.DynamicFilter;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the Iceberg write path using real HadoopCatalog
 * and Parquet files on local filesystem.
 * Covers: createTable, beginInsert, PageSink write, and read-back verification.
 */
class IcebergWritePathIntegrationTest {

    @TempDir
    static Path warehouse;

    private static Catalog catalog;
    private static IcebergMetadata metadata;
    private static IcebergSplitManager splitManager;
    private static IcebergPageSinkProvider pageSinkProvider;
    private static IcebergPageSourceProvider pageSourceProvider;

    private static final String CATALOG_NAME = "write_path_catalog";
    private static final String NAMESPACE = "write_db";

    private static final ConnectorTransactionHandle TX = ConnectorTransactionHandle.EMPTY;
    private static final DynamicFilter NO_FILTER = DynamicFilter.ALL;
    private static final Constraint NO_CONSTRAINT = Constraint.EMPTY;

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of(CATALOG_NAME); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of("tenant_id", "write-path-test"); }
    };

    private static final List<ConnectorTableMetadata.Column> TEST_COLUMNS = List.of(
            new ConnectorTableMetadata.Column("id", PangrovaType.INTEGER, OptionalInt.of(1)),
            new ConnectorTableMetadata.Column("name", PangrovaType.VARCHAR, OptionalInt.of(2)),
            new ConnectorTableMetadata.Column("amount", PangrovaType.DOUBLE, OptionalInt.of(3)));

    @BeforeAll
    static void setUp() {
        catalog = buildHadoopCatalog();
        createNamespace();
        metadata = new IcebergMetadata(catalog, CATALOG_NAME);
        splitManager = new IcebergSplitManager(CATALOG_NAME, catalog, new IcebergExpressionToSubstrait());
        pageSinkProvider = new IcebergPageSinkProvider(catalog);
        pageSourceProvider = new IcebergPageSourceProvider(catalog);
    }

    // -- test methods --

    @Test
    void insertAndReadBack_roundTrip() {
        ConnectorOutputTableHandle outputHandle = createTestTable("round_trip_table");
        writeRows(outputHandle, buildRows(0, 5));

        List<TypedRow> rows = readAllRows("round_trip_table");
        assertThat(rows).hasSize(5);
        assertRowValues(rows.get(0), 0, "row_0", 0.0);
        assertRowValues(rows.get(4), 4, "row_4", 4.0);
    }

    @Test
    void createTable_createsTableWithCorrectSchema() {
        createTestTable("schema_check_table");

        ConnectorTableHandle handle = loadTableHandle("schema_check_table");
        ConnectorTableMetadata meta = metadata.getTableMetadata(SESSION, handle);

        assertThat(meta.table()).isEqualTo(new SchemaTableName(NAMESPACE, "schema_check_table"));
        assertThat(meta.columns()).containsExactly("id", "name", "amount");
        assertTypedColumn(meta.typedColumns().get(0), "id", PangrovaType.INTEGER);
        assertTypedColumn(meta.typedColumns().get(1), "name", PangrovaType.VARCHAR);
        assertTypedColumn(meta.typedColumns().get(2), "amount", PangrovaType.DOUBLE);
    }

    @Test
    void multipleAppends_allDataVisible() {
        ConnectorOutputTableHandle outputHandle = createTestTable("multi_append_table");
        writeRows(outputHandle, buildRows(0, 3));

        ConnectorOutputTableHandle insertHandle = beginInsert("multi_append_table");
        writeRows(insertHandle, buildRows(3, 6));

        List<TypedRow> rows = readAllRows("multi_append_table");
        assertThat(rows).hasSize(6);
        List<TypedRow> sorted = rows.stream()
                .sorted(java.util.Comparator.comparingInt(r -> (int) r.getRaw("id")))
                .toList();
        assertRowValues(sorted.get(0), 0, "row_0", 0.0);
        assertRowValues(sorted.get(5), 5, "row_5", 5.0);
    }

    // -- setup helpers --

    private static Catalog buildHadoopCatalog() {
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        return org.apache.iceberg.CatalogUtil.buildIcebergCatalog(
                CATALOG_NAME, props, new org.apache.hadoop.conf.Configuration());
    }

    private static void createNamespace() {
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            nsCatalog.createNamespace(Namespace.of(NAMESPACE));
        }
    }

    // -- write helpers --

    private ConnectorOutputTableHandle createTestTable(String tableName) {
        SchemaTableName schemaTable = new SchemaTableName(NAMESPACE, tableName);
        ConnectorTableMetadata tableMeta = new ConnectorTableMetadata(
                schemaTable, List.of("id", "name", "amount"), TEST_COLUMNS, Map.of());
        return metadata.createTable(SESSION, tableMeta);
    }

    private ConnectorOutputTableHandle beginInsert(String tableName) {
        ConnectorTableHandle handle = loadTableHandle(tableName);
        return metadata.beginInsert(SESSION, handle);
    }

    private void writeRows(ConnectorOutputTableHandle outputHandle, List<TypedRow> rows) {
        try (ConnectorPageSink sink = pageSinkProvider.createPageSink(TX, SESSION, outputHandle)) {
            Page page = new Page(List.of("id", "name", "amount"), rows);
            sink.appendPage(page);
            byte[] commitMetadata = sink.finishWithMetadata();
            metadata.finishDistributedWrite(SESSION, outputHandle, List.of(commitMetadata));
        }
    }

    private List<TypedRow> buildRows(int startInclusive, int endExclusive) {
        List<TypedRow> rows = new ArrayList<>();
        for (int i = startInclusive; i < endExclusive; i++) {
            rows.add(TypedRow.builder()
                    .putInt("id", i)
                    .putString("name", "row_" + i)
                    .putRaw("amount", (double) i)
                    .build());
        }
        return rows;
    }

    // -- read helpers --

    private ConnectorTableHandle loadTableHandle(String tableName) {
        return metadata.getTableHandle(SESSION, new SchemaTableName(NAMESPACE, tableName));
    }

    private List<TypedRow> readAllRows(String tableName) {
        return readAllPages(tableName).stream()
                .flatMap(p -> p.rows().stream())
                .toList();
    }

    private List<Page> readAllPages(String tableName) {
        IcebergTableHandle handle = (IcebergTableHandle) loadTableHandle(tableName);
        List<ConnectorSplit> splits = drainSplits(handle);
        List<Page> allPages = new ArrayList<>();
        for (ConnectorSplit split : splits) {
            allPages.addAll(readPagesFromSplit((IcebergSplit) split, handle));
        }
        return allPages;
    }

    private List<ConnectorSplit> drainSplits(IcebergTableHandle handle) {
        ConnectorSplitSource source = splitManager.getSplits(
                TX, SESSION, handle, NO_FILTER, NO_CONSTRAINT);
        List<ConnectorSplit> splits = source.getNextBatch(1000);
        source.close();
        return splits;
    }

    private List<Page> readPagesFromSplit(IcebergSplit split, IcebergTableHandle handle) {
        ConnectorPageSource source = pageSourceProvider.createPageSource(
                TX, SESSION, split, handle, allColumnHandles(), NO_FILTER);
        List<Page> pages = drainSource(source);
        source.close();
        return pages;
    }

    private List<Page> drainSource(ConnectorPageSource source) {
        List<Page> pages = new ArrayList<>();
        while (!source.isFinished()) {
            Page page = source.getNextPage();
            if (page != null) {
                pages.add(page);
            }
        }
        return pages;
    }

    private List<ColumnHandle> allColumnHandles() {
        return List.of(
                new IcebergColumnHandle(1, "id", PangrovaType.INTEGER),
                new IcebergColumnHandle(2, "name", PangrovaType.VARCHAR),
                new IcebergColumnHandle(3, "amount", PangrovaType.DOUBLE));
    }

    // -- assertion helpers --

    private void assertRowValues(TypedRow row, int id, String name, double amount) {
        assertThat(row.getRaw("id")).isEqualTo(id);
        assertThat(row.getRaw("name")).isEqualTo(name);
        assertThat(row.getRaw("amount")).isEqualTo(amount);
    }

    private void assertTypedColumn(ConnectorTableMetadata.Column col, String name, PangrovaType type) {
        assertThat(col.name()).isEqualTo(name);
        assertThat(col.type()).isEqualTo(type);
    }
}
