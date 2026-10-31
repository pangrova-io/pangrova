package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.handle.IcebergColumnHandle;
import org.pangrova.connector.iceberg.handle.IcebergSplit;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandleFactory;
import org.pangrova.connector.iceberg.read.IcebergExpressionToSubstrait;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.ConnectorTransactionHandle;
import org.pangrova.connector.spi.data.page.Page;
import org.pangrova.connector.spi.data.type.PangrovaType;
import org.pangrova.connector.spi.data.ConnectorPageSource;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.connector.spi.metadata.ConnectorSplit;
import org.pangrova.connector.spi.metadata.ConnectorSplitSource;
import org.pangrova.connector.spi.metadata.Constraint;
import org.pangrova.connector.spi.metadata.DynamicFilter;
import org.pangrova.connector.spi.metadata.table.ColumnHandle;
import org.pangrova.connector.spi.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.spi.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test for the Iceberg read path using real
 * HadoopCatalog and Parquet files on local filesystem.
 * Covers: listSchemaNames, listTables, getTableHandle, getTableMetadata,
 * getColumnHandles, getSplits, and PageSource data reading.
 */
class IcebergReadPathIntegrationTest {

    @TempDir
    static Path warehouse;

    private static Catalog catalog;
    private static IcebergMetadata metadata;
    private static IcebergSplitManager splitManager;
    private static IcebergPageSourceProvider pageSourceProvider;
    private static Table testTable;

    private static final String CATALOG_NAME = "read_path_catalog";
    private static final String NAMESPACE = "test_db";
    private static final String TABLE_NAME = "orders";
    private static final int ROW_COUNT = 5;

    private static final ConnectorTransactionHandle TX = ConnectorTransactionHandle.empty();
    private static final DynamicFilter NO_FILTER = DynamicFilter.ALL;
    private static final Constraint NO_CONSTRAINT = Constraint.EMPTY;

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.connector.spi.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of(CATALOG_NAME); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "amount", Types.DoubleType.get()));

    @BeforeAll
    static void setUp() throws IOException {
        catalog = buildHadoopCatalog();
        createNamespace();
        testTable = createTableWithData();
        metadata = new IcebergMetadata(catalog, CATALOG_NAME);
        splitManager = new IcebergSplitManager(CATALOG_NAME, catalog, new IcebergExpressionToSubstrait());
        pageSourceProvider = new IcebergPageSourceProvider(catalog);
    }

    // -- metadata tests --

    @Test
    void listSchemaNames_returnsCreatedNamespace() {
        List<String> schemas = metadata.listSchemaNames(SESSION);
        assertThat(schemas).contains(NAMESPACE);
    }

    @Test
    void listTables_returnsCreatedTable() {
        List<SchemaTableName> tables = metadata.listTables(SESSION, Optional.of(NAMESPACE));
        assertThat(tables).contains(new SchemaTableName(NAMESPACE, TABLE_NAME));
    }

    @Test
    void getTableHandle_returnsValidHandle() {
        ConnectorTableHandle handle = metadata.getTableHandle(
                SESSION, new SchemaTableName(NAMESPACE, TABLE_NAME));

        assertThat(handle).isNotNull().isInstanceOf(IcebergTableHandle.class);
        IcebergTableHandle h = (IcebergTableHandle) handle;
        assertThat(h.schemaName()).isEqualTo(NAMESPACE);
        assertThat(h.tableName()).isEqualTo(TABLE_NAME);
        assertThat(h.tableLocation()).isNotBlank();
        assertThat(h.schemaJson()).isNotBlank();
    }

    @Test
    void getTableMetadata_returnsCorrectColumnsAndTypes() {
        ConnectorTableHandle handle = loadTableHandle();
        ConnectorTableMetadata meta = metadata.getTableMetadata(SESSION, handle);

        assertThat(meta.table()).isEqualTo(new SchemaTableName(NAMESPACE, TABLE_NAME));
        assertThat(meta.columns()).containsExactly("id", "name", "amount");
        assertTypedColumn(meta.typedColumns().get(0), "id", PangrovaType.INTEGER);
        assertTypedColumn(meta.typedColumns().get(1), "name", PangrovaType.VARCHAR);
        assertTypedColumn(meta.typedColumns().get(2), "amount", PangrovaType.DOUBLE);
    }

    @Test
    void getColumnHandles_returnsCorrectHandles() {
        ConnectorTableHandle handle = loadTableHandle();
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(SESSION, handle);

        assertThat(columns).hasSize(3).containsKeys("id", "name", "amount");
        assertColumnHandle(columns.get("id"), "id", PangrovaType.INTEGER);
        assertColumnHandle(columns.get("name"), "name", PangrovaType.VARCHAR);
        assertColumnHandle(columns.get("amount"), "amount", PangrovaType.DOUBLE);
    }

    // -- split tests --

    @Test
    void getSplits_returnsAtLeastOneSplit() {
        List<ConnectorSplit> splits = drainSplits();

        assertThat(splits).isNotEmpty();
        IcebergSplit split = (IcebergSplit) splits.get(0);
        assertThat(split.filePath()).endsWith(".parquet");
        assertThat(split.fileFormat()).isEqualTo("PARQUET");
        assertThat(split.fileLength()).isGreaterThan(0);
    }

    // -- page source tests --

    @Test
    void pageSource_readsCorrectRowCount() {
        List<Page> pages = readAllPages(allColumnHandles());

        long totalRows = pages.stream().mapToLong(Page::getPositionCount).sum();
        assertThat(totalRows).isEqualTo(ROW_COUNT);
    }

    @Test
    void pageSource_readsCorrectData() {
        List<TypedRow> rows = readAllRows(allColumnHandles());

        assertThat(rows).hasSize(ROW_COUNT);
        assertThat(rows.get(0).getRaw("id")).isEqualTo(0);
        assertThat(rows.get(0).getRaw("name")).isEqualTo("row_0");
        assertThat(rows.get(0).getRaw("amount")).isEqualTo(0.0);
        assertThat(rows.get(2).getRaw("id")).isEqualTo(2);
        assertThat(rows.get(2).getRaw("name")).isEqualTo("row_2");
    }

    @Test
    void pageSource_appliesColumnProjection() {
        List<ColumnHandle> projected = List.of(
                new IcebergColumnHandle(2, "name", PangrovaType.VARCHAR));

        List<Page> pages = readAllPages(projected);
        long totalRows = pages.stream().mapToLong(Page::getPositionCount).sum();

        assertThat(totalRows).isEqualTo(ROW_COUNT);
        assertThat(pages.get(0).columns()).containsExactly("name");
    }

    @Test
    void pageSource_isFinishedAfterDrain() {
        IcebergSplit split = firstSplit();
        IcebergTableHandle handle = createTestTableHandle();

        ConnectorPageSource source = pageSourceProvider.createPageSource(
                TX, SESSION, split, handle, allColumnHandles(), NO_FILTER);
        drainSource(source);

        assertThat(source.isFinished()).isTrue();
        source.close();
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

    private static Table createTableWithData() throws IOException {
        TableIdentifier id = TableIdentifier.of(NAMESPACE, TABLE_NAME);
        Table table = catalog.createTable(id, TEST_SCHEMA);
        writeTestData(table);
        return table;
    }

    private static void writeTestData(Table table) throws IOException {
        String path = table.location() + "/data/test-data.parquet";
        OutputFile outputFile = table.io().newOutputFile(path);
        DataWriter<Record> writer = buildParquetWriter(outputFile);
        try (writer) {
            writeRows(writer);
        }
        table.newAppend().appendFile(writer.toDataFile()).commit();
    }

    private static DataWriter<Record> buildParquetWriter(OutputFile outputFile)
            throws IOException {
        return Parquet.writeData(outputFile)
                .schema(TEST_SCHEMA)
                .createWriterFunc(GenericParquetWriter::create)
                .overwrite()
                .withSpec(org.apache.iceberg.PartitionSpec.unpartitioned())
                .build();
    }

    private static void writeRows(DataWriter<Record> writer) {
        for (int i = 0; i < ROW_COUNT; i++) {
            GenericRecord record = GenericRecord.create(TEST_SCHEMA);
            record.setField("id", i);
            record.setField("name", "row_" + i);
            record.setField("amount", (double) i);
            writer.write(record);
        }
    }

    // -- read helpers --

    private ConnectorTableHandle loadTableHandle() {
        return metadata.getTableHandle(SESSION, new SchemaTableName(NAMESPACE, TABLE_NAME));
    }

    private List<ConnectorSplit> drainSplits() {
        IcebergTableHandle handle = createTestTableHandle();
        ConnectorSplitSource source = splitManager.getSplits(
                TX, SESSION, handle, NO_FILTER, NO_CONSTRAINT);
        List<ConnectorSplit> splits = source.getNextBatch(1000);
        source.close();
        return splits;
    }

    private IcebergSplit firstSplit() {
        return (IcebergSplit) drainSplits().get(0);
    }

    private IcebergTableHandle createTestTableHandle() {
        return IcebergTableHandleFactory.fromTable(NAMESPACE, TABLE_NAME, testTable);
    }

    private List<Page> readAllPages(List<ColumnHandle> columns) {
        IcebergSplit split = firstSplit();
        IcebergTableHandle handle = createTestTableHandle();
        ConnectorPageSource source = pageSourceProvider.createPageSource(
                TX, SESSION, split, handle, columns, NO_FILTER);
        List<Page> pages = drainSource(source);
        source.close();
        return pages;
    }

    private List<TypedRow> readAllRows(List<ColumnHandle> columns) {
        return readAllPages(columns).stream()
                .flatMap(p -> p.rows().stream())
                .toList();
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

    private void assertTypedColumn(ConnectorTableMetadata.Column col, String name, PangrovaType type) {
        assertThat(col.name()).isEqualTo(name);
        assertThat(col.type()).isEqualTo(type);
    }

    private void assertColumnHandle(ColumnHandle handle, String name, PangrovaType type) {
        assertThat(handle).isInstanceOf(IcebergColumnHandle.class);
        IcebergColumnHandle col = (IcebergColumnHandle) handle;
        assertThat(col.name()).isEqualTo(name);
        assertThat(col.type()).isEqualTo(type);
    }
}
