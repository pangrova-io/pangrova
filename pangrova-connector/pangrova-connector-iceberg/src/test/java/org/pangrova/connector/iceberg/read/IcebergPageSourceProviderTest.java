package org.pangrova.connector.iceberg.read;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.connector.iceberg.HadoopTestGuard;
import org.pangrova.connector.iceberg.IcebergPageSourceProvider;
import org.pangrova.connector.iceberg.handle.IcebergColumnHandle;
import org.pangrova.connector.iceberg.handle.IcebergDeleteFile;
import org.pangrova.connector.iceberg.handle.IcebergSplit;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandleFactory;
import org.pangrova.connector.iceberg.write.IcebergDeleteWriter;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.spi.connector.data.page.BatchFormat;
import org.pangrova.spi.connector.data.page.NativePageConverter;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.data.page.NativePage;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.ConnectorPageSource;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.DynamicFilter;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for IcebergPageSourceProvider using real Parquet files.
 * Validates: Requirement 30.7 — createPageSource produces real data.
 */
class IcebergPageSourceProviderTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;
    private IcebergPageSourceProvider provider;

    private static final ConnectorTransactionHandle TX = new ConnectorTransactionHandle() {};
    private static final DynamicFilter NO_FILTER = DynamicFilter.ALL;
    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of("test"); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "amount", Types.DoubleType.get()));

    @BeforeEach
    void setUp() {
        HadoopTestGuard.assumeHadoopCompatible();
        catalog = createHadoopCatalog();
        provider = new IcebergPageSourceProvider(catalog);
    }

    @Test
    void readsAllColumnsFromParquetFile() throws IOException {
        Table table = createTableWithData("db", "orders", 3);
        IcebergSplit split = firstSplit(table);
        List<ColumnHandle> allColumns = allColumnHandles();

        List<Page> pages = drainPages("db", "orders", table, split, allColumns);

        long totalRows = pages.stream().mapToLong(Page::getPositionCount).sum();
        assertThat(totalRows).isEqualTo(3);
        assertThat(pages.get(0).columns()).containsExactly("id", "name", "amount");
    }

    @Test
    void appliesColumnProjection() throws IOException {
        Table table = createTableWithData("db", "projected", 5);
        IcebergSplit split = firstSplit(table);
        List<ColumnHandle> projected = List.of(
                new IcebergColumnHandle(1, "id", PangrovaType.BIGINT));

        List<Page> pages = drainPages("db", "projected", table, split, projected);

        long totalRows = pages.stream().mapToLong(Page::getPositionCount).sum();
        assertThat(totalRows).isEqualTo(5);
        assertThat(pages.get(0).columns()).containsExactly("id");
    }

    @Test
    void returnsCorrectValues() throws IOException {
        Table table = createTableWithData("db", "values", 2);
        IcebergSplit split = firstSplit(table);
        List<ColumnHandle> allColumns = allColumnHandles();

        List<Page> pages = drainPages("db", "values", table, split, allColumns);
        List<TypedRow> allRows = flattenRows(pages);

        assertThat(allRows).hasSize(2);
        assertThat(allRows.get(0).getRaw("id")).isEqualTo(0L);
        assertThat(allRows.get(0).getRaw("name")).isEqualTo("row_0");
        assertThat(allRows.get(0).getRaw("amount")).isEqualTo(0.0);
        assertThat(allRows.get(1).getRaw("id")).isEqualTo(1L);
    }

    @Test
    void singleRowFile() throws IOException {
        Table table = createTableWithData("db", "single", 1);
        IcebergSplit split = firstSplit(table);
        List<ColumnHandle> allColumns = allColumnHandles();

        List<Page> pages = drainPages("db", "single", table, split, allColumns);

        long totalRows = pages.stream().mapToLong(Page::getPositionCount).sum();
        assertThat(totalRows).isEqualTo(1);
    }

    @Test
    void pageSourceIsFinishedAfterDrain() throws IOException {
        Table table = createTableWithData("db", "finished", 1);
        IcebergSplit split = firstSplit(table);
        List<ColumnHandle> allColumns = allColumnHandles();
        IcebergTableHandle handle = IcebergTableHandleFactory.fromTable("db", "finished", table);

        ConnectorPageSource source = provider.createPageSource(
                TX, SESSION, split, handle, allColumns, NO_FILTER);

        while (!source.isFinished()) {
            source.getNextPage();
        }
        assertThat(source.isFinished()).isTrue();
        source.close();
    }

    @Test
    void nativePageContainsCorrectRowData() throws IOException {
        Table table = createTableWithData("db", "native_page", 3);
        IcebergSplit split = firstSplit(table);
        List<ColumnHandle> allColumns = allColumnHandles();
        IcebergTableHandle handle = IcebergTableHandleFactory.fromTable("db", "native_page", table);

        ConnectorPageSource source = provider.createPageSource(
                TX, SESSION, split, handle, allColumns, NO_FILTER);

        List<NativePage> nativePages = new ArrayList<>();
        while (!source.isFinished()) {
            NativePage page = source.getNextNativePage();
            if (page != null) {
                nativePages.add(page);
            }
        }
        source.close();

        assertThat(nativePages).isNotEmpty();
        NativePage firstPage = nativePages.get(0);
        assertThat(firstPage.format()).isEqualTo(BatchFormat.CUSTOM_BINARY);
        assertThat(firstPage.serializedBatch()).isNotEmpty();
        assertThat(firstPage.serializedBatch().length).isGreaterThan(0);

        Page deserialized = NativePageConverter.toPage(firstPage);
        assertThat(deserialized.getPositionCount()).isGreaterThan(0);
        assertThat(deserialized.columns()).containsExactly("id", "name", "amount");

        List<TypedRow> allRows = nativePages.stream()
                .map(NativePageConverter::toPage)
                .flatMap(p -> p.rows().stream())
                .toList();
        assertThat(allRows).hasSize(3);
        assertThat(allRows.get(0).getRaw("id")).isEqualTo(0L);
        assertThat(allRows.get(0).getRaw("name")).isEqualTo("row_0");
        assertThat(allRows.get(0).getRaw("amount")).isEqualTo(0.0);
        assertThat(allRows.get(1).getRaw("id")).isEqualTo(1L);
        assertThat(allRows.get(2).getRaw("id")).isEqualTo(2L);
    }

    @Test
    void equalityDeleteFailsClosedInsteadOfReturningDeletedRows() throws IOException {
        Table table = createTableWithData("db", "equality_delete", 3);
        FileScanTask dataTask = table.newScan().planFiles().iterator().next();

        Schema deleteSchema = new Schema(table.schema().findField("id"));
        GenericRecord deletedRow = GenericRecord.create(deleteSchema);
        deletedRow.setField("id", 1L);
        DeleteFile equalityDelete = IcebergDeleteWriter.buildEqualityDeleteFile(
                table,
                new int[] {table.schema().findField("id").fieldId()},
                deleteSchema,
                List.of(deletedRow));
        table.newRowDelta().addDeletes(equalityDelete).commit();
        table.refresh();

        DeleteFile committedDelete = table.newScan().planFiles().iterator().next()
                .deletes().get(0);
        IcebergDeleteFile descriptor = new IcebergDeleteFile(
                committedDelete.path().toString(),
                "EQUALITY",
                List.of("id"),
                committedDelete.recordCount(),
                committedDelete.dataSequenceNumber());
        IcebergSplit split = IcebergSplit.builder(
                        dataTask.file().path().toString(),
                        dataTask.start(),
                        dataTask.length(),
                        "PARQUET")
                .tableName("db.equality_delete")
                .snapshotId(table.currentSnapshot().snapshotId())
                .deleteFiles(List.of(descriptor))
                .build();
        IcebergTableHandle handle =
                IcebergTableHandleFactory.fromTable("db", "equality_delete", table);

        assertThatThrownBy(() -> provider.createPageSource(
                TX, SESSION, split, handle, allColumnHandles(), NO_FILTER))
                .isInstanceOfSatisfying(ConnectorException.class, error -> {
                    assertThat(error.getErrorCode())
                            .isEqualTo(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION);
                    assertThat(error.getMessage())
                            .contains("equality deletes")
                            .contains("db.equality_delete")
                            .contains(descriptor.filePath());
                });
    }

    // -- helpers --

    private List<Page> drainPages(String namespace, String tableName, Table table, IcebergSplit split, List<ColumnHandle> columns) {
        IcebergTableHandle handle = IcebergTableHandleFactory.fromTable(namespace, tableName, table);
        ConnectorPageSource source = provider.createPageSource(
                TX, SESSION, split, handle, columns, NO_FILTER);
        List<Page> pages = new ArrayList<>();
        while (!source.isFinished()) {
            Page page = source.getNextPage();
            if (page != null) {
                pages.add(page);
            }
        }
        source.close();
        return pages;
    }

    private List<TypedRow> flattenRows(List<Page> pages) {
        return pages.stream()
                .flatMap(p -> p.rows().stream())
                .toList();
    }

    private Table createTableWithData(String ns, String tableName, int rowCount)
            throws IOException {
        createNamespace(ns);
        TableIdentifier id = TableIdentifier.of(ns, tableName);
        Table table = catalog.createTable(id, TEST_SCHEMA);
        if (rowCount > 0) {
            writeParquetData(table, rowCount);
        }
        return table;
    }

    private void writeParquetData(Table table, int rowCount) throws IOException {
        String dataPath = table.location() + "/data/test-data.parquet";
        OutputFile outputFile = table.io().newOutputFile(dataPath);

        DataWriter<Record> writer = Parquet.writeData(outputFile)
                .schema(TEST_SCHEMA)
                .createWriterFunc(GenericParquetWriter::create)
                .overwrite()
                .withSpec(org.apache.iceberg.PartitionSpec.unpartitioned())
                .build();

        try (writer) {
            for (int i = 0; i < rowCount; i++) {
                GenericRecord record = GenericRecord.create(TEST_SCHEMA);
                record.setField("id", (long) i);
                record.setField("name", "row_" + i);
                record.setField("amount", (double) i);
                writer.write(record);
            }
        }

        DataFile dataFile = writer.toDataFile();
        table.newAppend().appendFile(dataFile).commit();
    }

    private IcebergSplit firstSplit(Table table) {
        FileScanTask task = table.newScan().planFiles().iterator().next();
        return IcebergSplit.builder(
                        task.file().path().toString(),
                        task.start(),
                        task.length(),
                        task.file().format().name())
                .build();
    }

    private List<ColumnHandle> allColumnHandles() {
        return List.of(
                new IcebergColumnHandle(1, "id", PangrovaType.BIGINT),
                new IcebergColumnHandle(2, "name", PangrovaType.VARCHAR),
                new IcebergColumnHandle(3, "amount", PangrovaType.DOUBLE));
    }

    private Catalog createHadoopCatalog() {
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        return org.apache.iceberg.CatalogUtil.buildIcebergCatalog(
                "test", props, new org.apache.hadoop.conf.Configuration());
    }

    private void createNamespace(String name) {
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            try {
                nsCatalog.createNamespace(Namespace.of(name));
            } catch (org.apache.iceberg.exceptions.AlreadyExistsException ignored) {
            }
        }
    }
}
