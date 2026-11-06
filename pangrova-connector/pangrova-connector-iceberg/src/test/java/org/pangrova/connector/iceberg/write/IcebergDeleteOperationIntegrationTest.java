package org.pangrova.connector.iceberg.write;

import org.pangrova.connector.iceberg.IcebergMetadata;
import org.pangrova.connector.iceberg.IcebergSplitManager;
import org.pangrova.connector.iceberg.read.IcebergExpressionToSubstrait;
import org.pangrova.connector.iceberg.handle.IcebergSplit;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.api.error.ConnectorException;
import org.pangrova.batch.api.page.Page;
import org.pangrova.types.PangrovaType;
import org.pangrova.connector.spi.data.ConnectorPageSink;
import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.spi.metadata.ConnectorSplitSource;
import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.metadata.DynamicFilter;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for Iceberg v2 delete operations (position + equality deletes).
 * Uses real HadoopCatalog with Parquet files on local filesystem.
 */
class IcebergDeleteOperationIntegrationTest {

    @TempDir
    static Path warehouse;

    private static Catalog catalog;
    private static IcebergMetadata metadata;
    private static IcebergSplitManager splitManager;
    private static IcebergPageSinkProvider pageSinkProvider;

    private static final String CATALOG_NAME = "delete_test_catalog";
    private static final String NAMESPACE = "delete_db";

    private static final ConnectorTransactionHandle TX = ConnectorTransactionHandle.empty();
    private static final DynamicFilter NO_FILTER = DynamicFilter.ALL;
    private static final Constraint NO_CONSTRAINT = Constraint.EMPTY;

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of(CATALOG_NAME); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of("tenant_id", "delete-operation-test"); }
    };

    private static final List<ConnectorTableMetadata.Column> TEST_COLUMNS = List.of(
            new ConnectorTableMetadata.Column("id", PangrovaType.BIGINT, OptionalInt.of(1)),
            new ConnectorTableMetadata.Column("name", PangrovaType.VARCHAR, OptionalInt.of(2)));

    @BeforeAll
    static void setUp() {
        catalog = buildHadoopCatalog();
        createNamespace();
        metadata = new IcebergMetadata(catalog, CATALOG_NAME);
        splitManager = new IcebergSplitManager(CATALOG_NAME, catalog, new IcebergExpressionToSubstrait());
        pageSinkProvider = new IcebergPageSinkProvider(catalog);
    }

    @Test
    void positionDelete_deleteFileAssociatedWithDataFile() throws IOException {
        ConnectorOutputTableHandle outputHandle = createTestTable("pos_delete_table");
        writeRows(outputHandle, buildRows(0, 5));

        String dataFilePath = findDataFilePath("pos_delete_table");
        Table table = loadIcebergTable("pos_delete_table");

        DeleteFile deleteFile = IcebergDeleteWriter.buildPositionDeleteFile(
                table, dataFilePath, List.of(1L, 3L));
        table.newRowDelta().addDeletes(deleteFile).commit();

        // Verify delete files are associated with the data file split
        List<IcebergSplit> splits = findSplits("pos_delete_table");
        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).deleteFiles()).hasSize(1);

        // Verify position delete file content: rows at positions 1,3 marked for deletion
        String deleteFilePath = splits.get(0).deleteFiles().get(0).filePath();
        List<Record> deleteRecords = readDeleteFile(table, deleteFilePath);
        assertThat(deleteRecords).hasSize(2);
        assertPositionDeleteRecord(deleteRecords.get(0), dataFilePath, 1L);
        assertPositionDeleteRecord(deleteRecords.get(1), dataFilePath, 3L);
    }

    @Test
    void equalityDelete_deleteFileCommittedToTable() throws IOException {
        ConnectorOutputTableHandle outputHandle = createTestTable("eq_delete_table");
        writeRows(outputHandle, buildRows(0, 5));

        Table table = loadIcebergTable("eq_delete_table");
        Schema fullSchema = table.schema();
        int idFieldId = fullSchema.findField("id").fieldId();
        Schema deleteSchema = new Schema(fullSchema.findField("id"));

        List<Record> deleteRecords = buildEqualityDeleteRecords(deleteSchema, List.of(1L, 3L));
        DeleteFile eqDeleteFile = IcebergDeleteWriter.buildEqualityDeleteFile(
                table, new int[]{idFieldId}, deleteSchema, deleteRecords);
        table.newRowDelta().addDeletes(eqDeleteFile).commit();

        // Verify the equality delete file is committed to the table
        List<DeleteFile> deleteFiles = new ArrayList<>();
        table.currentSnapshot().addedDeleteFiles(table.io()).forEach(deleteFiles::add);
        assertThat(deleteFiles).hasSize(1);

        // Verify equality delete file content: id values 1 and 3
        List<Record> readBack = readEqualityDeleteFile(table, deleteFiles.get(0).path().toString());
        assertThat(readBack).hasSize(2);
        assertThat(readBack.get(0).getField("id")).isEqualTo(1L);
        assertThat(readBack.get(1).getField("id")).isEqualTo(3L);

        // Reads fail closed instead of silently ignoring unqualified equality deletes
        assertThatThrownBy(() -> findSplits("eq_delete_table"))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("equality deletes are not supported");
    }

    // -- catalog setup --

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
                schemaTable, List.of("id", "name"), TEST_COLUMNS, Map.of());
        return metadata.createTable(SESSION, tableMeta);
    }

    private void writeRows(ConnectorOutputTableHandle handle, List<Map<String, Object>> rows) {
        try (ConnectorPageSink sink = pageSinkProvider.createPageSink(TX, SESSION, handle)) {
            List<TypedRow> typedRows = rows.stream().map(TypedRow::fromRawMap).toList();
            sink.appendPage(new Page(List.of("id", "name"), typedRows));
            byte[] commitMetadata = sink.finishWithMetadata();
            metadata.finishDistributedWrite(SESSION, handle, List.of(commitMetadata));
        }
    }

    private List<Map<String, Object>> buildRows(int startInclusive, int endExclusive) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = startInclusive; i < endExclusive; i++) {
            rows.add(Map.of("id", (long) i, "name", "row_" + i));
        }
        return rows;
    }

    // -- read helpers --

    private String findDataFilePath(String tableName) {
        List<IcebergSplit> splits = findSplits(tableName);
        assertThat(splits).isNotEmpty();
        return splits.get(0).filePath();
    }

    private List<IcebergSplit> findSplits(String tableName) {
        IcebergTableHandle handle = (IcebergTableHandle) loadTableHandle(tableName);
        ConnectorSplitSource source = splitManager.getSplits(
                TX, SESSION, handle, NO_FILTER, NO_CONSTRAINT);
        List<ConnectorSplit> splits = source.getNextBatch(100);
        source.close();
        return splits.stream().map(IcebergSplit.class::cast).toList();
    }

    private Table loadIcebergTable(String tableName) {
        IcebergTableHandle handle = (IcebergTableHandle) loadTableHandle(tableName);
        TableIdentifier id = TableIdentifier.of(handle.schemaName(), handle.tableName());
        return catalog.loadTable(id);
    }

    private ConnectorTableHandle loadTableHandle(String tableName) {
        return metadata.getTableHandle(SESSION, new SchemaTableName(NAMESPACE, tableName));
    }

    private List<Record> readDeleteFile(Table table, String deleteFilePath) throws IOException {
        InputFile inputFile = table.io().newInputFile(deleteFilePath);
        Schema posDeleteSchema = positionDeleteSchema();
        return drainRecords(inputFile, posDeleteSchema);
    }

    private List<Record> readEqualityDeleteFile(Table table, String path) throws IOException {
        InputFile inputFile = table.io().newInputFile(path);
        Schema eqSchema = new Schema(table.schema().findField("id"));
        return drainRecords(inputFile, eqSchema);
    }

    private List<Record> drainRecords(InputFile file, Schema schema) throws IOException {
        List<Record> result = new ArrayList<>();
        try (CloseableIterable<Record> iterable = Parquet.read(file)
                .project(schema)
                .createReaderFunc(fs -> GenericParquetReaders.buildReader(schema, fs))
                .build()) {
            iterable.forEach(result::add);
        }
        return result;
    }

    // -- assertion helpers --

    private void assertPositionDeleteRecord(Record record, String expectedPath, long expectedPos) {
        assertThat(record.getField("file_path")).isEqualTo(expectedPath);
        assertThat(record.getField("pos")).isEqualTo(expectedPos);
    }

    private Schema positionDeleteSchema() {
        return new Schema(
                org.apache.iceberg.types.Types.NestedField.required(
                        2147483546, "file_path", org.apache.iceberg.types.Types.StringType.get()),
                org.apache.iceberg.types.Types.NestedField.required(
                        2147483545, "pos", org.apache.iceberg.types.Types.LongType.get()));
    }

    private List<Record> buildEqualityDeleteRecords(Schema deleteSchema, List<Long> ids) {
        List<Record> records = new ArrayList<>();
        for (long id : ids) {
            GenericRecord record = GenericRecord.create(deleteSchema);
            record.setField("id", id);
            records.add(record);
        }
        return records;
    }
}
