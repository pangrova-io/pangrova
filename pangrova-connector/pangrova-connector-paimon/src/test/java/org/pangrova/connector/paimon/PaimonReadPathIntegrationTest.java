package org.pangrova.connector.paimon;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.connector.paimon.handle.PaimonColumnHandle;
import org.pangrova.connector.paimon.handle.PaimonSplit;
import org.pangrova.connector.paimon.handle.PaimonTableHandle;
import org.pangrova.connector.paimon.handle.PaimonTableHandleFactory;
import org.pangrova.protocol.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.PaimonReadMode;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.ConnectorPageSource;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorSplit;
import org.pangrova.spi.connector.metadata.ConnectorSplitSource;
import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.spi.connector.metadata.DynamicFilter;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test for the Paimon read path using real
 * filesystem Catalog and Paimon tables with primary key deduplication.
 * Covers: listDatabases, listTables, getTableHandle, getTableMetadata,
 * getColumnHandles, getSplits, and PageSource data reading.
 */
class PaimonReadPathIntegrationTest {

    @TempDir
    static Path warehouse;

    private static Catalog catalog;
    private static PaimonMetadata metadata;
    private static PaimonSplitManager splitManager;
    private static PaimonPageSourceProvider pageSourceProvider;
    private static Table testTable;

    private static final String CATALOG_NAME = "paimon_read_test";
    private static final String DATABASE = "test_db";
    private static final String TABLE_NAME = "orders";
    private static final int ROW_COUNT = 5;

    private static final ConnectorTransactionHandle TX = ConnectorTransactionHandle.EMPTY;
    private static final DynamicFilter NO_FILTER = DynamicFilter.ALL;
    private static final Constraint NO_CONSTRAINT = Constraint.EMPTY;

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of(CATALOG_NAME); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    @BeforeAll
    static void setUp() throws Exception {
        catalog = buildCatalog();
        catalog.createDatabase(DATABASE, false);
        testTable = createTableWithData();
        metadata = new PaimonMetadata(catalog, CATALOG_NAME);
        org.pangrova.connector.paimon.PaimonSplitRegistry registry = new org.pangrova.connector.paimon.PaimonSplitRegistry();
        splitManager = new PaimonSplitManager(catalog, registry);
        pageSourceProvider = new PaimonPageSourceProvider(catalog, registry);
    }

    // -- metadata tests --

    @Test
    void listSchemaNamesReturnsCreatedDatabase() {
        List<String> schemas = metadata.listSchemaNames(SESSION);
        assertThat(schemas).contains(DATABASE);
    }

    @Test
    void listTablesReturnsCreatedTable() {
        List<SchemaTableName> tables = metadata.listTables(SESSION, Optional.of(DATABASE));
        assertThat(tables).contains(new SchemaTableName(DATABASE, TABLE_NAME));
    }

    @Test
    void getTableHandleReturnsValidHandle() {
        ConnectorTableHandle handle = metadata.getTableHandle(
                SESSION, new SchemaTableName(DATABASE, TABLE_NAME));

        assertThat(handle).isNotNull().isInstanceOf(PaimonTableHandle.class);
        PaimonTableHandle h = (PaimonTableHandle) handle;
        assertThat(h.schemaName()).isEqualTo(DATABASE);
        assertThat(h.tableName()).isEqualTo(TABLE_NAME);
        // Verify immutable handle contains metadata snapshot
        assertThat(h.tableLocation()).isNotBlank();
        assertThat(h.rowTypeJson()).isNotBlank();
    }

    @Test
    void getTableHandleReturnsNullForMissing() {
        ConnectorTableHandle handle = metadata.getTableHandle(
                SESSION, new SchemaTableName(DATABASE, "nonexistent"));
        assertThat(handle).isNull();
    }

    @Test
    void getTableMetadataReturnsCorrectColumnsAndTypes() {
        ConnectorTableHandle handle = loadTableHandle();
        ConnectorTableMetadata meta = metadata.getTableMetadata(SESSION, handle);

        assertThat(meta.table()).isEqualTo(new SchemaTableName(DATABASE, TABLE_NAME));
        assertThat(meta.columns()).containsExactly("id", "name", "amount");
        assertTypedColumn(meta.typedColumns().get(0), "id", PangrovaType.BIGINT);
        assertTypedColumn(meta.typedColumns().get(1), "name", PangrovaType.VARCHAR);
        assertTypedColumn(meta.typedColumns().get(2), "amount", PangrovaType.DOUBLE);
    }

    @Test
    void pinnedMetadataReadsActualSnapshotSchemaAfterSchemaEvolution() throws Exception {
        var id = Identifier.create(DATABASE, "historical_schema");
        catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT())
                .option("bucket", "1").option("bucket-key", "id").build(), false);
        var original = catalog.getTable(id);
        var builder = original.newBatchWriteBuilder();
        try (var writer = builder.newWrite(); var commit = builder.newCommit()) {
            writer.write(GenericRow.of(1L));
            commit.commit(writer.prepareCommit());
        }
        catalog.alterTable(id, List.of(org.apache.paimon.schema.SchemaChange.addColumn(
                "new_column", DataTypes.STRING())), false);
        var current = (PaimonTableHandle) metadata.getTableHandle(SESSION,
                new SchemaTableName(DATABASE, "historical_schema"));
        assertThat(current.tableLocation()).isEqualTo(
                ((org.apache.paimon.table.FileStoreTable) catalog.getTable(id)).location().toString());
        assertThat(metadata.getTableMetadata(SESSION, current).columns()).containsExactly("id", "new_column");
        var pinned = current.withTimeTravelSnapshot(1);
        assertThat(metadata.getTableMetadata(SESSION, pinned).columns()).containsExactly("id");
        assertThat(metadata.getColumnHandles(SESSION, pinned)).containsOnlyKeys("id");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> metadata.getTableMetadata(
                SESSION, current.withTimeTravelSnapshot(9999))).isInstanceOfSatisfying(ConnectorException.class,
                        error -> assertThat(error.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_SNAPSHOT_NOT_FOUND))
                .hasMessageContaining("Snapshot not found: snapshot-id=9999");
    }

    @Test
    void pinnedMetadataRejectsMissingHistoricalSchemaWithSnapshotError() throws Exception {
        var id = Identifier.create(DATABASE, "missing_historical_schema");
        catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT())
                .option("bucket", "1").option("bucket-key", "id").build(), false);
        var original = (org.apache.paimon.table.FileStoreTable) catalog.getTable(id);
        var builder = original.newBatchWriteBuilder();
        try (var writer = builder.newWrite(); var commit = builder.newCommit()) {
            writer.write(GenericRow.of(1L));
            commit.commit(writer.prepareCommit());
        }
        catalog.alterTable(id, List.of(org.apache.paimon.schema.SchemaChange.addColumn(
                "new_column", DataTypes.STRING())), false);
        var current = (PaimonTableHandle) metadata.getTableHandle(SESSION,
                new SchemaTableName(DATABASE, "missing_historical_schema"));
        var oldSchema = original.schemaManager().toSchemaPath(original.snapshotManager().snapshot(1).schemaId());
        original.fileIO().delete(oldSchema, false);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> metadata.getTableMetadata(
                SESSION, current.withTimeTravelSnapshot(1))).isInstanceOfSatisfying(ConnectorException.class,
                        error -> assertThat(error.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_SNAPSHOT_NOT_FOUND))
                .hasMessageContaining("Snapshot not found: snapshot-id=1").hasMessageContaining("schema-id=0 is absent");
    }

    @Test
    void getColumnHandlesReturnsCorrectHandles() {
        ConnectorTableHandle handle = loadTableHandle();
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(SESSION, handle);

        assertThat(columns).hasSize(3).containsKeys("id", "name", "amount");
        assertColumnHandle(columns.get("id"), "id", PangrovaType.BIGINT);
        assertColumnHandle(columns.get("name"), "name", PangrovaType.VARCHAR);
        assertColumnHandle(columns.get("amount"), "amount", PangrovaType.DOUBLE);
    }

    // -- split tests --

    @Test
    void getSplitsReturnsAtLeastOneSplit() {
        List<ConnectorSplit> splits = drainSplits();

        assertThat(splits).isNotEmpty();
        assertThat(splits.get(0)).isInstanceOf(PaimonSplit.class);
    }

    @Test
    void plannedPrimaryKeySplitCarriesCanonicalReadModeToNative() throws Exception {
        List<ConnectorSplit> splits = drainSplits();

        assertThat(splits).isNotEmpty();
        for (ConnectorSplit split : splits) {
            DataSplit wire = DataSplit.parseFrom(split.toSplitInfoProto());
            PaimonSplit paimon = (PaimonSplit) split;
            assertThat(paimon.snapshotId()).isPositive();
            assertThat(wire.getSourceSnapshot()).isEqualTo(Long.toString(paimon.snapshotId()));
            var fileUri = java.net.URI.create(paimon.primaryFilePath());
            Path filePath = fileUri.getScheme() == null ? Path.of(paimon.primaryFilePath()) : Path.of(fileUri);
            assertThat(wire.getLength()).isEqualTo(java.nio.file.Files.size(filePath));
            assertThat(paimon.primaryFile().format()).isEqualTo("parquet");
            assertThat(DataSplitProtoCodec.fileDetail(wire).getPaimon().getReadMode())
                    .isEqualTo(PaimonReadMode.PAIMON_READ_MODE_PRIMARY_KEY);
        }
    }

    // -- page source tests --

    @Test
    void pageSourceReadsCorrectRowCount() {
        List<Page> pages = readAllPages(allColumnHandles());

        long totalRows = pages.stream().mapToLong(Page::getPositionCount).sum();
        assertThat(totalRows).isEqualTo(ROW_COUNT);
    }

    @Test
    void pageSourceReadsCorrectData() {
        List<TypedRow> rows = readAllRows(allColumnHandles());

        assertThat(rows).hasSize(ROW_COUNT);
        // Paimon primary key table — rows ordered by primary key
        TypedRow firstRow = findRowById(rows, 0L);
        assertThat(firstRow).isNotNull();
        assertThat(firstRow.getRaw("name")).isEqualTo("row_0");
        assertThat(firstRow.getRaw("amount")).isEqualTo(0.0);
    }

    @Test
    void pageSourcePrimaryKeyDeduplication() throws Exception {
        // Write duplicate key — should overwrite previous value
        writeRow(testTable, 0L, "updated_0", 99.0);

        List<TypedRow> rows = readAllRows(allColumnHandles());
        TypedRow row = findRowById(rows, 0L);
        assertThat(row).isNotNull();
        assertThat(row.getRaw("name")).isEqualTo("updated_0");
        assertThat(row.getRaw("amount")).isEqualTo(99.0);
    }

    @Test
    void pageSourceIsFinishedAfterDrain() {
        PaimonSplit split = firstSplit();
        PaimonTableHandle handle = createTestTableHandle();

        ConnectorPageSource source = pageSourceProvider.createPageSource(
                TX, SESSION, split, handle, allColumnHandles(), NO_FILTER);
        drainSource(source);

        assertThat(source.isFinished()).isTrue();
        source.close();
    }

    // -- setup helpers --

    private static Catalog buildCatalog() {
        Options options = new Options();
        options.setString("warehouse", warehouse.toString());
        CatalogContext context = CatalogContext.create(options);
        return CatalogFactory.createCatalog(context);
    }

    private static Table createTableWithData() throws Exception {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("name", DataTypes.STRING())
                .column("amount", DataTypes.DOUBLE())
                .primaryKey("id")
                .option("bucket", "1")
                .build();
        Identifier id = Identifier.create(DATABASE, TABLE_NAME);
        catalog.createTable(id, schema, false);
        Table table = catalog.getTable(id);
        writeInitialData(table);
        return table;
    }

    private static void writeInitialData(Table table) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
             BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < ROW_COUNT; i++) {
                write.write(GenericRow.of((long) i, BinaryString.fromString("row_" + i), (double) i));
            }
            List<CommitMessage> messages = write.prepareCommit();
            commit.commit(messages);
        }
    }

    private static void writeRow(Table table, long id, String name, double amount) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
             BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(id, BinaryString.fromString(name), amount));
            commit.commit(write.prepareCommit());
        }
    }

    // -- read helpers --

    private ConnectorTableHandle loadTableHandle() {
        return metadata.getTableHandle(SESSION, new SchemaTableName(DATABASE, TABLE_NAME));
    }

    /**
     * Creates an immutable table handle from the test table using the factory.
     */
    private PaimonTableHandle createTestTableHandle() {
        return PaimonTableHandleFactory.fromTable(DATABASE, TABLE_NAME, testTable);
    }

    private List<ConnectorSplit> drainSplits() {
        PaimonTableHandle handle = createTestTableHandle();
        ConnectorSplitSource source = splitManager.getSplits(
                TX, SESSION, handle, NO_FILTER, NO_CONSTRAINT);
        List<ConnectorSplit> splits = source.getNextBatch(1000);
        source.close();
        return splits;
    }

    private PaimonSplit firstSplit() {
        return (PaimonSplit) drainSplits().get(0);
    }

    private List<Page> readAllPages(List<ColumnHandle> columns) {
        List<Page> allPages = new ArrayList<>();
        for (ConnectorSplit split : drainSplits()) {
            PaimonTableHandle handle = createTestTableHandle();
            ConnectorPageSource source = pageSourceProvider.createPageSource(
                    TX, SESSION, split, handle, columns, NO_FILTER);
            allPages.addAll(drainSource(source));
            source.close();
        }
        return allPages;
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
                new PaimonColumnHandle(0, "id", PangrovaType.BIGINT),
                new PaimonColumnHandle(1, "name", PangrovaType.VARCHAR),
                new PaimonColumnHandle(2, "amount", PangrovaType.DOUBLE));
    }

    private TypedRow findRowById(List<TypedRow> rows, long id) {
        return rows.stream()
                .filter(r -> Long.valueOf(id).equals(r.getRaw("id")))
                .findFirst()
                .orElse(null);
    }

    // -- assertion helpers --

    private void assertTypedColumn(ConnectorTableMetadata.Column col, String name, PangrovaType type) {
        assertThat(col.name()).isEqualTo(name);
        assertThat(col.type()).isEqualTo(type);
    }

    private void assertColumnHandle(ColumnHandle handle, String name, PangrovaType type) {
        assertThat(handle).isInstanceOf(PaimonColumnHandle.class);
        PaimonColumnHandle col = (PaimonColumnHandle) handle;
        assertThat(col.name()).isEqualTo(name);
        assertThat(col.type()).isEqualTo(type);
    }
}
