package org.pangrova.coordinator.integration.connector.paimon.write;

import org.pangrova.connector.paimon.write.PaimonPageSinkProvider;
import org.pangrova.connector.paimon.PaimonMetadata;
import org.pangrova.testfixtures.tableformat.PaimonCommitTestSupport;
import org.pangrova.connector.paimon.PaimonPageSourceProvider;
import org.pangrova.connector.paimon.PaimonSplitManager;
import org.pangrova.connector.paimon.handle.PaimonColumnHandle;
import org.pangrova.connector.paimon.handle.PaimonTableHandle;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.batch.api.page.Page;
import org.pangrova.types.PangrovaType;
import org.pangrova.connector.spi.data.ConnectorPageSink;
import org.pangrova.connector.spi.data.ConnectorPageSource;
import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.spi.metadata.ConnectorSplitSource;
import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.metadata.DynamicFilter;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
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
 * Integration tests for the Paimon write path using real filesystem Catalog.
 * Covers: createTable, beginInsert, PageSink write, and read-back verification
 * for both primary key and append-only tables.
 */
class PaimonWritePathIntegrationTest {

    @TempDir
    static Path warehouse;

    private static Catalog catalog;
    private static PaimonMetadata metadata;
    private static PaimonSplitManager splitManager;
    private static PaimonPageSinkProvider pageSinkProvider;
    private static PaimonPageSourceProvider pageSourceProvider;

    private static final String CATALOG_NAME = "paimon_write_test";
    private static final String DATABASE = "write_db";

    private static final ConnectorTransactionHandle TX = ConnectorTransactionHandle.empty();
    private static final DynamicFilter NO_FILTER = DynamicFilter.ALL;
    private static final Constraint NO_CONSTRAINT = Constraint.EMPTY;

    private static final org.pangrova.coordinator.task.TestConnectorQuery QUERY =
            new org.pangrova.coordinator.task.TestConnectorQuery("paimon-write", "test", "test-tenant", Map.of());
    private static final ConnectorSession SESSION = QUERY.session(CATALOG_NAME, DATABASE);

    private static final List<ConnectorTableMetadata.Column> PK_COLUMNS = List.of(
            new ConnectorTableMetadata.Column("id", PangrovaType.BIGINT, OptionalInt.of(0)),
            new ConnectorTableMetadata.Column("name", PangrovaType.VARCHAR, OptionalInt.of(1)),
            new ConnectorTableMetadata.Column("amount", PangrovaType.DOUBLE, OptionalInt.of(2)));

    private static final List<ConnectorTableMetadata.Column> APPEND_COLUMNS = List.of(
            new ConnectorTableMetadata.Column("seq", PangrovaType.BIGINT, OptionalInt.of(0)),
            new ConnectorTableMetadata.Column("message", PangrovaType.VARCHAR, OptionalInt.of(1)));

    @BeforeAll
    static void setUp() throws Exception {
        catalog = buildCatalog();
        catalog.createDatabase(DATABASE, false);
        org.pangrova.connector.paimon.PaimonSplitRegistry registry = new org.pangrova.connector.paimon.PaimonSplitRegistry();
        metadata = new PaimonMetadata(catalog, CATALOG_NAME, registry);
        metadata.beginQuery(QUERY.authority());
        splitManager = new PaimonSplitManager(catalog, registry);
        pageSinkProvider = new PaimonPageSinkProvider(catalog);
        pageSourceProvider = new PaimonPageSourceProvider(registry);
    }


    @org.junit.jupiter.api.AfterAll
    static void tearDown() throws Exception {
        QUERY.seal();
        metadata.cleanupQuery(QUERY.authority());
        QUERY.retire();
        catalog.close();
    }

    // -- primary key table tests --

    @Test
    void insertAndReadBack_primaryKeyTable() {
        ConnectorOutputTableHandle outputHandle = createPrimaryKeyTable("pk_round_trip");
        writeRows(outputHandle, buildPkRows(0, 5));

        List<TypedRow> rows = readAllRows("pk_round_trip", pkColumnHandles());
        assertThat(rows).hasSize(5);
        assertPkRow(findRowById(rows, 0L), 0L, "row_0", 0.0);
        assertPkRow(findRowById(rows, 4L), 4L, "row_4", 4.0);
    }

    @Test
    void upsertSemantics_latestWins() {
        ConnectorOutputTableHandle outputHandle = createPrimaryKeyTable("pk_upsert");
        writeRows(outputHandle, buildPkRows(0, 3));

        ConnectorOutputTableHandle insertHandle = beginInsert("pk_upsert");
        writeRows(insertHandle, List.of(
                TypedRow.builder().putLong("id", 0L).putString("name", "updated_0").putRaw("amount", 99.0).build(),
                TypedRow.builder().putLong("id", 1L).putString("name", "updated_1").putRaw("amount", 88.0).build()));

        List<TypedRow> rows = readAllRows("pk_upsert", pkColumnHandles());
        assertThat(rows).hasSize(3);
        assertPkRow(findRowById(rows, 0L), 0L, "updated_0", 99.0);
        assertPkRow(findRowById(rows, 1L), 1L, "updated_1", 88.0);
        assertPkRow(findRowById(rows, 2L), 2L, "row_2", 2.0);
    }

    // -- append-only table tests --

    @Test
    void insertAndReadBack_appendOnlyTable() {
        ConnectorOutputTableHandle outputHandle = createAppendOnlyTable("append_round_trip");
        writeRows(outputHandle, buildAppendRows(0, 4));

        List<TypedRow> rows = readAllRows("append_round_trip", appendColumnHandles());
        assertThat(rows).hasSize(4);
    }

    @Test
    void multipleAppends_allDataVisible() {
        ConnectorOutputTableHandle outputHandle = createAppendOnlyTable("append_multi");
        writeRows(outputHandle, buildAppendRows(0, 3));

        ConnectorOutputTableHandle insertHandle = beginInsert("append_multi");
        writeRows(insertHandle, buildAppendRows(3, 6));

        List<TypedRow> rows = readAllRows("append_multi", appendColumnHandles());
        assertThat(rows).hasSize(6);
    }

    // -- schema verification --

    @Test
    void createTable_schemaMatchesMetadata() {
        createPrimaryKeyTable("schema_verify");

        ConnectorTableHandle handle = loadTableHandle("schema_verify");
        ConnectorTableMetadata meta = metadata.getTableMetadata(SESSION, handle);

        assertThat(meta.table()).isEqualTo(new SchemaTableName(DATABASE, "schema_verify"));
        assertThat(meta.columns()).containsExactly("id", "name", "amount");
        assertTypedColumn(meta.typedColumns().get(0), "id", PangrovaType.BIGINT);
        assertTypedColumn(meta.typedColumns().get(1), "name", PangrovaType.VARCHAR);
        assertTypedColumn(meta.typedColumns().get(2), "amount", PangrovaType.DOUBLE);
    }

    // -- setup helpers --

    private static Catalog buildCatalog() {
        Options options = new Options();
        options.setString("warehouse", warehouse.toString());
        CatalogContext context = CatalogContext.create(options);
        return CatalogFactory.createCatalog(context);
    }

    // -- table creation helpers --

    private ConnectorOutputTableHandle createPrimaryKeyTable(String tableName) {
        SchemaTableName schemaTable = new SchemaTableName(DATABASE, tableName);
        ConnectorTableMetadata tableMeta = new ConnectorTableMetadata(
                schemaTable, List.of("id", "name", "amount"), PK_COLUMNS,
                Map.of("primary-key", "id", "bucket", "1"));
        return metadata.createTable(SESSION, tableMeta);
    }

    private ConnectorOutputTableHandle createAppendOnlyTable(String tableName) {
        SchemaTableName schemaTable = new SchemaTableName(DATABASE, tableName);
        ConnectorTableMetadata tableMeta = new ConnectorTableMetadata(
                schemaTable, List.of("seq", "message"), APPEND_COLUMNS, Map.of());
        return metadata.createTable(SESSION, tableMeta);
    }

    private ConnectorOutputTableHandle beginInsert(String tableName) {
        ConnectorTableHandle handle = loadTableHandle(tableName);
        return metadata.beginInsert(SESSION, handle);
    }

    // -- write helpers --

    private void writeRows(ConnectorOutputTableHandle outputHandle, List<TypedRow> rows) {
        ConnectorPageSink sink = pageSinkProvider.createPageSink(TX, SESSION, outputHandle);
        List<String> columns = rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
        sink.appendPage(new Page(columns, rows));
        byte[] commitMetadata = sink.finishWithMetadata();
        PaimonCommitTestSupport.finish(metadata, SESSION, outputHandle, List.of(commitMetadata));
        sink.close();
    }

    private List<TypedRow> buildPkRows(int startInclusive, int endExclusive) {
        List<TypedRow> rows = new ArrayList<>();
        for (int i = startInclusive; i < endExclusive; i++) {
            rows.add(TypedRow.builder()
                    .putLong("id", i)
                    .putString("name", "row_" + i)
                    .putRaw("amount", (double) i)
                    .build());
        }
        return rows;
    }

    private List<TypedRow> buildAppendRows(int startInclusive, int endExclusive) {
        List<TypedRow> rows = new ArrayList<>();
        for (int i = startInclusive; i < endExclusive; i++) {
            rows.add(TypedRow.builder()
                    .putLong("seq", i)
                    .putString("message", "msg_" + i)
                    .build());
        }
        return rows;
    }

    // -- read helpers --

    private ConnectorTableHandle loadTableHandle(String tableName) {
        return metadata.getTableHandle(SESSION, new SchemaTableName(DATABASE, tableName));
    }

    private List<TypedRow> readAllRows(String tableName, List<ColumnHandle> columns) {
        PaimonTableHandle handle = (PaimonTableHandle) loadTableHandle(tableName);
        List<ConnectorSplit> splits = drainSplits(handle);
        List<TypedRow> allRows = new ArrayList<>();
        for (ConnectorSplit split : splits) {
            ConnectorPageSource source = pageSourceProvider.createPageSource(
                    TX, SESSION, split, handle, columns, NO_FILTER);
            allRows.addAll(drainRows(source));
            source.close();
        }
        return allRows;
    }

    private List<ConnectorSplit> drainSplits(PaimonTableHandle handle) {
        ConnectorSplitSource source = splitManager.getSplits(
                TX, SESSION, handle, NO_FILTER, NO_CONSTRAINT);
        List<ConnectorSplit> splits = source.getNextBatch(1000);
        source.close();
        return splits;
    }

    private List<TypedRow> drainRows(ConnectorPageSource source) {
        List<TypedRow> rows = new ArrayList<>();
        while (!source.isFinished()) {
            Page page = source.getNextPage();
            if (page != null) {
                rows.addAll(page.rows());
            }
        }
        return rows;
    }

    // -- column handle helpers --

    private List<ColumnHandle> pkColumnHandles() {
        return List.of(
                new PaimonColumnHandle(0, "id", PangrovaType.BIGINT),
                new PaimonColumnHandle(1, "name", PangrovaType.VARCHAR),
                new PaimonColumnHandle(2, "amount", PangrovaType.DOUBLE));
    }

    private List<ColumnHandle> appendColumnHandles() {
        return List.of(
                new PaimonColumnHandle(0, "seq", PangrovaType.BIGINT),
                new PaimonColumnHandle(1, "message", PangrovaType.VARCHAR));
    }

    // -- assertion helpers --

    private TypedRow findRowById(List<TypedRow> rows, long id) {
        return rows.stream()
                .filter(r -> Long.valueOf(id).equals(r.getRaw("id")))
                .findFirst()
                .orElse(null);
    }

    private void assertPkRow(TypedRow row, long id, String name, double amount) {
        assertThat(row).isNotNull();
        assertThat(row.getRaw("id")).isEqualTo(id);
        assertThat(row.getRaw("name")).isEqualTo(name);
        assertThat(row.getRaw("amount")).isEqualTo(amount);
    }

    private void assertTypedColumn(ConnectorTableMetadata.Column col, String name, PangrovaType type) {
        assertThat(col.name()).isEqualTo(name);
        assertThat(col.type()).isEqualTo(type);
    }
}
