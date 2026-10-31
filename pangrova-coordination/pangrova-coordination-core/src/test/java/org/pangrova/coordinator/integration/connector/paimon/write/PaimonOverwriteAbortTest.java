package org.pangrova.coordinator.integration.connector.paimon.write;

import org.pangrova.connector.paimon.write.PaimonPageSinkProvider;
import org.pangrova.connector.paimon.PaimonMetadata;
import org.pangrova.testfixtures.tableformat.PaimonCommitTestSupport;
import org.pangrova.connector.paimon.PaimonPageSourceProvider;
import org.pangrova.connector.paimon.PaimonSplitManager;
import org.pangrova.connector.paimon.PaimonSplitRegistry;
import org.pangrova.connector.paimon.handle.PaimonColumnHandle;
import org.pangrova.connector.paimon.handle.PaimonTableHandle;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.ConnectorTransactionHandle;
import org.pangrova.connector.spi.data.ConnectorPageSink;
import org.pangrova.connector.spi.data.ConnectorPageSource;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.connector.spi.data.page.Page;
import org.pangrova.connector.spi.data.type.PangrovaType;
import org.pangrova.connector.spi.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.spi.metadata.ConnectorSplit;
import org.pangrova.connector.spi.metadata.ConnectorSplitSource;
import org.pangrova.connector.spi.metadata.Constraint;
import org.pangrova.connector.spi.metadata.DynamicFilter;
import org.pangrova.connector.spi.metadata.table.ColumnHandle;
import org.pangrova.connector.spi.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.spi.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;
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
 * Tests for Paimon overwrite semantics and abort resource cleanup.
 * Validates: P2-04 from metadata-connector review.
 */
class PaimonOverwriteAbortTest {

    @TempDir
    static Path warehouse;

    private static Catalog catalog;
    private static PaimonMetadata metadata;
    private static PaimonSplitManager splitManager;
    private static PaimonPageSinkProvider pageSinkProvider;
    private static PaimonPageSourceProvider pageSourceProvider;

    private static final String DATABASE = "overwrite_db";
    private static final ConnectorTransactionHandle TX = ConnectorTransactionHandle.empty();
    private static final DynamicFilter NO_FILTER = DynamicFilter.ALL;
    private static final Constraint NO_CONSTRAINT = Constraint.EMPTY;

    private static final org.pangrova.coordinator.task.TestConnectorQuery QUERY =
            new org.pangrova.coordinator.task.TestConnectorQuery("paimon-overwrite", "test", "test-tenant", Map.of());
    private static final ConnectorSession SESSION = QUERY.session("test", DATABASE);

    private static final List<ConnectorTableMetadata.Column> COLUMNS = List.of(
            new ConnectorTableMetadata.Column("id", PangrovaType.BIGINT, OptionalInt.of(0)),
            new ConnectorTableMetadata.Column("val", PangrovaType.VARCHAR, OptionalInt.of(1)));

    @BeforeAll
    static void setUp() throws Exception {
        Options options = new Options();
        options.setString("warehouse", warehouse.toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase(DATABASE, false);
        PaimonSplitRegistry registry = new PaimonSplitRegistry();
        metadata = new PaimonMetadata(catalog, "test", registry);
        metadata.beginQuery(QUERY.authority());
        splitManager = new PaimonSplitManager(catalog, registry);
        pageSinkProvider = new PaimonPageSinkProvider(catalog);
        pageSourceProvider = new PaimonPageSourceProvider(registry);
    }

    @Test
    void overwrite_replacesAllExistingData() {
        createAndPopulate("overwrite_tbl", buildRows(0, 5));

        ConnectorTableHandle handle = loadHandle("overwrite_tbl");
        ConnectorOutputTableHandle overwriteHandle = metadata.beginOverwrite(SESSION, handle);
        ConnectorPageSink sink = pageSinkProvider.createOverwritePageSink(TX, SESSION, overwriteHandle);
        sink.appendPage(new Page(List.of("id", "val"), buildRows(10, 12)));
        byte[] commitMetadata = sink.finishWithMetadata();
        PaimonCommitTestSupport.finish(metadata, SESSION, overwriteHandle, List.of(commitMetadata));
        sink.close();

        List<TypedRow> rows = readAll("overwrite_tbl");
        assertThat(rows).hasSize(2);
        assertThat(rows.stream().map(r -> r.getRaw("id")))
                .containsExactlyInAnyOrder(10L, 11L);
    }

    @Test
    void abort_discardsUncommittedData() {
        createAndPopulate("abort_tbl", buildRows(0, 3));

        ConnectorTableHandle handle = loadHandle("abort_tbl");
        ConnectorOutputTableHandle insertHandle = metadata.beginInsert(SESSION, handle);
        ConnectorPageSink sink = pageSinkProvider.createPageSink(TX, SESSION, insertHandle);
        sink.appendPage(new Page(List.of("id", "val"), buildRows(100, 105)));
        sink.abort();
        sink.close();

        List<TypedRow> rows = readAll("abort_tbl");
        assertThat(rows).hasSize(3)
                .as("Aborted rows must not be visible");
    }

    @Test
    void close_afterAbort_isIdempotent() {
        createAndPopulate("idempotent_tbl", buildRows(0, 1));

        ConnectorTableHandle handle = loadHandle("idempotent_tbl");
        ConnectorOutputTableHandle insertHandle = metadata.beginInsert(SESSION, handle);
        ConnectorPageSink sink = pageSinkProvider.createPageSink(TX, SESSION, insertHandle);
        sink.appendPage(new Page(List.of("id", "val"), buildRows(50, 52)));
        sink.abort();
        sink.close();
        sink.close(); // second close must not throw
    }


    @org.junit.jupiter.api.AfterAll
    static void tearDown() throws Exception {
        QUERY.seal();
        metadata.cleanupQuery(QUERY.authority());
        QUERY.retire();
        catalog.close();
    }

    // -- helpers --

    private void createAndPopulate(String tableName, List<TypedRow> rows) {
        SchemaTableName stn = new SchemaTableName(DATABASE, tableName);
        ConnectorTableMetadata meta = new ConnectorTableMetadata(
                stn, List.of("id", "val"), COLUMNS, Map.of());
        ConnectorOutputTableHandle out = metadata.createTable(SESSION, meta);
        writeSink(out, rows);
    }

    private void writeSink(ConnectorOutputTableHandle handle, List<TypedRow> rows) {
        ConnectorPageSink sink = pageSinkProvider.createPageSink(TX, SESSION, handle);
        sink.appendPage(new Page(List.of("id", "val"), rows));
        byte[] commitMetadata = sink.finishWithMetadata();
        PaimonCommitTestSupport.finish(metadata, SESSION, handle, List.of(commitMetadata));
        sink.close();
    }

    private List<TypedRow> buildRows(int from, int to) {
        List<TypedRow> rows = new ArrayList<>();
        for (int i = from; i < to; i++) {
            rows.add(TypedRow.builder().putLong("id", i).putString("val", "v" + i).build());
        }
        return rows;
    }

    private ConnectorTableHandle loadHandle(String tableName) {
        return metadata.getTableHandle(SESSION, new SchemaTableName(DATABASE, tableName));
    }

    private List<TypedRow> readAll(String tableName) {
        PaimonTableHandle handle = (PaimonTableHandle) loadHandle(tableName);
        ConnectorSplitSource src = splitManager.getSplits(TX, SESSION, handle, NO_FILTER, NO_CONSTRAINT);
        List<ConnectorSplit> splits = src.getNextBatch(1000);
        src.close();
        List<ColumnHandle> cols = List.of(
                new PaimonColumnHandle(0, "id", PangrovaType.BIGINT),
                new PaimonColumnHandle(1, "val", PangrovaType.VARCHAR));
        List<TypedRow> all = new ArrayList<>();
        for (ConnectorSplit split : splits) {
            ConnectorPageSource ps = pageSourceProvider.createPageSource(TX, SESSION, split, handle, cols, NO_FILTER);
            while (!ps.isFinished()) {
                Page page = ps.getNextPage();
                if (page != null) all.addAll(page.rows());
            }
            ps.close();
        }
        return all;
    }
}
