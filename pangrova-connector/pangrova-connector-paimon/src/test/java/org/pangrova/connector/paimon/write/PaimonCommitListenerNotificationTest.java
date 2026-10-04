package org.pangrova.connector.paimon.write;

import org.pangrova.connector.paimon.PaimonMetadata;
import org.pangrova.testfixtures.tableformat.PaimonCommitTestSupport;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.ConnectorPageSink;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.index.CommitEvent;
import org.pangrova.spi.connector.index.CommitEventType;
import org.pangrova.spi.connector.index.CommitListener;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
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
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for architecture-remediation #61: {@code finishDistributedWrite}
 * must report the real added/removed data files to the registered
 * {@link CommitListener}, otherwise Paimon index maintenance is silently broken.
 */
class PaimonCommitListenerNotificationTest {

    @TempDir
    static Path warehouse;

    private static Catalog catalog;
    private static PaimonMetadata metadata;
    private static PaimonPageSinkProvider pageSinkProvider;

    private static final String CATALOG_NAME = "paimon_listener_test";
    private static final String DATABASE = "listener_db";

    private static final ConnectorTransactionHandle TX = ConnectorTransactionHandle.EMPTY;

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of(CATALOG_NAME); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of("tenant_id", "test-tenant"); }
    };

    private static final List<ConnectorTableMetadata.Column> APPEND_COLUMNS = List.of(
            new ConnectorTableMetadata.Column("seq", PangrovaType.BIGINT, OptionalInt.of(0)),
            new ConnectorTableMetadata.Column("message", PangrovaType.VARCHAR, OptionalInt.of(1)));

    private static final List<ConnectorTableMetadata.Column> PK_COLUMNS = List.of(
            new ConnectorTableMetadata.Column("id", PangrovaType.BIGINT, OptionalInt.of(0)),
            new ConnectorTableMetadata.Column("name", PangrovaType.VARCHAR, OptionalInt.of(1)));

    /** Collects every CommitEvent the connector emits. */
    private static final class RecordingCommitListener implements CommitListener {
        final List<CommitEvent> events = new CopyOnWriteArrayList<>();
        @Override public void onCommit(CommitEvent event) { events.add(event); }
    }

    @BeforeAll
    static void setUp() throws Exception {
        Options options = new Options();
        options.setString("warehouse", warehouse.toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase(DATABASE, false);
        metadata = new PaimonMetadata(catalog, CATALOG_NAME, new org.pangrova.connector.paimon.PaimonSplitRegistry());
        pageSinkProvider = new PaimonPageSinkProvider(catalog);
    }

    @Test
    void appendWrite_reportsNonEmptyAddedFilesToListener() {
        RecordingCommitListener listener = new RecordingCommitListener();
        metadata.registerCommitListener(listener);

        ConnectorOutputTableHandle outputHandle = createAppendOnlyTable("append_listener");
        writeRows(outputHandle, buildAppendRows(0, 4));

        assertThat(listener.events).hasSize(1);
        CommitEvent event = listener.events.get(0);
        assertThat(event.eventType()).isEqualTo(CommitEventType.APPEND);
        assertThat(event.schemaName()).isEqualTo(DATABASE);
        assertThat(event.tableName()).isEqualTo("append_listener");
        // The core assertion: the listener must receive the real added file paths,
        // not the always-empty list the buggy implementation passed.
        assertThat(event.addedFiles()).isNotEmpty();
        assertThat(event.addedFiles()).allSatisfy(f -> assertThat(f).isNotBlank());
        assertThat(event.snapshotId()).isNotBlank();
    }

    @Test
    void primaryKeyUpsert_reportsAddedFilesAcrossCommits() {
        RecordingCommitListener listener = new RecordingCommitListener();
        metadata.registerCommitListener(listener);

        ConnectorOutputTableHandle outputHandle = createPrimaryKeyTable("pk_listener");
        writeRows(outputHandle, buildPkRows(0, 3));

        ConnectorOutputTableHandle insertHandle = metadata.beginInsert(SESSION, loadTableHandle("pk_listener"));
        writeRows(insertHandle, buildPkRows(0, 2));

        assertThat(listener.events).hasSize(2);
        assertThat(listener.events).allSatisfy(e ->
                assertThat(e.addedFiles()).isNotEmpty());
    }

    // -- helpers --

    private ConnectorOutputTableHandle createAppendOnlyTable(String tableName) {
        SchemaTableName schemaTable = new SchemaTableName(DATABASE, tableName);
        ConnectorTableMetadata tableMeta = new ConnectorTableMetadata(
                schemaTable, List.of("seq", "message"), APPEND_COLUMNS, Map.of());
        return metadata.createTable(SESSION, tableMeta);
    }

    private ConnectorOutputTableHandle createPrimaryKeyTable(String tableName) {
        SchemaTableName schemaTable = new SchemaTableName(DATABASE, tableName);
        ConnectorTableMetadata tableMeta = new ConnectorTableMetadata(
                schemaTable, List.of("id", "name"), PK_COLUMNS,
                Map.of("primary-key", "id", "bucket", "1"));
        return metadata.createTable(SESSION, tableMeta);
    }

    private ConnectorTableHandle loadTableHandle(String tableName) {
        return metadata.getTableHandle(SESSION, new SchemaTableName(DATABASE, tableName));
    }

    private void writeRows(ConnectorOutputTableHandle outputHandle, List<TypedRow> rows) {
        ConnectorPageSink sink = pageSinkProvider.createPageSink(TX, SESSION, outputHandle);
        List<String> columns = rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
        sink.appendPage(new Page(columns, rows));
        byte[] commitMetadata = sink.finishWithMetadata();
        PaimonCommitTestSupport.finish(metadata, SESSION, outputHandle, List.of(commitMetadata));
        sink.close();
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

    private List<TypedRow> buildPkRows(int startInclusive, int endExclusive) {
        List<TypedRow> rows = new ArrayList<>();
        for (int i = startInclusive; i < endExclusive; i++) {
            rows.add(TypedRow.builder()
                    .putLong("id", i)
                    .putString("name", "row_" + i)
                    .build());
        }
        return rows;
    }
}
