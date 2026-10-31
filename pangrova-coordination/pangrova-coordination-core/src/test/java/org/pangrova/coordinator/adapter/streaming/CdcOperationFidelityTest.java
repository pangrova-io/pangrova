package org.pangrova.coordinator.adapter.streaming;

import org.pangrova.streaming.adapter.coordinator.ConnectorPageSinkCdcWriter;
import org.pangrova.streaming.adapter.coordinator.PageSinkStreamWriterContext;
import org.pangrova.coordinator.query.statement.StatementConnectorSession;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.ConnectorTransactionHandle;
import org.pangrova.connector.spi.data.ConnectorPageSink;
import org.pangrova.connector.spi.data.ConnectorPageSinkProvider;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.connector.spi.data.page.Page;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.spi.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.spi.metadata.table.ColumnHandle;
import org.pangrova.connector.spi.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.spi.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;
import org.pangrova.connector.spi.metadata.table.TableStatistics;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcOperation;
import org.pangrova.streaming.api.cdc.CdcRow;
import org.pangrova.streaming.api.cdc.CdcWriteResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for task 169 (F-CHAIN-STREAM-02): CDC operation type (I/U/D) fidelity.
 *
 * <p>Previously {@code processLocally} was append-only, causing DELETE rows to be
 * silently dropped and UPDATE rows to land as duplicate inserts. The fix routes
 * each operation through the appropriate connector path:
 * <ul>
 *   <li>INSERT: {@code beginInsert} + {@code createPageSink} (append)</li>
 *   <li>DELETE: {@code beginDelete} + {@code createDeletePageSink} (equality delete via before-image)</li>
 *   <li>UPDATE: delete of before-image + insert of after-image</li>
 * </ul>
 */
class CdcOperationFidelityTest {

    @Test
    @DisplayName("INSERT-only batches succeed on the local sink path")
    void insertOnlyBatchSucceeds() {
        ConnectorPageSinkCdcWriter writer = buildWriter();
        CdcWriteResult result = writer.writeBatch(List.of(
                envelope(CdcOperation.INSERT, "t", null, row(1L, "a")),
                envelope(CdcOperation.INSERT, "t", null, row(2L, "b"))));
        assertThat(result.rowsWritten()).isEqualTo(2);
    }

    @Test
    @DisplayName("DELETE envelope routes through beginDelete + createDeletePageSink")
    void deleteEnvelopeRoutedThroughDeletePath() {
        RecordingMetadata metadata = new RecordingMetadata();
        RecordingSinkProvider sinkProvider = new RecordingSinkProvider();
        ConnectorPageSinkCdcWriter writer = buildWriter(metadata, sinkProvider);

        CdcWriteResult result = writer.writeBatch(List.of(
                envelope(CdcOperation.DELETE, "t", row(1L, "alice"), null)));

        assertThat(result.rowsWritten()).isEqualTo(1);
        // Delete phase committed via beginDelete
        assertThat(metadata.beginDeleteCount.get())
                .as("DELETE must route through beginDelete")
                .isEqualTo(1);
        // Delete page sink used (not the regular insert sink)
        assertThat(sinkProvider.deletePageSinkCreated.get())
                .as("DELETE must use createDeletePageSink")
                .isEqualTo(1);
        // The before-image must be written as the delete marker
        assertThat(sinkProvider.lastDeleteSink.appendedPages)
                .hasSize(1);
        Page deletePage = sinkProvider.lastDeleteSink.appendedPages.get(0);
        assertThat(deletePage.rows()).hasSize(1);
        assertThat(deletePage.rows().get(0).getRaw("name")).isEqualTo("alice");
    }

    @Test
    @DisplayName("UPDATE envelope produces delete of before-image + insert of after-image")
    void updateEnvelopeProducesDeleteAndInsert() {
        RecordingMetadata metadata = new RecordingMetadata();
        RecordingSinkProvider sinkProvider = new RecordingSinkProvider();
        ConnectorPageSinkCdcWriter writer = buildWriter(metadata, sinkProvider);

        CdcWriteResult result = writer.writeBatch(List.of(
                envelope(CdcOperation.UPDATE, "t", row(1L, "old"), row(1L, "new"))));

        assertThat(result.rowsWritten()).isEqualTo(1);
        // Both insert and delete paths committed
        assertThat(metadata.beginDeleteCount.get())
                .as("UPDATE must produce a delete of the old row")
                .isEqualTo(1);
        assertThat(metadata.beginInsertCount.get())
                .as("UPDATE must produce an insert of the new row")
                .isEqualTo(1);
        // Verify the before-image is in the delete page
        Page deletePage = sinkProvider.lastDeleteSink.appendedPages.get(0);
        assertThat(deletePage.rows().get(0).getRaw("name")).isEqualTo("old");
        // Verify the after-image is in the insert page
        Page insertPage = sinkProvider.lastInsertSink.appendedPages.get(0);
        assertThat(insertPage.rows().get(0).getRaw("name")).isEqualTo("new");
    }

    @Test
    @DisplayName("mixed batch with INSERT + DELETE + UPDATE commits all operations")
    void mixedBatchCommitsAllOperations() {
        RecordingMetadata metadata = new RecordingMetadata();
        RecordingSinkProvider sinkProvider = new RecordingSinkProvider();
        ConnectorPageSinkCdcWriter writer = buildWriter(metadata, sinkProvider);

        CdcWriteResult result = writer.writeBatch(List.of(
                envelope(CdcOperation.INSERT, "t", null, row(1L, "alice")),
                envelope(CdcOperation.UPDATE, "t", row(2L, "bob_old"), row(2L, "bob_new")),
                envelope(CdcOperation.DELETE, "t", row(3L, "carol"), null)));

        assertThat(result.rowsWritten()).isEqualTo(3);
        // Delete phase: UPDATE before-image + DELETE before-image = 2 delete rows
        assertThat(metadata.beginDeleteCount.get()).isEqualTo(1);
        Page deletePage = sinkProvider.lastDeleteSink.appendedPages.get(0);
        assertThat(deletePage.rows()).hasSize(2);
        // Insert phase: INSERT after-image + UPDATE after-image = 2 insert rows
        assertThat(metadata.beginInsertCount.get()).isEqualTo(1);
        Page insertPage = sinkProvider.lastInsertSink.appendedPages.get(0);
        assertThat(insertPage.rows()).hasSize(2);
        // Total commits = 2 (one delete phase, one insert phase)
        assertThat(metadata.commitCount.get()).isEqualTo(2);
    }

    @Test
    @DisplayName("DELETE with missing before-image fails fast with clear message")
    void deleteMissingBeforeImageFailsFast() {
        ConnectorPageSinkCdcWriter writer = buildWriter();
        assertThatThrownBy(() -> writer.writeBatch(List.of(
                envelope(CdcOperation.DELETE, "t", null, null))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("DELETE")
                .hasMessageContaining("no before-image");
    }

    @Test
    @DisplayName("UPDATE with missing before-image fails fast with clear message")
    void updateMissingBeforeImageFailsFast() {
        ConnectorPageSinkCdcWriter writer = buildWriter();
        assertThatThrownBy(() -> writer.writeBatch(List.of(
                envelope(CdcOperation.UPDATE, "t", null, row(1L, "new")))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("UPDATE")
                .hasMessageContaining("no before-image");
    }

    @Test
    @DisplayName("UPDATE with missing after-image fails fast with clear message")
    void updateMissingAfterImageFailsFast() {
        ConnectorPageSinkCdcWriter writer = buildWriter();
        assertThatThrownBy(() -> writer.writeBatch(List.of(
                envelope(CdcOperation.UPDATE, "t", row(1L, "old"), null))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("UPDATE")
                .hasMessageContaining("no after-image");
    }

    @Test
    @DisplayName("INSERT with missing after-image fails fast with clear message")
    void insertMissingAfterImageFailsFast() {
        ConnectorPageSinkCdcWriter writer = buildWriter();
        assertThatThrownBy(() -> writer.writeBatch(List.of(
                envelope(CdcOperation.INSERT, "t", null, null))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("INSERT")
                .hasMessageContaining("no after-image");
    }

    // ── helpers ──

    private static ConnectorPageSinkCdcWriter buildWriter() {
        return buildWriter(new RecordingMetadata(), new RecordingSinkProvider());
    }

    private static ConnectorPageSinkCdcWriter buildWriter(RecordingMetadata metadata,
                                                           RecordingSinkProvider sinkProvider) {
        ConnectorSession session = new StatementConnectorSession("iceberg", "default");
        PageSinkStreamWriterContext deps = new PageSinkStreamWriterContext(
                metadata, sinkProvider, session, "iceberg.default.target");
        return new ConnectorPageSinkCdcWriter(deps);
    }

    private static CdcEnvelope envelope(CdcOperation op, String table,
                                         CdcRow before, CdcRow after) {
        return new CdcEnvelope(op, table, before, after, Instant.now(), 1, Map.of());
    }

    private static CdcRow row(long id, String name) {
        return CdcRow.of(Map.of("id", id, "name", name));
    }

    // ── test doubles ──

    private record TH(String key) implements ConnectorTableHandle {}
    private record OH(String key) implements ConnectorOutputTableHandle {}
    private record CH(String name) implements ColumnHandle {
        @Override public String columnName() { return name; }
    }

    private static final class RecordingMetadata implements ConnectorMetadata {
        final AtomicInteger commitCount = new AtomicInteger();
        final AtomicInteger beginInsertCount = new AtomicInteger();
        final AtomicInteger beginDeleteCount = new AtomicInteger();

        @Override public List<String> listSchemaNames(ConnectorSession s) { return List.of("default"); }
        @Override public List<SchemaTableName> listTables(ConnectorSession s, Optional<String> schema) {
            return List.of(new SchemaTableName("default", "target"));
        }
        @Override public ConnectorTableHandle getTableHandle(ConnectorSession s, SchemaTableName t) {
            return new TH(t.schema() + "." + t.table());
        }
        @Override public ConnectorTableMetadata getTableMetadata(ConnectorSession s, ConnectorTableHandle t) {
            return new ConnectorTableMetadata(new SchemaTableName("default", "target"), List.of("id", "name"), Map.of());
        }
        @Override public Map<String, ColumnHandle> getColumnHandles(ConnectorSession s, ConnectorTableHandle t) {
            return Map.of("id", new CH("id"), "name", new CH("name"));
        }
        @Override public TableStatistics getTableStatistics(ConnectorSession s, ConnectorTableHandle t) {
            return TableStatistics.UNKNOWN;
        }
        @Override public ConnectorOutputTableHandle beginInsert(ConnectorSession s, ConnectorTableHandle t) {
            beginInsertCount.incrementAndGet();
            return new OH("insert-" + beginInsertCount.get());
        }
        @Override public ConnectorOutputTableHandle beginDelete(ConnectorSession s, ConnectorTableHandle t) {
            beginDeleteCount.incrementAndGet();
            return new OH("delete-" + beginDeleteCount.get());
        }
        @Override public void finishDistributedWrite(ConnectorSession s, ConnectorOutputTableHandle o, List<byte[]> f) {
            commitCount.incrementAndGet();
        }
    }

    private static final class RecordingSinkProvider implements ConnectorPageSinkProvider {
        final AtomicInteger insertPageSinkCreated = new AtomicInteger();
        final AtomicInteger deletePageSinkCreated = new AtomicInteger();
        RecordingPageSink lastInsertSink;
        RecordingPageSink lastDeleteSink;

        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle txn,
                                                 ConnectorSession session,
                                                 ConnectorOutputTableHandle outputHandle) {
            insertPageSinkCreated.incrementAndGet();
            lastInsertSink = new RecordingPageSink("insert-commit");
            return lastInsertSink;
        }

        @Override
        public ConnectorPageSink createDeletePageSink(ConnectorTransactionHandle txn,
                                                       ConnectorSession session,
                                                       ConnectorOutputTableHandle outputHandle) {
            deletePageSinkCreated.incrementAndGet();
            lastDeleteSink = new RecordingPageSink("delete-commit");
            return lastDeleteSink;
        }
    }

    private static final class RecordingPageSink implements ConnectorPageSink {
        private final String commitToken;
        final List<Page> appendedPages = new ArrayList<>();
        boolean aborted = false;
        boolean closed = false;

        RecordingPageSink(String commitToken) {
            this.commitToken = commitToken;
        }

        @Override public void appendPage(Page page) { appendedPages.add(page); }
        @Override public byte[] finishWithMetadata() { return commitToken.getBytes(StandardCharsets.UTF_8); }
        @Override public void abort() { aborted = true; }
        @Override public void close() { closed = true; }
    }
}
