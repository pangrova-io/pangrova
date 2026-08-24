package org.pangrova.coordinator.streaming;

import org.pangrova.coordinator.query.statement.StatementConnectorSession;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.ConnectorPageSink;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.streaming.CdcEnvelope;
import org.pangrova.spi.streaming.CdcOperation;
import org.pangrova.spi.streaming.CdcRow;
import org.pangrova.spi.streaming.CdcWriteResult;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for the CDC silent-data-loss bug: {@link ConnectorPageSinkCdcWriter}
 * wrote data files via {@code finishWithMetadata()} but never performed the catalog
 * commit via {@code finishDistributedWrite()}. Data files landed on storage with no
 * snapshot referencing them while the Kafka offset advanced past the batch.
 *
 * <p>This test asserts every committed batch invokes {@code finishDistributedWrite}
 * with the commit metadata produced by the sink, and that a fresh insert handle is
 * obtained per batch rather than reusing a single handle from sink construction.</p>
 */
class ConnectorPageSinkCdcWriterCommitTest {

    @Test
    void eachBatchCommitsViaFinishDistributedWrite() {
        RecordingMetadata metadata = new RecordingMetadata();
        RecordingPageSinkProvider sinkProvider = new RecordingPageSinkProvider();
        ConnectorSession session = new StatementConnectorSession("iceberg", "default");
        ConnectorOutputTableHandle outputHandle = new TestOutputHandle("default.cdc_target");

        PageSinkStreamWriterContext deps = new PageSinkStreamWriterContext(
                metadata, sinkProvider, session, outputHandle, "iceberg.default.cdc_target");
        ConnectorPageSinkCdcWriter writer = new ConnectorPageSinkCdcWriter(deps);

        CdcWriteResult r1 = writer.writeBatch(List.of(insert(1L, "alice")));
        CdcWriteResult r2 = writer.writeBatch(List.of(insert(2L, "bob")));

        assertThat(r1.rowsWritten()).isEqualTo(1);
        assertThat(r2.rowsWritten()).isEqualTo(1);

        // Both batches committed to the catalog.
        assertThat(metadata.commitCount.get())
                .as("each batch must commit via finishDistributedWrite")
                .isEqualTo(2);
        // Commit metadata threaded through from the sink (not empty/dropped).
        assertThat(metadata.committedFragments)
                .allSatisfy(frags -> assertThat(frags).hasSize(1));
        assertThat(new String(metadata.committedFragments.get(0).get(0), StandardCharsets.UTF_8))
                .isEqualTo("commit-0");
        assertThat(new String(metadata.committedFragments.get(1).get(0), StandardCharsets.UTF_8))
                .isEqualTo("commit-1");

        // Fresh insert handle per committed batch (not the single sink-construction handle).
        assertThat(metadata.beginInsertCount.get()).isEqualTo(2);
        assertThat(metadata.committedHandles.get(0))
                .isNotSameAs(metadata.committedHandles.get(1));
    }

    @Test
    void commitFailureDiscardsStagedFiles() {
        RecordingMetadata metadata = new RecordingMetadata();
        metadata.failCommit = true;
        RecordingPageSinkProvider sinkProvider = new RecordingPageSinkProvider();
        ConnectorSession session = new StatementConnectorSession("iceberg", "default");
        ConnectorOutputTableHandle outputHandle = new TestOutputHandle("default.cdc_target");

        PageSinkStreamWriterContext deps = new PageSinkStreamWriterContext(
                metadata, sinkProvider, session, outputHandle, "iceberg.default.cdc_target");
        ConnectorPageSinkCdcWriter writer = new ConnectorPageSinkCdcWriter(deps);

        try {
            writer.writeBatch(List.of(insert(1L, "alice")));
        } catch (RuntimeException expected) {
            // expected
        }
        // A-EOS-02 staged 2PC: the sink is finished+closed at stage time, so a commit (publish)
        // failure discards the staged files via abortDistributedWrite rather than aborting the
        // already-closed sink. The invariant preserved is "a failed commit leaks no files".
        assertThat(metadata.abortCount.get())
                .as("commit failure must discard staged files via abortDistributedWrite")
                .isEqualTo(1);
        assertThat(sinkProvider.lastSink.closed).isTrue();
    }

    /**
     * F-STREAM-001: the CDC target schema must be resolved from the qualified target
     * ({@code catalog.schema.table}, {@code schema.table}, or a bare {@code table}) rather than
     * hardcoded to {@code default}. Previously any non-default schema silently wrote to
     * {@code default.<table>}. This drives a real INSERT batch through the writer and asserts the
     * {@link SchemaTableName} handed to the catalog.
     */
    @Test
    void resolvesSchemaFromQualifiedTargetInsteadOfHardcodingDefault() {
        SchemaTableName threePart = resolveTargetTable("iceberg.sales.orders");
        assertThat(threePart.schema()).as("catalog.schema.table -> schema").isEqualTo("sales");
        assertThat(threePart.table()).isEqualTo("orders");

        SchemaTableName twoPart = resolveTargetTable("warehouse.lineitem");
        assertThat(twoPart.schema()).as("schema.table -> schema").isEqualTo("warehouse");
        assertThat(twoPart.table()).isEqualTo("lineitem");

        SchemaTableName bare = resolveTargetTable("events");
        assertThat(bare.schema()).as("bare table -> default schema").isEqualTo("default");
        assertThat(bare.table()).isEqualTo("events");
    }

    /** Runs one INSERT batch against {@code qualifiedTarget}; returns the SchemaTableName the
     *  writer asked the catalog to resolve. */
    private static SchemaTableName resolveTargetTable(String qualifiedTarget) {
        RecordingMetadata metadata = new RecordingMetadata();
        RecordingPageSinkProvider sinkProvider = new RecordingPageSinkProvider();
        ConnectorSession session = new StatementConnectorSession("iceberg", "default");
        ConnectorOutputTableHandle outputHandle = new TestOutputHandle("out");
        PageSinkStreamWriterContext deps = new PageSinkStreamWriterContext(
                metadata, sinkProvider, session, outputHandle, qualifiedTarget);
        new ConnectorPageSinkCdcWriter(deps).writeBatch(List.of(insert(1L, "alice")));
        return metadata.requestedTables.get(metadata.requestedTables.size() - 1);
    }

    private static CdcEnvelope insert(long id, String name) {
        return new CdcEnvelope(CdcOperation.INSERT, "cdc_target",
                null, CdcRow.of(Map.of("id", id, "name", name)),
                Instant.now(), 1, Map.of());
    }

    // ── Test doubles ─────────────────────────────────────────────────

    private static final class RecordingMetadata implements ConnectorMetadata {
        final AtomicInteger beginInsertCount = new AtomicInteger();
        final AtomicInteger commitCount = new AtomicInteger();
        final List<List<byte[]>> committedFragments = new ArrayList<>();
        final List<ConnectorOutputTableHandle> committedHandles = new ArrayList<>();
        final List<SchemaTableName> requestedTables = new ArrayList<>();
        boolean failCommit = false;
        final AtomicInteger abortCount = new AtomicInteger();

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of("default");
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
            return List.of(new SchemaTableName("default", "cdc_target"));
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
            requestedTables.add(tableName);
            return new TestTableHandle(tableName.schema() + "." + tableName.table());
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
            return new ConnectorTableMetadata(
                    new SchemaTableName("default", "cdc_target"), List.of("id", "name"), Map.of());
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
            return Map.of("id", new TestColumnHandle("id"), "name", new TestColumnHandle("name"));
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
            return TableStatistics.UNKNOWN;
        }

        @Override
        public ConnectorOutputTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle table) {
            beginInsertCount.incrementAndGet();
            return new TestOutputHandle("insert-" + beginInsertCount.get());
        }

        @Override
        public void finishDistributedWrite(ConnectorSession session,
                                           ConnectorOutputTableHandle outputHandle,
                                           List<byte[]> fragments) {
            if (failCommit) {
                throw new RuntimeException("commit failed");
            }
            committedFragments.add(List.copyOf(fragments));
            committedHandles.add(outputHandle);
            commitCount.incrementAndGet();
        }

        @Override
        public void abortDistributedWrite(ConnectorSession session,
                                          ConnectorOutputTableHandle outputHandle,
                                          List<byte[]> fragments) {
            abortCount.incrementAndGet();
        }
    }

    private record TestTableHandle(String key) implements ConnectorTableHandle {}

    private record TestOutputHandle(String key) implements ConnectorOutputTableHandle {}

    private record TestColumnHandle(String name) implements ColumnHandle {
        @Override public String columnName() { return name; }
    }

    private static final class RecordingPageSinkProvider implements ConnectorPageSinkProvider {
        RecordingPageSink lastSink;
        final AtomicInteger sinkSeq = new AtomicInteger();

        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle txn,
                                                ConnectorSession session,
                                                ConnectorOutputTableHandle outputHandle) {
            lastSink = new RecordingPageSink("commit-" + sinkSeq.getAndIncrement());
            return lastSink;
        }
    }

    private static final class RecordingPageSink implements ConnectorPageSink {
        private final String commitToken;
        boolean aborted = false;
        boolean closed = false;

        RecordingPageSink(String commitToken) {
            this.commitToken = commitToken;
        }

        @Override public void appendPage(Page page) {}

        @Override public byte[] finishWithMetadata() {
            return commitToken.getBytes(StandardCharsets.UTF_8);
        }

        @Override public void abort() { aborted = true; }

        @Override public void close() { closed = true; }
    }
}
