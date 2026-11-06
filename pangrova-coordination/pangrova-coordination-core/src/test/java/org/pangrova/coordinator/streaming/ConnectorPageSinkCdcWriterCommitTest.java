package org.pangrova.coordinator.streaming;

import org.pangrova.streaming.coordinator.ConnectorPageSinkCdcWriter;
import org.pangrova.streaming.coordinator.PageSinkStreamWriterContext;
import org.pangrova.coordinator.query.statement.StatementConnectorSession;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.spi.data.ConnectorPageSink;
import org.pangrova.connector.spi.data.ConnectorPageSinkProvider;
import org.pangrova.batch.api.page.Page;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcOperation;
import org.pangrova.streaming.api.cdc.CdcRow;
import org.pangrova.streaming.api.cdc.CdcWriteResult;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        PageSinkStreamWriterContext deps = new PageSinkStreamWriterContext(
                metadata, sinkProvider, session, "iceberg.default.cdc_target");
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
    void commitFailurePreservesFilesWhenPublicationOutcomeIsUnknown() {
        RecordingMetadata metadata = new RecordingMetadata();
        metadata.failCommit = true;
        RecordingPageSinkProvider sinkProvider = new RecordingPageSinkProvider();
        ConnectorSession session = new StatementConnectorSession("iceberg", "default");
        PageSinkStreamWriterContext deps = new PageSinkStreamWriterContext(
                metadata, sinkProvider, session, "iceberg.default.cdc_target");
        ConnectorPageSinkCdcWriter writer = new ConnectorPageSinkCdcWriter(deps);

        assertThatThrownBy(() -> writer.writeBatch(List.of(insert(1L, "alice"))))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("commit failed");
        // The connector may have published the snapshot before reporting failure. Deleting
        // the staged file here would make that snapshot unreadable.
        assertThat(metadata.abortCount.get())
                .as("an attempted catalog commit must never trigger file deletion")
                .isZero();
        assertThat(sinkProvider.lastSink.closed).isTrue();
    }

    @Test
    void discardBeforePublicationDeletesOnlyUnpublishedFiles() {
        RecordingMetadata metadata = new RecordingMetadata();
        RecordingPageSinkProvider sinkProvider = new RecordingPageSinkProvider();
        ConnectorSession session = new StatementConnectorSession("iceberg", "default");
        PageSinkStreamWriterContext deps = new PageSinkStreamWriterContext(
                metadata, sinkProvider, session, "iceberg.default.cdc_target");
        ConnectorPageSinkCdcWriter writer = new ConnectorPageSinkCdcWriter(deps);

        var staged = writer.stage(List.of(insert(1L, "alice")));
        writer.discard(staged);
        writer.discard(staged);

        assertThat(metadata.abortCount.get()).isEqualTo(1);
        assertThat(metadata.commitCount.get()).isZero();
    }

    @Test
    void stagedTokenCannotBePublishedOrDiscardedByAnotherWriter() {
        RecordingMetadata metadata = new RecordingMetadata();
        ConnectorSession session = new StatementConnectorSession("iceberg", "default");
        PageSinkStreamWriterContext deps = new PageSinkStreamWriterContext(
                metadata, new RecordingPageSinkProvider(), session, "iceberg.default.cdc_target");
        ConnectorPageSinkCdcWriter owner = new ConnectorPageSinkCdcWriter(deps);
        ConnectorPageSinkCdcWriter other = new ConnectorPageSinkCdcWriter(deps);
        var staged = owner.stage(List.of(insert(1L, "alice")));

        assertThatThrownBy(() -> other.publish(staged))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> other.discard(staged))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(metadata.commitCount.get()).isZero();
        assertThat(metadata.abortCount.get()).isZero();

        owner.discard(staged);
        assertThat(metadata.abortCount.get()).isEqualTo(1);
    }

    @Test
    void failedSecondStageDiscardsAlreadyPreparedDeleteFiles() {
        RecordingMetadata metadata = new RecordingMetadata();
        RecordingPageSinkProvider sinkProvider = new RecordingPageSinkProvider();
        sinkProvider.failInsertSinkCreation = true;
        ConnectorSession session = new StatementConnectorSession("iceberg", "default");
        PageSinkStreamWriterContext deps = new PageSinkStreamWriterContext(
                metadata, sinkProvider, session, "iceberg.default.cdc_target");
        ConnectorPageSinkCdcWriter writer = new ConnectorPageSinkCdcWriter(deps);
        CdcEnvelope update = new CdcEnvelope(CdcOperation.UPDATE, "cdc_target",
                CdcRow.of(Map.of("id", 1L, "name", "alice")),
                CdcRow.of(Map.of("id", 1L, "name", "bob")),
                Instant.now(), 1, Map.of());

        assertThatThrownBy(() -> writer.stage(List.of(update)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("insert sink unavailable");

        assertThat(metadata.abortCount.get()).isEqualTo(1);
        assertThat(metadata.commitCount.get()).isZero();
        assertThat(sinkProvider.lastSink.closed).isTrue();
    }

    @Test
    void offsetCommitAbortDoesNotDeletePublishedFiles() {
        RecordingMetadata metadata = new RecordingMetadata();
        RecordingPageSinkProvider sinkProvider = new RecordingPageSinkProvider();
        ConnectorSession session = new StatementConnectorSession("iceberg", "default");
        PageSinkStreamWriterContext deps = new PageSinkStreamWriterContext(
                metadata, sinkProvider, session, "iceberg.default.cdc_target");
        ConnectorPageSinkCdcWriter writer = new ConnectorPageSinkCdcWriter(deps);

        var staged = writer.stage(List.of(insert(1L, "alice")));
        assertThat(writer.publish(staged).rowsWritten()).isEqualTo(1);
        writer.discard(staged);
        assertThat(writer.publish(staged).rowsWritten()).isEqualTo(1);

        assertThat(metadata.abortCount.get()).isZero();
        assertThat(metadata.commitCount.get()).isEqualTo(1);
    }

    @Test
    void listenerFailureAfterCatalogCommitDoesNotDeletePublishedFiles() {
        RecordingMetadata metadata = new RecordingMetadata();
        metadata.failAfterCommit = true;
        RecordingPageSinkProvider sinkProvider = new RecordingPageSinkProvider();
        ConnectorSession session = new StatementConnectorSession("iceberg", "default");
        PageSinkStreamWriterContext deps = new PageSinkStreamWriterContext(
                metadata, sinkProvider, session, "iceberg.default.cdc_target");
        ConnectorPageSinkCdcWriter writer = new ConnectorPageSinkCdcWriter(deps);

        assertThatThrownBy(() -> writer.writeBatch(List.of(insert(1L, "alice"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("notification failed");

        assertThat(metadata.commitCount.get()).isEqualTo(1);
        assertThat(metadata.abortCount.get()).isZero();
    }

    @Test
    void discardCannotRaceWithCatalogPublication() throws Exception {
        RecordingMetadata metadata = new RecordingMetadata();
        metadata.commitEntered = new CountDownLatch(1);
        metadata.continueCommit = new CountDownLatch(1);
        ConnectorSession session = new StatementConnectorSession("iceberg", "default");
        PageSinkStreamWriterContext deps = new PageSinkStreamWriterContext(
                metadata, new RecordingPageSinkProvider(), session, "iceberg.default.cdc_target");
        ConnectorPageSinkCdcWriter writer = new ConnectorPageSinkCdcWriter(deps);
        var staged = writer.stage(List.of(insert(1L, "alice")));
        var executor = Executors.newSingleThreadExecutor();
        try {
            var publication = executor.submit(() -> writer.publish(staged));
            assertThat(metadata.commitEntered.await(5, TimeUnit.SECONDS)).isTrue();
            writer.discard(staged);
            metadata.continueCommit.countDown();
            assertThat(publication.get(5, TimeUnit.SECONDS).rowsWritten()).isEqualTo(1);
            assertThat(metadata.abortCount.get()).isZero();
        } finally {
            metadata.continueCommit.countDown();
            executor.shutdownNow();
        }
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
        PageSinkStreamWriterContext deps = new PageSinkStreamWriterContext(
                metadata, sinkProvider, session, qualifiedTarget);
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
        boolean failAfterCommit = false;
        CountDownLatch commitEntered;
        CountDownLatch continueCommit;
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
        public ConnectorOutputTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle table) {
            return new TestOutputHandle("delete");
        }

        @Override
        public void finishDistributedWrite(ConnectorSession session,
                                           ConnectorOutputTableHandle outputHandle,
                                           List<byte[]> fragments) {
            if (failCommit) {
                throw new RuntimeException("commit failed");
            }
            if (commitEntered != null) {
                commitEntered.countDown();
                try {
                    if (!continueCommit.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("timed out waiting to finish commit");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("commit interrupted", interrupted);
                }
            }
            committedFragments.add(List.copyOf(fragments));
            committedHandles.add(outputHandle);
            commitCount.incrementAndGet();
            if (failAfterCommit) {
                throw new IllegalStateException("notification failed after catalog publication");
            }
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
        boolean failInsertSinkCreation;

        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle txn,
                                                ConnectorSession session,
                                                ConnectorOutputTableHandle outputHandle) {
            if (failInsertSinkCreation) {
                throw new IllegalStateException("insert sink unavailable");
            }
            lastSink = new RecordingPageSink("commit-" + sinkSeq.getAndIncrement());
            return lastSink;
        }

        @Override
        public ConnectorPageSink createDeletePageSink(ConnectorTransactionHandle txn,
                                                      ConnectorSession session,
                                                      ConnectorOutputTableHandle outputHandle) {
            lastSink = new RecordingPageSink("delete-" + sinkSeq.getAndIncrement());
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
