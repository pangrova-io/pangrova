package org.pangrova.worker.property.jni.write;

import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.ConnectorPageSink;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.worker.jni.write.CatalogResourceResolver;
import org.pangrova.worker.jni.write.NativeWriteExecutor;
import org.pangrova.worker.jni.write.WriteDescriptor;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property test for NativeWriteExecutor session cleanup completeness.
 *
 * <p>For any number (1-100) of active write sessions, calling
 * {@code closeAllSessions()} must result in {@code activeSessionCount() == 0},
 * and every session's {@link ConnectorPageSink} must have both
 * {@code abort()} and {@code close()} invoked.</p>
 *
 * <p><b>Validates: Requirements 3.4</b></p>
 */
class NativeWriteExecutorSessionCleanupPropertyTest {

    /**
     * Property 5: For any session count in [1, 100], opening that many write
     * sessions and then calling closeAllSessions() leaves zero active sessions
     * with all sinks aborted and closed.
     *
     * <p><b>Validates: Requirements 3.4</b></p>
     */
    @Property(tries = 100)
    void closeAllSessionsDrainsAllActiveSessions(
            @ForAll @IntRange(min = 1, max = 100) int sessionCount) {

        var sinks = new ArrayList<TrackingPageSink>();
        NativeWriteExecutor executor = buildExecutor(sinks);

        for (int i = 0; i < sessionCount; i++) {
            executor.beginWrite(descriptor("table_" + i));
        }
        assertThat(executor.activeSessionCount()).isEqualTo(sessionCount);

        executor.closeAllSessions();

        assertThat(executor.activeSessionCount()).isZero();
        assertThat(sinks).hasSize(sessionCount);
        for (TrackingPageSink sink : sinks) {
            assertThat(sink.aborted.get())
                    .as("abort() must be called on every session sink")
                    .isTrue();
            assertThat(sink.closed.get())
                    .as("close() must be called on every session sink")
                    .isTrue();
        }
    }

    // --- Test infrastructure (hand-written test doubles, no mocks) ---

    private static NativeWriteExecutor buildExecutor(List<TrackingPageSink> sinkCollector) {
        return new NativeWriteExecutor(new org.pangrova.worker.jni.write.TestCatalogResourceResolver(
                (taskId, catalogName) -> {
            ConnectorMetadata metadata = new StubMetadata();
            ConnectorPageSinkProvider provider = (txn, session, outputHandle) -> {
                var sink = new TrackingPageSink();
                sinkCollector.add(sink);
                return sink;
            };
            ConnectorSession session = new StubSession();
            return new CatalogResourceResolver.CatalogResources(metadata, provider, session,
                    org.pangrova.worker.jni.write.TestOutputHandles.outputHandle(),
                    List.of("id"));
        }));
    }

    private static WriteDescriptor descriptor(String tableName) {
        return new WriteDescriptor(
                "test-catalog", "test-schema", tableName,
                WriteDescriptor.DmlOperation.INSERT,
                List.of(new WriteDescriptor.ColumnSchema("id", "BIGINT")),
                Map.of("taskId", "property-task"));
    }

    /** Tracks abort/close calls without performing real I/O. */
    private static final class TrackingPageSink implements ConnectorPageSink {
        final AtomicBoolean aborted = new AtomicBoolean();
        final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public void appendPage(Page page) {
            // No-op: property test focuses on session cleanup, not data writing
        }

        @Override
        public byte[] finishWithMetadata() {
            return new byte[0];
        }

        @Override
        public void abort() {
            aborted.set(true);
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }

    private static final class StubMetadata implements ConnectorMetadata {
        private static final ConnectorTableHandle TABLE_HANDLE = new ConnectorTableHandle() {};
        private static final ConnectorOutputTableHandle OUTPUT_HANDLE = new ConnectorOutputTableHandle() {};

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of();
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schema) {
            return List.of();
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName name) {
            return TABLE_HANDLE;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(
                ConnectorSession session, ConnectorTableHandle table) {
            return null;
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(
                ConnectorSession session, ConnectorTableHandle table) {
            return Map.of();
        }

        @Override
        public TableStatistics getTableStatistics(
                ConnectorSession session, ConnectorTableHandle table) {
            return TableStatistics.UNKNOWN;
        }

        @Override
        public ConnectorOutputTableHandle beginInsert(
                ConnectorSession session, ConnectorTableHandle table) {
            return OUTPUT_HANDLE;
        }
    }

    private static final class StubSession implements ConnectorSession {
        @Override public String getUser() { return "test-user"; }
        @Override public Optional<String> getCatalog() { return Optional.of("test-catalog"); }
        @Override public Optional<String> getSchema() { return Optional.of("test-schema"); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    }
}
