package org.pangrova.worker.jni.write;

import org.pangrova.spi.connector.ConnectorSession;
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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests verifying that {@link NativeWriteExecutor} correctly
 * delegates write operations through the CatalogResourceResolver and
 * ConnectorPageSink pipeline.
 */
class NativeWriteExecutorMetricsTest {

    @Test
    void commitWriteReturnsResult() {
        NativeWriteExecutor executor = createExecutor();

        long handle = executor.beginWrite(descriptor("t1"));
        WriteCommitResult result = executor.commitWrite(handle);

        assertThat(result.rowsWritten()).isZero();
    }

    @Test
    void abortWriteDoesNotThrow() {
        NativeWriteExecutor executor = createExecutor();

        long handle = executor.beginWrite(descriptor("t1"));
        executor.abortWrite(handle);
        // No exception = success
    }

    @Test
    void activeSessionCountTracksOpenSessions() {
        NativeWriteExecutor executor = createExecutor();

        assertThat(executor.activeSessionCount()).isZero();

        long h1 = executor.beginWrite(descriptor("t1"));
        assertThat(executor.activeSessionCount()).isEqualTo(1);

        long h2 = executor.beginWrite(descriptor("t2"));
        assertThat(executor.activeSessionCount()).isEqualTo(2);

        executor.commitWrite(h1);
        assertThat(executor.activeSessionCount()).isEqualTo(1);

        executor.abortWrite(h2);
        assertThat(executor.activeSessionCount()).isZero();
    }

    @Test
    void closeAllSessionsCleansUp() {
        NativeWriteExecutor executor = createExecutor();

        executor.beginWrite(descriptor("t1"));
        executor.beginWrite(descriptor("t2"));
        assertThat(executor.activeSessionCount()).isEqualTo(2);

        executor.closeAllSessions();
        assertThat(executor.activeSessionCount()).isZero();
    }

    @Test
    void commitFailureIncreasesNoActiveSessions() {
        NativeWriteExecutor executor = new NativeWriteExecutor(new TestCatalogResourceResolver(
                (taskId, catalogName) -> {
                    ConnectorMetadata metadata = new StubMetadata();
                    ConnectorPageSinkProvider sinkProvider = (txn, session, outputHandle) ->
                            new FailingPageSink();
                    return new CatalogResourceResolver.CatalogResources(
                            metadata, sinkProvider, new StubSession(),
                            TestOutputHandles.outputHandle(), List.of("id"));
                }));

        long handle = executor.beginWrite(descriptor("t1"));

        try {
            executor.commitWrite(handle);
        } catch (RuntimeException e) {
            // expected
        }

        // Session is removed after commit (even if it fails)
        assertThat(executor.activeSessionCount()).isZero();
    }

    // --- Test infrastructure ---

    private static NativeWriteExecutor createExecutor() {
        return new NativeWriteExecutor(new TestCatalogResourceResolver(
                (taskId, catalogName) -> {
                    ConnectorMetadata metadata = new StubMetadata();
                    ConnectorPageSinkProvider sinkProvider = (txn, session, outputHandle) ->
                            new TrackingPageSink();
                    return new CatalogResourceResolver.CatalogResources(
                            metadata, sinkProvider, new StubSession(),
                            TestOutputHandles.outputHandle(), List.of("id"));
                }));
    }

    private static WriteDescriptor descriptor(String tableName) {
        return new WriteDescriptor(
                "test-catalog", "test-schema", tableName,
                WriteDescriptor.DmlOperation.INSERT,
                List.of(new WriteDescriptor.ColumnSchema("id", "BIGINT")),
                Map.of("taskId", "metrics-task"));
    }

    private static final class TrackingPageSink implements ConnectorPageSink {
        @Override public void appendPage(Page page) {}
        @Override public byte[] finishWithMetadata() { return new byte[0]; }
        @Override public void abort() {}
        @Override public void close() {}
    }

    private static final class FailingPageSink implements ConnectorPageSink {
        @Override public void appendPage(Page page) {}
        @Override public byte[] finishWithMetadata() {
            throw new RuntimeException("simulated commit failure");
        }
        @Override public void abort() {}
        @Override public void close() {}
    }

    private static final class StubMetadata implements ConnectorMetadata {
        private static final ConnectorTableHandle TABLE_HANDLE = new ConnectorTableHandle() {};
        private static final ConnectorOutputTableHandle OUTPUT_HANDLE = new ConnectorOutputTableHandle() {};

        @Override
        public List<String> listSchemaNames(ConnectorSession session) { return List.of(); }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
            return List.of();
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
            return TABLE_HANDLE;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
            return null;
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
            return Map.of();
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
            return TableStatistics.UNKNOWN;
        }

        @Override
        public ConnectorOutputTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle table) {
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
