package org.pangrova.worker.jni.write;

import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSink;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link NativeWriteExecutor} session lifecycle management,
 * focusing on {@link NativeWriteExecutor#closeAllSessions()} and
 * {@link AutoCloseable} behavior.
 */
class NativeWriteExecutorTest {

    @Test
    void closeAllSessionsAbortsAndClosesEachActiveSink() {
        var sinks = new ArrayList<TrackingPageSink>();
        NativeWriteExecutor executor = createExecutor(sinks);

        executor.beginWrite(descriptor("t1"));
        executor.beginWrite(descriptor("t2"));
        assertThat(executor.activeSessionCount()).isEqualTo(2);

        executor.closeAllSessions();

        assertThat(executor.activeSessionCount()).isZero();
        assertThat(sinks).hasSize(2);
        for (TrackingPageSink sink : sinks) {
            assertThat(sink.aborted.get()).isTrue();
            assertThat(sink.closed.get()).isTrue();
        }
    }

    @Test
    void closeAllSessionsOnEmptyMapIsNoOp() {
        NativeWriteExecutor executor = createExecutor(new ArrayList<>());
        assertThat(executor.activeSessionCount()).isZero();

        executor.closeAllSessions();

        assertThat(executor.activeSessionCount()).isZero();
    }

    @Test
    void closeAllSessionsContinuesWhenAbortThrows() {
        var sinks = new ArrayList<TrackingPageSink>();
        NativeWriteExecutor executor = createExecutor(sinks, true);

        executor.beginWrite(descriptor("t1"));
        executor.beginWrite(descriptor("t2"));

        executor.closeAllSessions();

        assertThat(executor.activeSessionCount()).isZero();
        assertThat(sinks).hasSize(2);
        for (TrackingPageSink sink : sinks) {
            assertThat(sink.aborted.get()).isTrue();
            assertThat(sink.closed.get()).isTrue();
        }
    }

    @Test
    void closeViaAutoCloseableDelegatesToCloseAllSessions() {
        var sinks = new ArrayList<TrackingPageSink>();
        NativeWriteExecutor executor = createExecutor(sinks);

        executor.beginWrite(descriptor("t1"));
        assertThat(executor.activeSessionCount()).isEqualTo(1);

        executor.close();

        assertThat(executor.activeSessionCount()).isZero();
        assertThat(sinks).hasSize(1);
        assertThat(sinks.get(0).aborted.get()).isTrue();
        assertThat(sinks.get(0).closed.get()).isTrue();
    }

    @Test
    void closeIsIdempotent() {
        var sinks = new ArrayList<TrackingPageSink>();
        NativeWriteExecutor executor = createExecutor(sinks);

        executor.beginWrite(descriptor("t1"));
        executor.close();
        executor.close();

        assertThat(executor.activeSessionCount()).isZero();
    }

    @Test
    void committedSessionsAreNotAffectedByCloseAllSessions() {
        var sinks = new ArrayList<TrackingPageSink>();
        NativeWriteExecutor executor = createExecutor(sinks);

        long h1 = executor.beginWrite(descriptor("t1"));
        executor.beginWrite(descriptor("t2"));
        executor.commitWrite(h1);

        assertThat(executor.activeSessionCount()).isEqualTo(1);

        executor.closeAllSessions();

        assertThat(executor.activeSessionCount()).isZero();
        // h1 was committed (removed from map), so closeAllSessions only touches h2
        TrackingPageSink sink2 = sinks.get(1);
        assertThat(sink2.aborted.get()).isTrue();
        assertThat(sink2.closed.get()).isTrue();
    }

    // --- Test infrastructure ---

    private static NativeWriteExecutor createExecutor(List<TrackingPageSink> sinkCollector) {
        return createExecutor(sinkCollector, false);
    }

    private static NativeWriteExecutor createExecutor(
            List<TrackingPageSink> sinkCollector, boolean abortThrows) {
        return new NativeWriteExecutor(catalogName -> {
            ConnectorMetadata metadata = new StubMetadata();
            ConnectorPageSinkProvider provider = (txn, session, outputHandle) -> {
                var sink = new TrackingPageSink(abortThrows);
                sinkCollector.add(sink);
                return sink;
            };
            ConnectorSession session = new StubSession();
            return new CatalogResourceResolver.CatalogResources(metadata, provider, session);
        });
    }

    private static WriteDescriptor descriptor(String tableName) {
        return new WriteDescriptor(
                "test-catalog", "test-schema", tableName,
                WriteDescriptor.DmlOperation.INSERT,
                List.of(new WriteDescriptor.ColumnSchema("id", "BIGINT")),
                Map.of());
    }

    private static final class TrackingPageSink implements ConnectorPageSink {
        final AtomicBoolean aborted = new AtomicBoolean();
        final AtomicBoolean closed = new AtomicBoolean();
        private final boolean abortThrows;

        TrackingPageSink(boolean abortThrows) {
            this.abortThrows = abortThrows;
        }

        @Override
        public void appendPage(Page page) {
            // intentional no-op: tests focus on abort/close lifecycle
        }

        @Override
        public byte[] finishWithMetadata() {
            return new byte[0];
        }

        @Override
        public void abort() {
            aborted.set(true);
            if (abortThrows) {
                throw new RuntimeException("simulated abort failure");
            }
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }

    private static final class StubMetadata implements ConnectorMetadata {
        private static final ConnectorTableHandle TABLE_HANDLE =
                new ConnectorTableHandle() {};
        private static final ConnectorOutputTableHandle OUTPUT_HANDLE =
                new ConnectorOutputTableHandle() {};

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of();
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
            return List.of();
        }

        @Override
        public ConnectorTableHandle getTableHandle(
                ConnectorSession session, SchemaTableName tableName) {
            return TABLE_HANDLE;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(
                ConnectorSession session, ConnectorTableHandle table) {
            return null;
        }

        @Override
        public Map<String, org.pangrova.spi.connector.metadata.table.ColumnHandle> getColumnHandles(
                ConnectorSession session, ConnectorTableHandle table) {
            return Map.of();
        }

        @Override
        public org.pangrova.spi.connector.metadata.table.TableStatistics getTableStatistics(
                ConnectorSession session, ConnectorTableHandle table) {
            return org.pangrova.spi.connector.metadata.table.TableStatistics.UNKNOWN;
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
