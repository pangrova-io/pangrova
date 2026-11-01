package org.pangrova.worker.nativebridge.jni.write;

import org.pangrova.worker.nativebridge.connector.spi.CatalogResourceResolver;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.value.ColumnValue;
import org.pangrova.spi.connector.data.ConnectorPageSink;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises repeated appends and independent sessions under the executor's shared allocator. */
class WriteSessionAllocatorReuseTest {
    private NativeWriteExecutor executor;

    @AfterEach
    void closeExecutor() {
        if (executor != null) {
            executor.close();
        }
    }

    @Test
    void multipleBatchesReuseSessionAllocator() throws Exception {
        var receivedPages = new ArrayList<Page>();
        executor = createExecutor(receivedPages);

        long handle = executor.beginWrite(descriptor("allocator_reuse_table"));

        // Append multiple batches under one live session.
        int batchCount = 10;
        for (int i = 0; i < batchCount; i++) {
            byte[] batch = buildBatch(5, i * 5);
            executor.appendDirectBatch(handle, WriteBatchFixtures.directRange(batch), 5, 2);
        }

        WriteCommitResult result = executor.commitWrite(handle);

        // All 10 batches were processed successfully
        assertThat(result.rowsWritten()).isEqualTo(50);
        assertThat(receivedPages).hasSize(batchCount * 5);

        // Row-only sinks receive one bounded page per row, in input order.
        for (int i = 0; i < receivedPages.size(); i++) {
            Page page = receivedPages.get(i);
            assertThat(page.columns()).containsExactly("id", "name");
            assertThat(page.getPositionCount()).isEqualTo(1);
            assertThat(ColumnValue.toRaw(page.rows().get(0).get("id"))).isEqualTo(i);
            assertThat(ColumnValue.toRaw(page.rows().get(0).get("name"))).isEqualTo("row-" + i);
        }
    }

    @Test
    void allocatorIsClosedOnCommit() throws Exception {
        var receivedPages = new ArrayList<Page>();
        executor = createExecutor(receivedPages);

        long handle = executor.beginWrite(descriptor("close_on_commit_table"));

        byte[] batch = buildBatch(3, 0);
        executor.appendDirectBatch(handle, WriteBatchFixtures.directRange(batch), 3, 2);

        // Commit ends the session; teardown closes the shared allocator and detects leaks.
        executor.commitWrite(handle);

        assertThat(receivedPages).hasSize(3);
    }

    @Test
    void allocatorIsClosedOnAbort() throws Exception {
        var receivedPages = new ArrayList<Page>();
        executor = createExecutor(receivedPages);

        long handle = executor.beginWrite(descriptor("close_on_abort_table"));

        byte[] batch = buildBatch(3, 0);
        executor.appendDirectBatch(handle, WriteBatchFixtures.directRange(batch), 3, 2);

        // Abort must leave no decoded buffers for executor teardown.
        executor.abortWrite(handle);
    }

    @Test
    void allocatorIsClosedOnCloseAllSessions() throws Exception {
        var receivedPages = new ArrayList<Page>();
        executor = createExecutor(receivedPages);

        long handle = executor.beginWrite(descriptor("close_all_table"));

        byte[] batch = buildBatch(3, 0);
        executor.appendDirectBatch(handle, WriteBatchFixtures.directRange(batch), 3, 2);

        // Close all sessions before the shared allocator is closed in teardown.
        executor.closeAllSessions();

        assertThat(executor.activeSessionCount()).isZero();
    }

    @Test
    void separateSessionsGetIndependentAllocators() throws Exception {
        var receivedPages = new ArrayList<Page>();
        executor = createExecutor(receivedPages);

        long handle1 = executor.beginWrite(descriptor("table_a"));
        long handle2 = executor.beginWrite(descriptor("table_b"));

        // Each session can process batches independently
        byte[] batch = buildBatch(3, 0);
        executor.appendDirectBatch(handle1, WriteBatchFixtures.directRange(batch), 3, 2);
        executor.appendDirectBatch(handle2, WriteBatchFixtures.directRange(batch), 3, 2);

        // Committing one session should not affect the other
        executor.commitWrite(handle1);

        // Second session still works after first is committed
        executor.appendDirectBatch(handle2, WriteBatchFixtures.directRange(batch), 3, 2);
        executor.commitWrite(handle2);

        // Three rows from handle1, six rows from handle2.
        assertThat(receivedPages).hasSize(9);
    }

    @Test
    void catalogResolutionOccursOnceAcrossRepeatedAppends() throws Exception {
        var resolutionCount = new AtomicInteger(0);
        executor = createResolutionCountingExecutor(new ArrayList<>(), resolutionCount);

        long handle = executor.beginWrite(descriptor("counting_table"));
        assertThat(resolutionCount.get()).isEqualTo(1);

        // Append many batches — count should NOT increase
        for (int i = 0; i < 20; i++) {
            byte[] batch = buildBatch(5, i * 5);
            executor.appendDirectBatch(handle, WriteBatchFixtures.directRange(batch), 5, 2);
        }

        assertThat(resolutionCount.get())
            .as("Catalog resources are resolved once, not once per batch")
                .isEqualTo(1);

        executor.commitWrite(handle);
    }

    // --- Test infrastructure ---

    /** Counts catalog lookups without claiming to instrument allocator construction. */
    private static NativeWriteExecutor createResolutionCountingExecutor(
            List<Page> pageCollector, AtomicInteger resolutionCount) {
        return new NativeWriteExecutor(new TestCatalogResourceResolver((taskId) -> {
            resolutionCount.incrementAndGet();
            ConnectorMetadata metadata = new StubMetadata();
            ConnectorPageSinkProvider provider = (txn, session, outputHandle) ->
                    new CollectingPageSink(pageCollector);
            ConnectorSession session = new StubSession();
            return new CatalogResourceResolver.CatalogResources(
                    metadata, provider, session, TestOutputHandles.outputHandle(),
                    List.of("id", "name"));
        }));
    }

    private static NativeWriteExecutor createExecutor(List<Page> pageCollector) {
        return new NativeWriteExecutor(new TestCatalogResourceResolver((taskId) -> {
            ConnectorMetadata metadata = new StubMetadata();
            ConnectorPageSinkProvider provider = (txn, session, outputHandle) ->
                    new CollectingPageSink(pageCollector);
            ConnectorSession session = new StubSession();
            return new CatalogResourceResolver.CatalogResources(
                    metadata, provider, session, TestOutputHandles.outputHandle(),
                    List.of("id", "name"));
        }));
    }

    private static byte[] buildBatch(int rowCount, int startId) throws IOException {
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));

        try (BufferAllocator allocator = new RootAllocator(16L * 1024 * 1024);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            IntVector idVec = (IntVector) root.getVector("id");
            VarCharVector nameVec = (VarCharVector) root.getVector("name");
            root.allocateNew();
            for (int i = 0; i < rowCount; i++) {
                idVec.setSafe(i, startId + i);
                nameVec.setSafe(i, ("row-" + (startId + i)).getBytes(StandardCharsets.UTF_8));
            }
            root.setRowCount(rowCount);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(
                    root, null, Channels.newChannel(baos))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return baos.toByteArray();
        }
    }

    private static WriteDescriptor descriptor(String tableName) {
        return new WriteDescriptor(
                WriteDescriptor.DmlOperation.INSERT,
                List.of(
                        new WriteDescriptor.ColumnSchema("id", "INTEGER"),
                        new WriteDescriptor.ColumnSchema("name", "VARCHAR")),
                Map.of("taskId", "allocator-task"));
    }

    private static final class CollectingPageSink implements ConnectorPageSink {
        private final List<Page> pages;

        CollectingPageSink(List<Page> pages) {
            this.pages = pages;
        }

        @Override
        public void appendPage(Page page) {
            pages.add(page);
        }

        @Override
        public byte[] finishWithMetadata() {
            return new byte[0];
        }

        @Override
        public void abort() {}

        @Override
        public void close() {}
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
        public Map<String, org.pangrova.spi.connector.metadata.table.ColumnHandle> getColumnHandles(
                ConnectorSession session, ConnectorTableHandle table) {
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
        @Override
        public java.util.Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "test-user"; }
        @Override public Optional<String> getCatalog() { return Optional.of("test-catalog"); }
        @Override public Optional<String> getSchema() { return Optional.of("test-schema"); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    }
}
