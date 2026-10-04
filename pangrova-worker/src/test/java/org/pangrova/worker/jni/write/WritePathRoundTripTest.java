package org.pangrova.worker.jni.write;

import org.pangrova.worker.jni.write.catalog.CatalogResourceResolver;

import org.pangrova.protocol.batch.ColumnarBatch;
import org.pangrova.protocol.batch.ColumnarPageSink;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ColumnValue;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies Arrow IPC round-trip correctness for the write path.
 *
 * <p>Serializes data to Arrow IPC format (simulating what the native C++ side
 * does), passes it through {@link NativeWriteExecutor#appendSerializedBatch},
 * and verifies the connector page sink receives correctly decoded data.</p>
 *
 * <p>This validates requirement C.6: the write path uses Arrow IPC consistently
 * with the read path, and schema propagation works correctly through the format.</p>
 */
class WritePathRoundTripTest {
    private NativeWriteExecutor executor;

    @AfterEach
    void closeExecutor() {
        if (executor != null) {
            executor.close();
        }
    }

    @Test
    void writeViaSerializedBatchPreservesDataCorrectly() throws Exception {
        var receivedPages = new ArrayList<Page>();
        executor = createExecutor(receivedPages);

        long handle = executor.beginWrite(descriptor("round_trip_table"));

        byte[] batchData = buildBatch(5);
        executor.appendDirectBatch(handle, WriteBatchFixtures.directRange(batchData), 5, 2);

        WriteCommitResult result = executor.commitWrite(handle);

        assertThat(result.rowsWritten()).isEqualTo(5);
        assertThat(receivedPages).hasSize(1);

        Page page = receivedPages.get(0);
        assertThat(page.columns()).containsExactly("id", "name");
        assertThat(page.getPositionCount()).isEqualTo(5);

        for (int i = 0; i < 5; i++) {
            var row = page.rows().get(i);
            assertThat(ColumnValue.toRaw(row.get("id"))).isEqualTo(i);
            assertThat(ColumnValue.toRaw(row.get("name"))).isEqualTo("row-" + i);
        }
    }

    @Test
    void writeMultipleBatchesPreservesOrderAndSchema() throws Exception {
        var receivedPages = new ArrayList<Page>();
        executor = createExecutor(receivedPages);

        long handle = executor.beginWrite(descriptor("multi_batch_table"));

        byte[] batch1 = buildBatch(3);
        byte[] batch2 = buildBatchWithOffset(4, 3);
        executor.appendDirectBatch(handle, WriteBatchFixtures.directRange(batch1), 3, 2);
        executor.appendDirectBatch(handle, WriteBatchFixtures.directRange(batch2), 4, 2);

        WriteCommitResult result = executor.commitWrite(handle);

        assertThat(result.rowsWritten()).isEqualTo(7);
        assertThat(receivedPages).hasSize(2);

        // First batch: rows 0-2
        Page page1 = receivedPages.get(0);
        assertThat(page1.columns()).containsExactly("id", "name");
        assertThat(page1.getPositionCount()).isEqualTo(3);
        assertThat(ColumnValue.toRaw(page1.rows().get(0).get("id"))).isEqualTo(0);

        // Second batch: rows 3-6
        Page page2 = receivedPages.get(1);
        assertThat(page2.columns()).containsExactly("id", "name");
        assertThat(page2.getPositionCount()).isEqualTo(4);
        assertThat(ColumnValue.toRaw(page2.rows().get(0).get("id"))).isEqualTo(3);
    }

    @Test
    void emptyBatchProducesEmptyPage() throws Exception {
        var receivedPages = new ArrayList<Page>();
        executor = createExecutor(receivedPages);

        long handle = executor.beginWrite(descriptor("empty_table"));

        byte[] emptyBatch = buildBatch(0);
        executor.appendDirectBatch(handle, WriteBatchFixtures.directRange(emptyBatch), 0, 2);

        executor.commitWrite(handle);

        assertThat(receivedPages).hasSize(1);
        Page page = receivedPages.get(0);
        assertThat(page.getPositionCount()).isZero();
    }

    @Test
    void schemaPreservedThroughRoundTrip() throws Exception {
        var receivedPages = new ArrayList<Page>();
        executor = createExecutor(receivedPages);

        long handle = executor.beginWrite(descriptor("schema_table"));

        byte[] batchData = buildBatch(2);
        executor.appendDirectBatch(handle, WriteBatchFixtures.directRange(batchData), 2, 2);
        executor.commitWrite(handle);

        Page page = receivedPages.get(0);
        // Schema propagation: column names from Arrow IPC schema must match
        assertThat(page.columns()).containsExactly("id", "name");
        // Verify types are correctly decoded
        assertThat(ColumnValue.toRaw(page.rows().get(0).get("id"))).isEqualTo(0);
        assertThat(ColumnValue.toRaw(page.rows().get(0).get("name"))).isEqualTo("row-0");
    }

    // --- Test infrastructure ---

    private static byte[] buildBatch(int rowCount) throws IOException {
        return buildBatchWithOffset(rowCount, 0);
    }

    private static byte[] buildBatchWithOffset(int rowCount, int startId) throws IOException {
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

    private static NativeWriteExecutor createExecutor(List<Page> pageCollector) {
        return new NativeWriteExecutor(new TestCatalogResourceResolver((taskId, catalogName) -> {
            ConnectorMetadata metadata = new StubMetadata();
            ConnectorPageSinkProvider provider = (txn, session, outputHandle) ->
                    new CollectingPageSink(pageCollector);
            ConnectorSession session = new StubSession();
            return new CatalogResourceResolver.CatalogResources(
                    metadata, provider, session, TestOutputHandles.outputHandle(),
                    List.of("id", "name"));
        }));
    }

    private static WriteDescriptor descriptor(String tableName) {
        return new WriteDescriptor(
                "test-catalog", "test-schema", tableName,
                WriteDescriptor.DmlOperation.INSERT,
                List.of(
                        new WriteDescriptor.ColumnSchema("id", "INTEGER"),
                        new WriteDescriptor.ColumnSchema("name", "VARCHAR")),
                Map.of("taskId", "round-trip-task"));
    }

    private static final class CollectingPageSink implements ColumnarPageSink {
        private final List<Page> pages;

        CollectingPageSink(List<Page> pages) {
            this.pages = pages;
        }

        @Override
        public void appendPage(Page page) {
            throw new AssertionError("Columnar capability must not dispatch appendPage");
        }

        @Override
        public void appendColumnarBatch(ColumnarBatch batch) {
            pages.add(WriteBatchFixtures.snapshot(batch));
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
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of();
        }

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
        @Override public String getUser() { return "test-user"; }
        @Override public Optional<String> getCatalog() { return Optional.of("test-catalog"); }
        @Override public Optional<String> getSchema() { return Optional.of("test-schema"); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    }
}
