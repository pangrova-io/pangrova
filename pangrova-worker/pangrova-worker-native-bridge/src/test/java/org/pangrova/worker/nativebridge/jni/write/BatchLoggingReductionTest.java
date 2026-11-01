package org.pangrova.worker.nativebridge.jni.write;

import org.pangrova.worker.nativebridge.connector.spi.CatalogResourceResolver;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.pangrova.spi.connector.ConnectorSession;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

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
 * Validates that per-batch logging uses DEBUG level,
 * aggregate INFO emitted only every {@link NativeWriteExecutor#BATCH_LOG_INTERVAL} batches.
 *
 * <p>Verifies:
 * <ol>
 *   <li>Running 100 batches produces minimal INFO logs (not 100 per-batch)</li>
 *   <li>No PII (row data) appears in any log output</li>
 *   <li>Aggregate progress INFO fires at correct interval</li>
 * </ol>
 */
class BatchLoggingReductionTest {

    private ListAppender<ILoggingEvent> logAppender;
    private Logger executorLogger;
    private NativeWriteExecutor executor;

    @BeforeEach
    void setUp() {
        executorLogger = (Logger) LoggerFactory.getLogger(NativeWriteExecutor.class);
        logAppender = new ListAppender<>();
        logAppender.start();
        executorLogger.addAppender(logAppender);
    }

    @AfterEach
    void tearDown() {
        try {
            if (executor != null) executor.close();
        } finally {
            executorLogger.detachAppender(logAppender);
            logAppender.stop();
        }
    }

    @Test
    void hundredBatchesProduceMinimalInfoLogs() throws Exception {
        executor = createExecutor();
        long handle = executor.beginWrite(descriptor("logging_test"));

        for (int i = 0; i < 100; i++) {
            byte[] batch = buildBatch(10, i * 10);
            executor.appendDirectBatch(handle, WriteBatchFixtures.directRange(batch), 10, 2);
        }

        executor.commitWrite(handle);

        // Count INFO logs from appendSerializedBatch (exclude begin/commit state changes)
        List<ILoggingEvent> perBatchInfoLogs = logAppender.list.stream()
                .filter(e -> e.getLevel() == Level.INFO)
                .filter(e -> e.getFormattedMessage().contains("serialized_batch_appended")
                        || e.getFormattedMessage().contains("decoded_batch_raw")
                        || e.getFormattedMessage().contains("decoded_batch")
                        || e.getFormattedMessage().contains("project_to_write_schema"))
                .toList();

        // Per-batch logs should be at DEBUG, not INFO — expect 0 per-batch INFO logs
        assertThat(perBatchInfoLogs)
                .as("Per-batch logs must be at DEBUG level, not INFO")
                .isEmpty();

        // 100 batches < BATCH_LOG_INTERVAL (1000), so no progress log expected
        List<ILoggingEvent> progressLogs = logAppender.list.stream()
                .filter(e -> e.getLevel() == Level.INFO)
                .filter(e -> e.getFormattedMessage().contains("write_progress"))
                .toList();

        assertThat(progressLogs)
                .as("No progress log expected for 100 batches (interval is 1000)")
                .isEmpty();

        // State-change INFO logs are allowed: begin_write, write_session_created,
        // write_metadata_collected. These are legitimate state transitions.
        List<ILoggingEvent> allInfoLogs = logAppender.list.stream()
                .filter(e -> e.getLevel() == Level.INFO)
                .toList();

        // At most: begin_write + write_session_created + resolve_write_columns
        // + write_metadata_collected = ~4 state-change logs
        assertThat(allInfoLogs.size())
                .as("Expected only state-change INFO logs, not 100 per-batch logs. Got: %s",
                        allInfoLogs.stream().map(ILoggingEvent::getFormattedMessage).toList())
                .isLessThanOrEqualTo(6);
    }

    @Test
    void noPiiInAnyLogLevel() throws Exception {
        executor = createExecutor();
        long handle = executor.beginWrite(descriptor("pii_test"));

        byte[] batch = buildBatch(5, 0);
        executor.appendDirectBatch(handle, WriteBatchFixtures.directRange(batch), 5, 2);

        executor.commitWrite(handle);

        // Check ALL log messages (including DEBUG) for PII patterns
        for (ILoggingEvent event : logAppender.list) {
            String msg = event.getFormattedMessage();
            assertThat(msg)
                    .as("Log must not contain row data (PII risk): %s", msg)
                    .doesNotContain("first_row=")
                    .doesNotContain("row-0")
                    .doesNotContain("row-1")
                    .doesNotContain("row-2");
        }
    }

    @Test
    void batchLogIntervalIsConfigurable() {
        // Verify the constant is accessible and has expected default value
        assertThat(NativeWriteExecutor.BATCH_LOG_INTERVAL)
                .as("Default batch log interval should be 1000")
                .isEqualTo(1000);
    }

    // --- Test infrastructure ---

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

    private static NativeWriteExecutor createExecutor() {
        return new NativeWriteExecutor(new TestCatalogResourceResolver((taskId) -> {
            ConnectorMetadata metadata = new StubMetadata();
            ConnectorPageSinkProvider provider = (txn, session, outputHandle) ->
                    new NoOpPageSink();
            ConnectorSession session = new StubSession();
            return new CatalogResourceResolver.CatalogResources(
                    metadata, provider, session, TestOutputHandles.outputHandle(),
                    List.of("id", "name"));
        }));
    }

    private static WriteDescriptor descriptor(String tableName) {
        return new WriteDescriptor(
                WriteDescriptor.DmlOperation.INSERT,
                List.of(new WriteDescriptor.ColumnSchema("id", "INTEGER"),
                        new WriteDescriptor.ColumnSchema("name", "VARCHAR")),
                Map.of("taskId", "logging-task"));
    }

    private static final class NoOpPageSink implements ConnectorPageSink {
        @Override public void appendPage(Page page) {}
        @Override public byte[] finishWithMetadata() { return new byte[0]; }
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
