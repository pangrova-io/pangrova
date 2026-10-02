package org.pangrova.worker.jni.write;

import org.pangrova.protocol.batch.ColumnarBatch;
import org.pangrova.protocol.batch.ColumnarPageSink;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSink;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/** Real stream encoding and small synchronous sinks shared by writer tests. */
final class WriteBatchFixtures {
    private WriteBatchFixtures() {}

    static byte[] integers(List<String> columns, Integer[]... rows) throws IOException {
        return integerStream(columns, rows, 1);
    }

    static byte[] integerStream(List<String> columns, Integer[][] rows, int batches) throws IOException {
        Schema schema = new Schema(columns.stream()
                .map(name -> new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), List.of()))
                .toList());
        try (var allocator = new RootAllocator(16L * 1024 * 1024);
                var root = VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew();
            for (int column = 0; column < columns.size(); column++) {
                IntVector vector = (IntVector) root.getVector(column);
                for (int row = 0; row < rows.length; row++) {
                    Integer value = rows[row][column];
                    if (value == null) {
                        vector.setNull(row);
                    } else {
                        vector.setSafe(row, value);
                    }
                }
            }
            root.setRowCount(rows.length);
            return serialize(root, batches);
        }
    }

    static byte[] serialize(VectorSchemaRoot root, int batches) throws IOException {
        try (var output = new ByteArrayOutputStream();
                var writer = new ArrowStreamWriter(root, null, output)) {
            writer.start();
            for (int batch = 0; batch < batches; batch++) {
                writer.writeBatch();
            }
            writer.end();
            return output.toByteArray();
        }
    }

    static byte[] complex(boolean duplicateMapKeys) throws IOException {
        Field item = new Field("item", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(
                field("label", ArrowType.Utf8.INSTANCE), field("quantity", new ArrowType.Int(32, true))));
        Field entries = new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE), List.of(
                new Field("key", FieldType.notNullable(new ArrowType.Int(32, true)), List.of()),
                field("value", ArrowType.Utf8.INSTANCE)));
        Schema schema = new Schema(List.of(
                field("id", new ArrowType.Int(64, true)), field("name", ArrowType.Utf8.INSTANCE),
                new Field("items", FieldType.nullable(ArrowType.List.INSTANCE), List.of(item)),
                new Field("attributes", FieldType.nullable(new ArrowType.Map(false)), List.of(entries)),
                field("payload", ArrowType.Binary.INSTANCE), field("amount", new ArrowType.Decimal(12, 2, 128))));
        try (var allocator = new RootAllocator(16L * 1024 * 1024);
                var root = VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew();
            BigIntVector ids = (BigIntVector) root.getVector("id");
            ids.setSafe(0, Long.MIN_VALUE);
            ids.setSafe(1, Long.MAX_VALUE);
            VarCharVector names = (VarCharVector) root.getVector("name");
            names.setSafe(0, "café".getBytes(StandardCharsets.UTF_8));
            names.setNull(1);
            VarBinaryVector payload = (VarBinaryVector) root.getVector("payload");
            payload.setSafe(0, new byte[] {0, 1, -1});
            payload.setSafe(1, new byte[0]);
            DecimalVector amounts = (DecimalVector) root.getVector("amount");
            amounts.setSafe(0, new BigDecimal("-1234.50"));
            amounts.setNull(1);
            populateItems((ListVector) root.getVector("items"));
            populateAttributes((MapVector) root.getVector("attributes"), duplicateMapKeys);
            root.setRowCount(2);
            return serialize(root, 1);
        }
    }

    private static Field field(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), List.of());
    }

    private static void populateItems(ListVector items) {
        StructVector values = (StructVector) items.getDataVector();
        items.startNewValue(0);
        values.setIndexDefined(0);
        values.getChild("label", VarCharVector.class).setSafe(0, "nested".getBytes(StandardCharsets.UTF_8));
        values.getChild("quantity", IntVector.class).setNull(0);
        values.setNull(1);
        items.endValue(0, 2);
        items.startNewValue(1);
        items.endValue(1, 0);
    }

    private static void populateAttributes(MapVector attributes, boolean duplicateKeys) {
        StructVector entries = (StructVector) attributes.getDataVector();
        attributes.startNewValue(0);
        entries.setIndexDefined(0);
        entries.setIndexDefined(1);
        IntVector keys = entries.getChild("key", IntVector.class);
        keys.setSafe(0, -7);
        keys.setSafe(1, duplicateKeys ? -7 : 9);
        VarCharVector values = entries.getChild("value", VarCharVector.class);
        values.setSafe(0, "typed-key".getBytes(StandardCharsets.UTF_8));
        values.setNull(1);
        attributes.endValue(0, 2);
        attributes.setNull(1);
    }

    static ByteBuffer directRange(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length + 16);
        buffer.putLong(0x123456789abcdef0L).put(bytes).putLong(0x123456789abcdef0L);
        buffer.limit(8 + bytes.length).position(8);
        return buffer;
    }

    static void append(NativeWriteExecutor executor, long handle, byte[] bytes,
                       boolean direct, int rows, int columns) {
        if (direct) {
            executor.appendDirectBatch(handle, directRange(bytes), rows, columns);
        } else {
            executor.appendDirectBatch(handle, bytes == null ? null : ByteBuffer.wrap(bytes), rows, columns);
        }
    }

    static Page snapshot(ColumnarBatch batch) {
        List<TypedRow> rows = new ArrayList<>();
        for (int row = 0; row < batch.rowCount(); row++) {
            TypedRow.Builder builder = TypedRow.builder();
            for (int column = 0; column < batch.columns().size(); column++) {
                builder.putRaw(batch.columns().get(column), batch.value(row, column));
            }
            rows.add(builder.build());
        }
        return new Page(batch.columns(), rows);
    }

    static WriteDescriptor descriptor(String... columns) {
        return new WriteDescriptor("catalog", "schema", "table", WriteDescriptor.DmlOperation.INSERT,
                List.of(columns).stream().map(name -> new WriteDescriptor.ColumnSchema(name, "INTEGER")).toList(),
                Map.of("taskId", "write-task"));
    }

    static NativeWriteExecutor executor(ConnectorPageSink sink, String... tableColumns) {
        if (tableColumns.length == 0) {
            throw new IllegalArgumentException("tableColumns must describe the target table");
        }
        return new NativeWriteExecutor(new TestCatalogResourceResolver((taskId, catalog) ->
                new CatalogResourceResolver.CatalogResources(
                new TableMetadata(List.of(tableColumns)), (transaction, session, output) -> sink,
                new WriteSession(), TestOutputHandles.outputHandle(), List.of(tableColumns))));
    }

    static class RowSink implements ConnectorPageSink {
        final List<String> events = new ArrayList<>();
        final Consumer<Page> onPage;
        int pageCalls;
        RuntimeException abortFailure;
        RuntimeException closeFailure;

        RowSink(Consumer<Page> onPage) {
            this.onPage = onPage;
        }

        @Override
        public void appendPage(Page page) {
            pageCalls++;
            onPage.accept(page);
        }

        @Override
        public byte[] finishWithMetadata() {
            events.add("finish");
            return new byte[] {1, 2, 3};
        }

        @Override
        public void abort() {
            events.add("abort");
            if (abortFailure != null) {
                throw abortFailure;
            }
        }

        @Override
        public void close() {
            events.add("close");
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    static final class BatchSink extends RowSink implements ColumnarPageSink {
        private final Consumer<ColumnarBatch> onBatch;
        int batchCalls;
        // Retained only to assert that the borrowed view is invalid after return.
        ColumnarBatch borrowed;

        BatchSink(Consumer<ColumnarBatch> onBatch) {
            super(page -> { throw new AssertionError("Columnar capability must not receive a Page"); });
            this.onBatch = onBatch;
        }

        @Override
        public void appendColumnarBatch(ColumnarBatch batch) {
            batchCalls++;
            borrowed = batch;
            onBatch.accept(batch);
        }
    }

    private record TableMetadata(List<String> columns) implements ConnectorMetadata {
        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of("schema");
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schema) {
            return List.of(new SchemaTableName("schema", "table"));
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName table) {
            return new ConnectorTableHandle() {};
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
            return new ConnectorTableMetadata(new SchemaTableName("schema", "table"), columns, List.of(), Map.of());
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
            return new ConnectorOutputTableHandle() {};
        }
    }

    private static final class WriteSession implements ConnectorSession {
        @Override public String getUser() { return "test-user"; }
        @Override public Optional<String> getCatalog() { return Optional.of("catalog"); }
        @Override public Optional<String> getSchema() { return Optional.of("schema"); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    }
}
