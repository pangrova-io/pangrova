package org.pangrova.client.cli;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that ResultBatchDecoder correctly decodes Arrow IPC bytes into readable row data,
 * replacing the previous empty-table behavior.
 *
 * <p><b>Validates: Requirements 1.52, 2.52</b></p>
 */
class ResultBatchDecoderTest {

    @Test
    void actualSchemaDisplaysRowsWhenAsynchronousSubmitHasNoColumns() throws Exception {
        String display = SqlShell.formatResults(buildIpcWithIntAndString(), List.of());
        assertThat(display).contains("id", "name", "Alice", "Bob", "Charlie");
        assertThat(display).doesNotContain("OK (no result set)");
    }

    @Test
    void actualEmptyResultSchemaOverridesStaleSubmitColumnNames() throws Exception {
        String display = SqlShell.formatResults(buildEmptyIpc(), List.of("stale"));
        assertThat(display).contains("id", "name").doesNotContain("stale");
    }

    @Test
    void decodesIntAndStringColumnsIntoRows() throws Exception {
        byte[] batchData = buildIpcWithIntAndString();

        ResultBatchDecoder.DecodedResult result = ResultBatchDecoder.decode(batchData);

        assertThat(result.columnNames()).containsExactly("id", "name");
        assertThat(result.rows()).hasSize(3);
        assertThat(result.rows().get(0)).containsExactly("1", "Alice");
        assertThat(result.rows().get(1)).containsExactly("2", "Bob");
        assertThat(result.rows().get(2)).containsExactly("3", "Charlie");
    }

    @Test
    void handlesNullValues() throws Exception {
        byte[] batchData = buildIpcWithNulls();

        ResultBatchDecoder.DecodedResult result = ResultBatchDecoder.decode(batchData);

        assertThat(result.columnNames()).containsExactly("id", "name");
        assertThat(result.rows()).hasSize(2);
        assertThat(result.rows().get(0)).containsExactly("1", "NULL");
        assertThat(result.rows().get(1)).containsExactly("NULL", "Bob");
    }

    @Test
    void handlesEmptyResult() throws Exception {
        byte[] batchData = buildEmptyIpc();

        ResultBatchDecoder.DecodedResult result = ResultBatchDecoder.decode(batchData);

        assertThat(result.columnNames()).containsExactly("id", "name");
        assertThat(result.rows()).isEmpty();
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    void handlesNullInput() {
        ResultBatchDecoder.DecodedResult result = ResultBatchDecoder.decode(null);
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    void handlesEmptyByteArray() {
        ResultBatchDecoder.DecodedResult result = ResultBatchDecoder.decode(new byte[0]);
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    void corruptBatchDataThrowsResultDecodeException() {
        byte[] corrupt = {0x01, 0x02, 0x03, 0x04, 0x05};
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> ResultBatchDecoder.decode(corrupt))
                .isInstanceOf(ResultDecodeException.class)
                .hasMessageContaining("Failed to decode Arrow IPC response");
    }

    @Test
    void decodesMultipleArrowTypes() throws Exception {
        byte[] batchData = buildIpcWithMultipleTypes();

        ResultBatchDecoder.DecodedResult result = ResultBatchDecoder.decode(batchData);

        assertThat(result.columnNames()).containsExactly("int_col", "float_col", "bool_col", "bigint_col");
        assertThat(result.rows()).hasSize(2);
        assertThat(result.rows().get(0).get(0)).isEqualTo("42");
        assertThat(result.rows().get(0).get(1)).isEqualTo("3.14");
        assertThat(result.rows().get(0).get(2)).isEqualTo("true");
        assertThat(result.rows().get(0).get(3)).isEqualTo("9999999999");
    }

    @Test
    void truncatesLongValues() throws Exception {
        String longValue = "x".repeat(100);
        byte[] batchData = buildIpcWithLongValue(longValue);

        ResultBatchDecoder.DecodedResult result = ResultBatchDecoder.decode(batchData);

        assertThat(result.rows()).hasSize(1);
        String cell = result.rows().get(0).get(0);
        assertThat(cell).hasSize(ResultBatchDecoder.MAX_CELL_WIDTH);
        assertThat(cell).endsWith("...");
    }

    @Test
    void formattedTableContainsActualData() throws Exception {
        byte[] batchData = buildIpcWithIntAndString();
        ResultBatchDecoder.DecodedResult result = ResultBatchDecoder.decode(batchData);

        String table = TableFormatter.format(result.columnNames(), result.rows());

        assertThat(table).contains("Alice");
        assertThat(table).contains("Bob");
        assertThat(table).contains("Charlie");
        assertThat(table).contains("id");
        assertThat(table).contains("name");
        assertThat(table).contains("3 rows");
    }

    @Test
    void formatsVectorCoordinatesInsteadOfArrayIdentity() throws Exception {
        Field child = new Field("item", FieldType.notNullable(
                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
        Field vector = new Field("embedding", new FieldType(true, ArrowType.List.INSTANCE,
                null, Map.of("ARROW:extension:name", "pangrova.vector",
                        "ARROW:extension:metadata", "2")), List.of(child));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(vector)), allocator)) {
            root.allocateNew();
            ListVector list = (ListVector) root.getVector(0);
            list.startNewValue(0);
            Float4Vector coordinates = (Float4Vector) list.getDataVector();
            coordinates.setSafe(0, 0.25f);
            coordinates.setSafe(1, -0.5f);
            list.endValue(0, 2);
            root.setRowCount(1);

            ResultBatchDecoder.DecodedResult result = ResultBatchDecoder.decode(serializeToIpc(root));

            assertThat(result.rows().get(0)).containsExactly("[0.25, -0.5]");
        }
    }

    // ---- Arrow IPC byte builders ----

    private static byte[] buildIpcWithIntAndString() throws Exception {
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

            IntVector idVec = (IntVector) root.getVector("id");
            VarCharVector nameVec = (VarCharVector) root.getVector("name");

            root.allocateNew();
            idVec.setSafe(0, 1);
            nameVec.setSafe(0, "Alice".getBytes(StandardCharsets.UTF_8));
            idVec.setSafe(1, 2);
            nameVec.setSafe(1, "Bob".getBytes(StandardCharsets.UTF_8));
            idVec.setSafe(2, 3);
            nameVec.setSafe(2, "Charlie".getBytes(StandardCharsets.UTF_8));
            root.setRowCount(3);

            return serializeToIpc(root);
        }
    }

    private static byte[] buildIpcWithNulls() throws Exception {
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

            IntVector idVec = (IntVector) root.getVector("id");
            VarCharVector nameVec = (VarCharVector) root.getVector("name");

            root.allocateNew();
            idVec.setSafe(0, 1);
            nameVec.setNull(0);
            idVec.setNull(1);
            nameVec.setSafe(1, "Bob".getBytes(StandardCharsets.UTF_8));
            root.setRowCount(2);

            return serializeToIpc(root);
        }
    }

    private static byte[] buildEmptyIpc() throws Exception {
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew();
            root.setRowCount(0);
            // Write schema only, no batches with data
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(
                    root, null, Channels.newChannel(baos))) {
                writer.start();
                writer.end();
            }
            return baos.toByteArray();
        }
    }

    private static byte[] buildIpcWithMultipleTypes() throws Exception {
        Schema schema = new Schema(List.of(
                new Field("int_col", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("float_col", FieldType.nullable(new ArrowType.FloatingPoint(
                        org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)), null),
                new Field("bool_col", FieldType.nullable(ArrowType.Bool.INSTANCE), null),
                new Field("bigint_col", FieldType.nullable(new ArrowType.Int(64, true)), null)));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

            IntVector intVec = (IntVector) root.getVector("int_col");
            Float8Vector floatVec = (Float8Vector) root.getVector("float_col");
            BitVector boolVec = (BitVector) root.getVector("bool_col");
            BigIntVector bigintVec = (BigIntVector) root.getVector("bigint_col");

            root.allocateNew();
            intVec.setSafe(0, 42);
            floatVec.setSafe(0, 3.14);
            boolVec.setSafe(0, 1);
            bigintVec.setSafe(0, 9999999999L);

            intVec.setSafe(1, 0);
            floatVec.setSafe(1, -1.5);
            boolVec.setSafe(1, 0);
            bigintVec.setSafe(1, -100L);

            root.setRowCount(2);
            return serializeToIpc(root);
        }
    }

    private static byte[] buildIpcWithLongValue(String longValue) throws Exception {
        Schema schema = new Schema(List.of(
                new Field("data", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

            VarCharVector vec = (VarCharVector) root.getVector("data");
            root.allocateNew();
            vec.setSafe(0, longValue.getBytes(StandardCharsets.UTF_8));
            root.setRowCount(1);

            return serializeToIpc(root);
        }
    }

    private static byte[] serializeToIpc(VectorSchemaRoot root) throws Exception {
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
