package org.pangrova.transport;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float4Vector;
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
import java.sql.Types;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ArrowBatchDecoderVectorTest {

    private static final Field FLOAT_CHILD = new Field(
            "item", FieldType.notNullable(new ArrowType.FloatingPoint(
                    FloatingPointPrecision.SINGLE)), null);

    @Test
    void annotatedVectorRoundTripsWithoutConvertingOrdinaryFloatArray() throws Exception {
        byte[] ipc = listBatch("2", new float[] {0.25f, -0.5f}, true);

        ResultBatch result = ArrowBatchDecoder.decode(ipc);

        assertThat(result.columns()).hasSize(2);
        assertThat(result.columns().get(0).typeName()).isEqualTo("VECTOR(2)");
        assertThat(result.columns().get(0).sqlType()).isEqualTo(Types.ARRAY);
        assertThat(result.columns().get(0).precision()).isEqualTo(2);
        assertThat(result.columns().get(1).typeName()).doesNotContain("VECTOR");
        assertThat(result.rows()).hasSize(1);
        assertThat(result.rows().get(0)[0]).isEqualTo(new float[] {0.25f, -0.5f});
        assertThat(result.rows().get(0)[1]).isInstanceOf(List.class);
    }

    @Test
    void annotatedVectorRejectsDimensionMismatch() throws Exception {
        byte[] ipc = listBatch("3", new float[] {0.25f, -0.5f}, false);

        assertThatThrownBy(() -> ArrowBatchDecoder.decode(ipc))
                .isInstanceOf(TransportException.class)
                .hasMessageContaining("declared dimension 3");
    }

    @Test
    void annotatedVectorRejectsNonFiniteCoordinate() throws Exception {
        byte[] ipc = listBatch("2", new float[] {Float.NaN, -0.5f}, false);

        assertThatThrownBy(() -> ArrowBatchDecoder.decode(ipc))
                .isInstanceOf(TransportException.class)
                .hasMessageContaining("finite Float32");
    }

    @Test
    void annotatedVectorRejectsInvalidDimensionMetadata() throws Exception {
        byte[] ipc = listBatch("0", new float[] {0.25f, -0.5f}, false);

        assertThatThrownBy(() -> ArrowBatchDecoder.decode(ipc))
                .isInstanceOf(TransportException.class)
                .hasMessageContaining("positive decimal dimension");
    }

    @Test
    void annotatedVectorRejectsWrongStorageTypeBeforeReadingRows() throws Exception {
        Field integerChild = new Field("item",
                FieldType.notNullable(new ArrowType.Int(32, true)), null);
        Field vector = new Field("embedding", new FieldType(true,
                ArrowType.List.INSTANCE, null,
                Map.of("ARROW:extension:name", "pangrova.vector",
                        "ARROW:extension:metadata", "2")), List.of(integerChild));
        byte[] ipc = emptyBatch(new Schema(List.of(vector)));

        assertThatThrownBy(() -> ArrowBatchDecoder.decode(ipc))
                .isInstanceOf(TransportException.class)
                .hasMessageContaining("List<Float32> storage");
    }

    private static byte[] listBatch(
            String dimension, float[] coordinates, boolean withOrdinaryArray)
            throws Exception {
        Field vector = new Field("embedding", new FieldType(true,
                ArrowType.List.INSTANCE, null,
                Map.of("ARROW:extension:name", "pangrova.vector",
                        "ARROW:extension:metadata", dimension)), List.of(FLOAT_CHILD));
        List<Field> fields = withOrdinaryArray
                ? List.of(vector, new Field("ordinary_array",
                        FieldType.nullable(ArrowType.List.INSTANCE), List.of(FLOAT_CHILD)))
                : List.of(vector);
        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), allocator)) {
            root.allocateNew();
            for (int col = 0; col < fields.size(); col++) {
                ListVector list = (ListVector) root.getVector(col);
                list.startNewValue(0);
                Float4Vector child = (Float4Vector) list.getDataVector();
                for (int i = 0; i < coordinates.length; i++) {
                    child.setSafe(i, coordinates[i]);
                }
                list.endValue(0, coordinates.length);
            }
            root.setRowCount(1);
            return writeRoot(root);
        }
    }

    private static byte[] emptyBatch(Schema schema) throws Exception {
        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew();
            root.setRowCount(0);
            return writeRoot(root);
        }
    }

    private static byte[] writeRoot(VectorSchemaRoot root) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(
                root, null, Channels.newChannel(output))) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        return output.toByteArray();
    }
}
