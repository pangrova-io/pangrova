package org.pangrova.transport.client;

import org.pangrova.transport.client.exception.TransportException;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pangrova.protocol.type.VectorLogicalType;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ArrowBatchDecoderVectorTest {

    @Test
    void canonicalMapEntriesAndNestedTextBecomeJavaValues() {
        Field key = new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null);
        Field value = new Field("value", FieldType.nullable(new ArrowType.Utf8()), null);
        Field entries = new Field("entries", FieldType.notNullable(new ArrowType.Struct()),
                List.of(key, value));
        Field map = new Field("props", FieldType.nullable(new ArrowType.Map(false)),
                List.of(entries));
        Object raw = List.of(
                Map.of("key", new Text("color"), "value", new Text("red")),
                Map.of("key", new Text("size"), "value", new Text("large")));

        assertThat(ArrowBatchDecoder.normalizeComplexValue(raw, map))
                .isEqualTo(Map.of("color", "red", "size", "large"));
        assertThatThrownBy(() -> ArrowBatchDecoder.normalizeComplexValue(List.of(
                Map.of("key", new Text("color"), "value", new Text("red")),
                Map.of("key", new Text("color"), "value", new Text("blue"))), map))
                .isInstanceOf(TransportException.class)
                .hasMessageContaining("duplicate key");
    }

    @ParameterizedTest
    @CsvSource({"false,false,false,false", "false,false,false,true", "false,false,true,false", "false,false,true,true",
            "false,true,false,false", "false,true,false,true", "false,true,true,false", "false,true,true,true",
            "true,false,false,false", "true,false,false,true", "true,false,true,false", "true,false,true,true",
            "true,true,false,false", "true,true,false,true", "true,true,true,false", "true,true,true,true"})
    void fullSchemaControlsCoordinatesForBothLayoutsEvenWhenTheBatchIsEmpty(
            boolean doublePrecision, boolean fixed, boolean parentNullable, boolean childNullable) throws Exception {
        Field field = vectorField(doublePrecision, fixed, parentNullable, childNullable);
        var expectedType = VectorLogicalType.create(3,
                doublePrecision ? VectorLogicalType.FLOAT64 : VectorLogicalType.FLOAT32, parentNullable, childNullable);
        ResultBatch empty = ArrowBatchDecoder.decode(emptyBatch(new Schema(List.of(field))));
        assertThat(empty.rows()).isEmpty();
        assertThat(empty.columns().get(0).logicalType()).contains(expectedType);
        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(field)), allocator)) {
            root.allocateNew();
            fillVector(root.getVector(0), doublePrecision, childNullable);
            if (parentNullable) root.getVector(0).setNull(2);
            root.setRowCount(parentNullable ? 3 : 2);
            ResultBatch decoded = ArrowBatchDecoder.decode(writeRoot(root));
            assertThat(decoded.columns()).isEqualTo(empty.columns());
            Object first = decoded.rows().get(0)[0];
            Object nonNull = decoded.rows().get(1)[0];
            if (doublePrecision && childNullable) {
                assertThat(first).isInstanceOf(Double[].class);
                assertThat((Double[]) first).containsExactly(1.0000000000000002, null, 3.5);
                assertThat((Double[]) nonNull).containsExactly(1.0000000000000002, -123456789.12345679, 3.5);
            } else if (doublePrecision) {
                assertThat(first).isInstanceOf(double[].class);
                assertThat((double[]) first).containsExactly(1.0000000000000002, -123456789.12345679, 3.5);
            } else if (childNullable) {
                assertThat(first).isInstanceOf(Float[].class);
                assertThat((Float[]) first).containsExactly(0.25f, null, 3.5f);
                assertThat((Float[]) nonNull).containsExactly(0.25f, -0.5f, 3.5f);
            } else {
                assertThat(first).isInstanceOf(float[].class);
                assertThat((float[]) first).containsExactly(0.25f, -0.5f, 3.5f);
            }
            assertThat(nonNull.getClass()).isEqualTo(first.getClass());
            if (parentNullable) assertThat(decoded.rows().get(2)[0]).isNull();
        }
    }

    @Test
    void requiredRowsAndRequiredCoordinatesRejectNullRatherThanInventingZero() throws Exception {
        Field field = vectorField(true, false, false, false);
        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(field)), allocator)) {
            root.allocateNew();
            fillVector(root.getVector(0), true, true);
            root.setRowCount(2);
            assertThatThrownBy(() -> ArrowBatchDecoder.decode(writeRoot(root)))
                    .isInstanceOf(TransportException.class).hasMessageContaining("required Double VECTOR coordinate is null");
            root.getVector(0).setNull(0);
            assertThatThrownBy(() -> ArrowBatchDecoder.decode(writeRoot(root)))
                    .isInstanceOf(TransportException.class).hasMessageContaining("required VECTOR row");
        }
    }

    @Test
    void descriptorsRejectDetachedOrInconsistentLogicalDeclarations() {
        var logical = VectorLogicalType.create(3, VectorLogicalType.FLOAT64, false, true);
        assertThatThrownBy(() -> new ColumnDescriptor("v", Types.ARRAY, "VECTOR(3,DOUBLE)", 3, 0,
                java.util.Optional.empty())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ColumnDescriptor("v", Types.ARRAY, "VECTOR(3)", 3, 0,
                java.util.Optional.of(logical))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ColumnDescriptor("v", Types.ARRAY, "VECTOR(3,DOUBLE)", 2, 0,
                java.util.Optional.of(logical))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ColumnDescriptor("v", Types.DOUBLE, "VECTOR(3,DOUBLE)", 3, 0,
                java.util.Optional.of(logical))).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    void slicedBuffersRespectActualRowOffsetsAndDecodedValuesOutliveTheBuffers(boolean fixed) {
        Field field = vectorField(true, fixed, true, true);
        Double[] decoded;
        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(field)), allocator)) {
            root.allocateNew();
            fillVector(root.getVector(0), true, true);
            root.setRowCount(2);
            var transfer = root.getVector(0).getTransferPair(allocator);
            try (FieldVector slice = (FieldVector) transfer.getTo()) {
                transfer.splitAndTransfer(1, 1);
                decoded = (Double[]) ArrowBatchDecoder.decodeVectorField(slice, 0);
            }
        }
        assertThat(decoded).containsExactly(1.0000000000000002, -123456789.12345679, 3.5);
    }

    private static Field vectorField(boolean doublePrecision, boolean fixed,
                                     boolean parentNullable, boolean childNullable) {
        Field child = new Field("item", new FieldType(childNullable, new ArrowType.FloatingPoint(
                doublePrecision ? FloatingPointPrecision.DOUBLE : FloatingPointPrecision.SINGLE), null), null);
        return new Field("embedding", new FieldType(parentNullable,
                fixed ? new ArrowType.FixedSizeList(3) : ArrowType.List.INSTANCE, null,
                Map.of("ARROW:extension:name", "pangrova.vector", "ARROW:extension:metadata", "3")), List.of(child));
    }

    private static void fillVector(FieldVector vector, boolean doublePrecision, boolean withNull) {
        FieldVector values = vector instanceof ListVector list ? list.getDataVector()
                : ((FixedSizeListVector) vector).getDataVector();
        for (int row = 0; row < 2; row++) {
            if (vector instanceof ListVector list) list.startNewValue(row);
            else ((FixedSizeListVector) vector).setNotNull(row);
            int offset = row * 3;
            if (doublePrecision) {
                ((Float8Vector) values).setSafe(offset, 1.0000000000000002);
                ((Float8Vector) values).setSafe(offset + 1, -123456789.12345679);
                ((Float8Vector) values).setSafe(offset + 2, 3.5);
            } else {
                ((Float4Vector) values).setSafe(offset, 0.25f);
                ((Float4Vector) values).setSafe(offset + 1, -0.5f);
                ((Float4Vector) values).setSafe(offset + 2, 3.5f);
            }
            if (withNull && row == 0) values.setNull(offset + 1);
            if (vector instanceof ListVector list) list.endValue(row, 3);
        }
    }

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
    void storageVectorPreservesIeeeValuesWithoutApplyingAiInputConstraints() throws Exception {
        byte[] ipc = listBatch("2", new float[] {Float.NaN, -0.5f}, false);

        assertThat((float[]) ArrowBatchDecoder.decode(ipc).rows().get(0)[0])
                .containsExactly(Float.NaN, -0.5f);
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
                .hasMessageContaining("List<Float32|Float64> storage");
    }

    @Test
    void doubleVectorPreservesPrecisionAndOrdinaryArrayIdentity() throws Exception {
        double[] coordinates = {1.0000000000000002, -123456789.12345679};
        Field child = new Field("item", FieldType.notNullable(new ArrowType.FloatingPoint(
                FloatingPointPrecision.DOUBLE)), null);
        Field vector = new Field("embedding", new FieldType(true, ArrowType.List.INSTANCE, null,
                Map.of("ARROW:extension:name", "pangrova.vector",
                        "ARROW:extension:metadata", "2")), List.of(child));
        Field ordinary = new Field("ordinary", FieldType.nullable(ArrowType.List.INSTANCE), List.of(child));
        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(vector, ordinary)), allocator)) {
            root.allocateNew();
            for (int col = 0; col < 2; col++) {
                ListVector list = (ListVector) root.getVector(col);
                list.startNewValue(0);
                Float8Vector values = (Float8Vector) list.getDataVector();
                values.setSafe(0, coordinates[0]);
                values.setSafe(1, coordinates[1]);
                list.endValue(0, 2);
            }
            root.setRowCount(1);
            ResultBatch result = ArrowBatchDecoder.decode(writeRoot(root));
            assertThat(result.columns().get(0).typeName()).isEqualTo("VECTOR(2,DOUBLE)");
            assertThat(result.rows().get(0)[0]).isEqualTo(coordinates);
            assertThat(result.rows().get(0)[1]).isEqualTo(List.of(coordinates[0], coordinates[1]));
            assertThat(coordinates[0]).isNotEqualTo((double) (float) coordinates[0]);
        }
    }

    @Test
    void storageVectorRejectsDimensionBeyondCanonicalMaximumEvenForEmptyBatch() throws Exception {
        Field vector = new Field("embedding", new FieldType(true, ArrowType.List.INSTANCE, null,
                Map.of("ARROW:extension:name", "pangrova.vector",
                        "ARROW:extension:metadata", "65536")), List.of(FLOAT_CHILD));
        byte[] ipc = emptyBatch(new Schema(List.of(vector)));
        assertThatThrownBy(() -> ArrowBatchDecoder.decode(ipc))
                .isInstanceOf(TransportException.class).hasMessageContaining("maximum 65535");
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
