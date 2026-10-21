package org.pangrova.transport;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.sql.Types;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FixedSizeBinaryVector;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ArrowBatchDecoderUuidTest {
    // Produce standard extension metadata over physical bytes, without using
    // the consumer's UUID vector or its extension registration to write them.
    private static Field uuid(String name, int width) {
        return new Field(name, new FieldType(true, new ArrowType.FixedSizeBinary(width), null,
                Map.of("ARROW:extension:name", "arrow.uuid", "ARROW:extension:metadata", "")), null);
    }

    @Test
    void alreadyMaterializedPhysicalUuidFieldsDecodeWithoutDependingOnRegistrationOrder() {
        UUID expected = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
        byte[] bytes = ByteBuffer.allocate(16).putLong(expected.getMostSignificantBits())
                .putLong(expected.getLeastSignificantBits()).array();
        Field id = uuid("id", 16);
        Field row = new Field("row", FieldType.nullable(new ArrowType.Struct()), List.of(id));
        Field list = new Field("list", FieldType.nullable(new ArrowType.List()), List.of(id));
        assertThat(ArrowBatchDecoder.normalizeComplexValue(bytes, id)).isEqualTo(expected);
        assertThat(ArrowBatchDecoder.normalizeComplexValue(Map.of("id", bytes), row))
                .isEqualTo(Map.of("id", expected));
        assertThat(ArrowBatchDecoder.normalizeComplexValue(java.util.Arrays.asList(bytes, null), list))
                .isEqualTo(java.util.Arrays.asList(expected, null));
        Field plain = new Field("plain", FieldType.nullable(new ArrowType.FixedSizeBinary(16)), null);
        assertThat(ArrowBatchDecoder.normalizeComplexValue(bytes, plain)).isSameAs(bytes);
    }

    @ParameterizedTest
    @ValueSource(ints = {15, 17})
    void invalidPhysicalUuidDeclarationsFailEvenWhenTheValueIsNull(int width) {
        assertThatThrownBy(() -> ArrowBatchDecoder.normalizeComplexValue(null, uuid("id", width)))
                .isInstanceOf(TransportException.class).hasMessageContaining("UUID");
    }

    @Test
    void standardUuidBytesPreserveIdentityInsideEveryNestedShape() throws Exception {
        Field id = uuid("id", 16);
        Field plain = new Field("plain", FieldType.nullable(new ArrowType.FixedSizeBinary(16)), null);
        Field nested = new Field("nested", FieldType.nullable(new ArrowType.Struct()), List.of(uuid("value", 16)));
        Field items = new Field("items", FieldType.nullable(new ArrowType.List()), List.of(uuid("item", 16)));
        Field entries = new Field("entries", FieldType.notNullable(new ArrowType.Struct()), List.of(
                new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null), uuid("value", 16)));
        Field mapping = new Field("mapping", FieldType.nullable(new ArrowType.Map(false)), List.of(entries));
        UUID[] values = {new UUID(0, 0), new UUID(-1, -1),
                UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"), null};
        byte[] encoded;
        try (var allocator = new RootAllocator();
                var root = VectorSchemaRoot.create(new Schema(List.of(id, plain, nested, items, mapping)), allocator)) {
            root.allocateNew();
            var struct = (StructVector) root.getVector("nested");
            var list = (ListVector) root.getVector("items");
            var map = (MapVector) root.getVector("mapping");
            var pairs = (StructVector) map.getDataVector();
            for (int i = 0; i < values.length; i++) {
                put((FixedSizeBinaryVector) root.getVector("id"), i, values[i]);
                put((FixedSizeBinaryVector) root.getVector("plain"), i, values[i]);
                struct.setIndexDefined(i);
                put(struct.getChild("value", FixedSizeBinaryVector.class), i, values[i]);
                int item = list.startNewValue(i);
                put((FixedSizeBinaryVector) list.getDataVector(), item, values[i]);
                list.endValue(i, 1);
                int entry = map.startNewValue(i);
                pairs.setIndexDefined(entry);
                pairs.getChild("key", VarCharVector.class).setSafe(entry, new byte[] {'k'});
                put(pairs.getChild("value", FixedSizeBinaryVector.class), entry, values[i]);
                map.endValue(i, 1);
            }
            struct.setNull(2);
            root.setRowCount(values.length);
            encoded = encode(root);
        }
        var result = ArrowBatchDecoder.decode(encoded);
        assertThat(result.columns().get(0).sqlType()).isEqualTo(Types.OTHER);
        assertThat(result.columns().get(0).typeName()).isEqualTo("UUID");
        assertThat(result.columns().get(0).precision()).isZero();
        for (int i = 0; i < values.length; i++) {
            Object[] row = result.rows().get(i);
            assertThat(row[0]).isEqualTo(values[i]);
            var expected = new LinkedHashMap<String, Object>();
            expected.put("value", values[i]);
            assertThat(row[2]).isEqualTo(i == 2 ? null : expected);
            assertThat(row[3]).isEqualTo(Collections.singletonList(values[i]));
            expected = new LinkedHashMap<>();
            expected.put("k", values[i]);
            assertThat(row[4]).isEqualTo(expected);
            if (values[i] != null) assertThat((byte[]) row[1]).containsExactly(bytes(values[i]));
            else assertThat(row[1]).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {15, 17})
    void invalidUuidStorageWidthFailsBeforeReturningRows(int width) throws Exception {
        byte[] encoded;
        try (var allocator = new RootAllocator();
                var root = VectorSchemaRoot.create(new Schema(List.of(uuid("id", width))), allocator)) {
            root.allocateNew();
            root.setRowCount(0);
            encoded = encode(root);
        }
        assertThatThrownBy(() -> ArrowBatchDecoder.decode(encoded))
                .isInstanceOf(TransportException.class).hasMessageContaining("UuidType");
    }

    private static void put(FixedSizeBinaryVector vector, int row, UUID value) {
        if (value == null) vector.setNull(row);
        else vector.setSafe(row, bytes(value));
    }

    private static byte[] bytes(UUID value) {
        return ByteBuffer.allocate(16).putLong(value.getMostSignificantBits()).putLong(value.getLeastSignificantBits()).array();
    }

    private static byte[] encode(VectorSchemaRoot root) throws Exception {
        var output = new ByteArrayOutputStream();
        try (var writer = new ArrowStreamWriter(root, null, Channels.newChannel(output))) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        return output.toByteArray();
    }
}
