package org.pangrova.batch.arrow;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchValuesUuidTest {
    private static final UUID ID = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
    private static final byte[] WIRE = HexFormat.of().parseHex("00112233445566778899aabbccddeeff");

    @Test
    void coldPhysicalFieldsRetainDeclaredUuidIdentityWithoutRegistryInitialization() {
        assertThat(BatchValues.detach(uuid(16), WIRE)).isEqualTo(ID);
        assertThat(BatchValues.detach(uuid(16), null)).isNull();
        assertThat(BatchValues.detach(uuid(16), ID)).isSameAs(ID);
        ByteBuffer bytes = ByteBuffer.wrap(WIRE).order(ByteOrder.LITTLE_ENDIAN);
        assertThat(BatchValues.detach(uuid(16), bytes)).isEqualTo(ID);
        assertThat(bytes.position()).isZero();
        assertThat(bytes.order()).isEqualTo(ByteOrder.LITTLE_ENDIAN);
    }

    @Test
    void nestedFieldsUseTheirOwnUuidDeclaration() {
        Field id = uuid(16);
        Field items = new Field("items", FieldType.nullable(ArrowType.List.INSTANCE), List.of(id));
        Field record = new Field("record", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(id, items));
        assertThat(BatchValues.detach(record, Map.of("id", WIRE, "items", List.of(WIRE))))
                .isEqualTo(Map.of("id", ID, "items", List.of(ID)));
    }

    @Test
    void plainFixedBinaryDoesNotAcquireUuidIdentity() {
        Field field = new Field("binary", FieldType.nullable(new ArrowType.FixedSizeBinary(16)), List.of());
        Object value = BatchValues.detach(field, WIRE);
        assertThat(value).isInstanceOf(byte[].class).isNotSameAs(WIRE);
        assertThat((byte[]) value).containsExactly(WIRE);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 15, 17})
    void malformedPayloadsCannotBeTruncated(int width) {
        assertThatThrownBy(() -> BatchValues.detach(uuid(16), new byte[width]))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("exactly 16");
    }

    @ParameterizedTest
    @ValueSource(ints = {15, 17})
    void malformedStorageDeclarationsFailEvenForNullValues(int width) {
        assertThatThrownBy(() -> BatchValues.detach(uuid(width), null))
                .isInstanceOfAny(IllegalArgumentException.class, UnsupportedOperationException.class);
    }

    private static Field uuid(int width) {
        return new Field("id", new FieldType(true, new ArrowType.FixedSizeBinary(width), null,
                Map.of("ARROW:extension:name", "arrow.uuid", "ARROW:extension:metadata", "")), List.of());
    }
}
