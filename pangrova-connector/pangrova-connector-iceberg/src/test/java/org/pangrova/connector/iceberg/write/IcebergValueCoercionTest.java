package org.pangrova.connector.iceberg.write;

import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks framework representations for detached columnar values, including nested nulls. */
class IcebergValueCoercionTest {

    @Test
    void binaryBytesBecomeBufferWhileFixedBytesRemainArray() {
        byte[] bytes = {0, 1, -1};

        assertThat(IcebergValueCoercion.coerceValue(bytes, Types.BinaryType.get()))
                .isEqualTo(ByteBuffer.wrap(bytes));
        assertThat(IcebergValueCoercion.coerceValue(bytes, Types.FixedType.ofLength(3))).isSameAs(bytes);
        assertThat(IcebergValueCoercion.coerceValue(null, Types.BinaryType.get())).isNull();
    }

    @Test
    void nestedListsOfStructsCoerceValuesAndPreserveNullElements() {
        Types.StructType item = Types.StructType.of(
                Types.NestedField.optional(2, "quantity", Types.LongType.get()),
                Types.NestedField.optional(3, "label", Types.StringType.get()),
                Types.NestedField.optional(4, "payload", Types.BinaryType.get()));
        Types.ListType type = Types.ListType.ofOptional(1, item);
        List<?> input = Arrays.asList(Map.of("quantity", -7, "label", "café", "payload", new byte[] {1}), null);

        List<?> result = (List<?>) IcebergValueCoercion.coerceValue(input, type);

        assertThat(result).hasSize(2);
        GenericRecord first = (GenericRecord) result.get(0);
        assertThat(first.getField("quantity")).isEqualTo(-7L);
        assertThat(first.getField("label")).isEqualTo("café");
        assertThat(first.getField("payload")).isEqualTo(ByteBuffer.wrap(new byte[] {1}));
        assertThat(result.get(1)).isNull();
        assertThat((List<?>) IcebergValueCoercion.coerceValue(List.of(), type)).isEmpty();
    }

    @Test
    void mapKeysAndNestedDecimalValuesAreCoercedWithoutLosingNulls() {
        Types.MapType type = Types.MapType.ofOptional(1, 2, Types.LongType.get(),
                Types.ListType.ofOptional(3, Types.DecimalType.of(8, 2)));
        Map<Integer, Object> input = new LinkedHashMap<>();
        input.put(-7, Arrays.asList(new BigDecimal("1.2"), null));
        input.put(9, null);

        Map<?, ?> result = (Map<?, ?>) IcebergValueCoercion.coerceValue(input, type);

        assertThat(result).hasSize(2);
        assertThat(result.containsKey(-7L)).isTrue();
        List<?> amounts = (List<?>) result.get(-7L);
        assertThat(amounts.get(0)).isEqualTo(new BigDecimal("1.20"));
        assertThat(amounts.get(1)).isNull();
        assertThat(result.containsKey(9L)).isTrue();
        assertThat(result.get(9L)).isNull();
        assertThat((Map<?, ?>) IcebergValueCoercion.coerceValue(Map.of(), type)).isEmpty();
    }

    @Test
    void namedAndPositionalStructsTreatAbsentFieldsAsNull() {
        Types.StructType type = Types.StructType.of(
                Types.NestedField.optional(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "label", Types.StringType.get()));

        for (Object input : List.of(Map.of("id", -7), List.of(-7))) {
            GenericRecord record = (GenericRecord) IcebergValueCoercion.coerceValue(input, type);
            assertThat(record.getField("id")).isEqualTo(-7L);
            assertThat(record.getField("label")).isNull();
        }
    }
}