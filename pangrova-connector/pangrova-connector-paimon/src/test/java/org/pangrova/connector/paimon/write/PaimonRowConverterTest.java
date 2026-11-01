package org.pangrova.connector.paimon.write;

import org.pangrova.spi.connector.data.value.ColumnValue;
import org.pangrova.spi.connector.data.value.TypedRow;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for {@link PaimonRowConverter} type coercion, covering
 * DECIMAL, TINYINT, SMALLINT, TIME, and BINARY which previously fell through
 * the {@code default -> value} branch and produced type mismatches.
 */
class PaimonRowConverterTest {

    @Test
    void convert_decimal_producesPaimonDecimalWithFieldPrecisionAndScale() {
        RowType rowType = new RowType(List.of(
                new DataField(0, "amount", new DecimalType(10, 2))));
        PaimonRowConverter converter = new PaimonRowConverter(rowType);

        TypedRow row = TypedRow.of(java.util.Map.of(
                "amount", new ColumnValue.DecimalValue(new BigDecimal("123.45"))));

        GenericRow result = converter.convert(row);
        Object field = result.getField(0);

        assertThat(field).isInstanceOf(Decimal.class);
        Decimal decimal = (Decimal) field;
        assertThat(decimal.precision()).isEqualTo(10);
        assertThat(decimal.scale()).isEqualTo(2);
        assertThat(decimal.toBigDecimal()).isEqualByComparingTo(new BigDecimal("123.45"));
    }

    @Test
    void convert_tinyInt_producesByte() {
        RowType rowType = new RowType(List.of(
                new DataField(0, "t", new TinyIntType())));
        PaimonRowConverter converter = new PaimonRowConverter(rowType);

        TypedRow row = TypedRow.of(java.util.Map.of("t", new ColumnValue.IntValue(42)));

        Object field = converter.convert(row).getField(0);
        assertThat(field).isInstanceOf(Byte.class).isEqualTo((byte) 42);
    }

    @Test
    void convert_smallInt_producesShort() {
        RowType rowType = new RowType(List.of(
                new DataField(0, "s", new SmallIntType())));
        PaimonRowConverter converter = new PaimonRowConverter(rowType);

        TypedRow row = TypedRow.of(java.util.Map.of("s", new ColumnValue.IntValue(1000)));

        Object field = converter.convert(row).getField(0);
        assertThat(field).isInstanceOf(Short.class).isEqualTo((short) 1000);
    }

    @Test
    void convert_time_fromMillisInteger_producesIntMillisOfDay() {
        RowType rowType = new RowType(List.of(
                new DataField(0, "tm", new TimeType())));
        PaimonRowConverter converter = new PaimonRowConverter(rowType);

        int millis = 13 * 3600 * 1000 + 45 * 60 * 1000; // 13:45
        TypedRow row = TypedRow.of(java.util.Map.of("tm", new ColumnValue.IntValue(millis)));

        Object field = converter.convert(row).getField(0);
        assertThat(field).isInstanceOf(Integer.class).isEqualTo(millis);
    }

    @Test
    void convert_time_fromStringTime_producesIntMillisOfDay() {
        RowType rowType = new RowType(List.of(
                new DataField(0, "tm", new TimeType())));
        PaimonRowConverter converter = new PaimonRowConverter(rowType);

        LocalTime time = LocalTime.of(13, 45, 30);
        int expectedMillis = (int) (time.toNanoOfDay() / 1_000_000);
        TypedRow row = TypedRow.of(java.util.Map.of(
                "tm", new ColumnValue.StringValue(time.toString())));

        Object field = converter.convert(row).getField(0);
        assertThat(field).isInstanceOf(Integer.class).isEqualTo(expectedMillis);
    }

    @Test
    void convert_binary_producesByteArray() {
        RowType rowType = new RowType(List.of(
                new DataField(0, "b", new VarBinaryType(VarBinaryType.MAX_LENGTH))));
        PaimonRowConverter converter = new PaimonRowConverter(rowType);

        byte[] bytes = {1, 2, 3, 4};
        TypedRow row = TypedRow.of(java.util.Map.of("b", new ColumnValue.BytesValue(bytes)));

        Object field = converter.convert(row).getField(0);
        assertThat(field).isInstanceOf(byte[].class);
        assertThat((byte[]) field).containsExactly(1, 2, 3, 4);
    }

    @Test
    void convert_nullValues_remainNull() {
        RowType rowType = new RowType(List.of(
                new DataField(0, "amount", new DecimalType(10, 2)),
                new DataField(1, "n", new IntType())));
        PaimonRowConverter converter = new PaimonRowConverter(rowType);

        TypedRow row = TypedRow.of(java.util.Map.of(
                "amount", new ColumnValue.NullValue(),
                "n", new ColumnValue.NullValue()));

        GenericRow result = converter.convert(row);
        assertThat(result.getField(0)).isNull();
        assertThat(result.getField(1)).isNull();
    }

    @Test
    void convert_nestedArraysMapsAndRows_preservesNullsAndTypedKeys() {
        RowType itemType = DataTypes.ROW(DataTypes.FIELD(0, "label", DataTypes.STRING()),
                DataTypes.FIELD(1, "quantity", DataTypes.BIGINT()));
        RowType nestedType = DataTypes.ROW(
                DataTypes.FIELD(0, "items", DataTypes.ARRAY(itemType)),
                DataTypes.FIELD(1, "amounts", DataTypes.MAP(DataTypes.BIGINT(),
                        DataTypes.ARRAY(DataTypes.DECIMAL(8, 2)))));
        PaimonRowConverter converter = new PaimonRowConverter(DataTypes.ROW(
                DataTypes.FIELD(0, "nested", nestedType)));
        Map<Integer, Object> amounts = new LinkedHashMap<>();
        amounts.put(-7, Arrays.asList(new BigDecimal("12.34"), null));
        amounts.put(9, null);
        TypedRow input = TypedRow.of(Map.of("nested", new ColumnValue.RowValue(List.of(
                Arrays.asList(Map.of("label", "café", "quantity", -3), null), amounts))));

        GenericRow nested = (GenericRow) converter.convert(input).getField(0);

        var items = nested.getArray(0);
        assertThat(items.size()).isEqualTo(2);
        assertThat(items.getRow(0, 2).getString(0).toString()).isEqualTo("café");
        assertThat(items.getRow(0, 2).getLong(1)).isEqualTo(-3L);
        assertThat(items.isNullAt(1)).isTrue();
        var convertedMap = nested.getMap(1);
        assertThat(convertedMap.keyArray().getLong(0)).isEqualTo(-7L);
        assertThat(convertedMap.keyArray().getLong(1)).isEqualTo(9L);
        var decimals = convertedMap.valueArray().getArray(0);
        assertThat(decimals.getDecimal(0, 8, 2).toBigDecimal()).isEqualTo(new BigDecimal("12.34"));
        assertThat(decimals.isNullAt(1)).isTrue();
        assertThat(convertedMap.valueArray().isNullAt(1)).isTrue();
    }

    @Test
    void convert_nestedTemporalAndBinaryValues_usesInternalRepresentations() {
        RowType nestedType = DataTypes.ROW(
                DataTypes.FIELD(0, "date", DataTypes.DATE()),
                DataTypes.FIELD(1, "time", DataTypes.TIME()),
                DataTypes.FIELD(2, "timestamp", DataTypes.TIMESTAMP()),
                DataTypes.FIELD(3, "binary", DataTypes.BYTES()));
        PaimonRowConverter converter = new PaimonRowConverter(DataTypes.ROW(
                DataTypes.FIELD(0, "nested", nestedType)));
        LocalDate date = LocalDate.of(1969, 12, 31);
        LocalTime time = LocalTime.of(23, 59, 59, 123_000_000);
        LocalDateTime timestamp = LocalDateTime.of(date, time);
        TypedRow input = TypedRow.of(Map.of("nested", new ColumnValue.RowValue(
                List.of(date, time, timestamp, new byte[] {0, -1}))));

        var nested = converter.convert(input).getRow(0, 4);

        assertThat(nested.getInt(0)).isEqualTo(-1);
        assertThat(nested.getInt(1)).isEqualTo(86_399_123);
        assertThat(nested.getTimestamp(2, 6).toLocalDateTime()).isEqualTo(timestamp);
        assertThat(nested.getBinary(3)).containsExactly(0, -1);
    }

    @Test
    void convert_namedAndPositionalRows_missingFieldsBecomeNull() {
        RowType nestedType = DataTypes.ROW(DataTypes.FIELD(0, "label", DataTypes.STRING()),
                DataTypes.FIELD(1, "quantity", DataTypes.BIGINT()));
        PaimonRowConverter converter = new PaimonRowConverter(DataTypes.ROW(
                DataTypes.FIELD(0, "nested", nestedType)));
        List<ColumnValue> values = List.of(new ColumnValue.MapValue(Map.of("label", "named")),
                new ColumnValue.RowValue(List.of("positional")));

        for (ColumnValue value : values) {
            var nested = converter.convert(TypedRow.of(Map.of("nested", value))).getRow(0, 2);
            assertThat(nested.getString(0).toString()).isIn("named", "positional");
            assertThat(nested.isNullAt(1)).isTrue();
        }
    }

    @Test
    void convert_emptyComplexContainers_remainEmpty() {
        RowType nestedType = DataTypes.ROW(
                DataTypes.FIELD(0, "items", DataTypes.ARRAY(DataTypes.STRING())),
                DataTypes.FIELD(1, "attributes", DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())));
        PaimonRowConverter converter = new PaimonRowConverter(DataTypes.ROW(
                DataTypes.FIELD(0, "nested", nestedType)));
        TypedRow input = TypedRow.of(Map.of("nested", new ColumnValue.RowValue(List.of(List.of(), Map.of()))));

        var nested = converter.convert(input).getRow(0, 2);

        assertThat(nested.getArray(0).size()).isZero();
        assertThat(nested.getMap(1).size()).isZero();
    }

    @Test
    void convert_decimalOverflow_doesNotSilentlyBecomeNull() {
        PaimonRowConverter converter = new PaimonRowConverter(DataTypes.ROW(
                DataTypes.FIELD(0, "amount", DataTypes.DECIMAL(3, 2))));
        TypedRow input = TypedRow.of(Map.of("amount", new ColumnValue.DecimalValue(new BigDecimal("10.00"))));

        assertThatThrownBy(() -> converter.convert(input)).isInstanceOf(ArithmeticException.class)
                .hasMessageContaining("precision");
    }
}
