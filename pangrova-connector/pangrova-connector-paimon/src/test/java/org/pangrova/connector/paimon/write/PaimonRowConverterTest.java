package org.pangrova.connector.paimon.write;

import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.connector.data.TypedRow;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
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
import java.time.LocalTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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
}
