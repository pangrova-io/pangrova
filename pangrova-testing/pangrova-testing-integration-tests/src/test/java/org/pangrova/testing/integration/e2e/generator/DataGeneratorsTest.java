package org.pangrova.testing.integration.e2e.generator;

import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import org.pangrova.spi.connector.data.value.ColumnValue;
import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.testkit.TestDataGenerator;
import org.pangrova.testing.testkit.TestDataGenerator.ColumnType;
import org.pangrova.testing.testkit.TestDataGenerator.TableSchema;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for DataGenerators.
 */
class DataGeneratorsTest {

    private static final TableSchema ORDERS_SCHEMA =
            TestDataGenerator.getSchemas().get("orders");

    @Property(tries = 50)
    void rowForSchemaContainsAllColumns(
            @ForAll("ordersRows") TypedRow row) {
        Set<String> expectedColumns = Set.of(
                "o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice",
                "o_orderdate", "o_orderpriority", "o_clerk",
                "o_shippriority", "o_comment");
        assertThat(row.columnNames()).isEqualTo(expectedColumns);
    }

    @Property(tries = 50)
    void rowForSchemaValuesAreNonNull(
            @ForAll("ordersRows") TypedRow row) {
        for (ColumnValue value : row.values().values()) {
            assertThat(value).isNotInstanceOf(ColumnValue.NullValue.class);
        }
    }

    @Property(tries = 100)
    void valueForStringTypeProducesStringValue(
            @ForAll("stringValues") ColumnValue value) {
        assertThat(value).isInstanceOf(ColumnValue.StringValue.class);
        String str = ((ColumnValue.StringValue) value).value();
        assertThat(str).hasSizeBetween(1, 50);
        assertThat(str).matches("[a-zA-Z]+");
    }

    @Property(tries = 100)
    void valueForLongTypeProducesLongValue(
            @ForAll("longValues") ColumnValue value) {
        assertThat(value).isInstanceOf(ColumnValue.LongValue.class);
        long l = ((ColumnValue.LongValue) value).value();
        assertThat(l).isBetween(1L, 1_000_000L);
    }

    @Property(tries = 100)
    void valueForDoubleTypeProducesDoubleValue(
            @ForAll("doubleValues") ColumnValue value) {
        assertThat(value).isInstanceOf(ColumnValue.DoubleValue.class);
        double d = ((ColumnValue.DoubleValue) value).value();
        assertThat(d).isBetween(0.01, 99999.99);
    }

    @Provide
    Arbitrary<TypedRow> ordersRows() {
        return DataGenerators.rowForSchema(ORDERS_SCHEMA);
    }

    @Provide
    Arbitrary<ColumnValue> stringValues() {
        return DataGenerators.valueForType(ColumnType.STRING);
    }

    @Provide
    Arbitrary<ColumnValue> longValues() {
        return DataGenerators.valueForType(ColumnType.LONG);
    }

    @Provide
    Arbitrary<ColumnValue> doubleValues() {
        return DataGenerators.valueForType(ColumnType.DOUBLE);
    }
}
