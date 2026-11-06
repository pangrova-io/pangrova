package org.pangrova.testing.integration.e2e.generator;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Provide;

import org.pangrova.types.value.ColumnValue;
import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.testkit.TestDataGenerator;
import org.pangrova.testing.testkit.TestDataGenerator.ColumnType;
import org.pangrova.testing.testkit.TestDataGenerator.TableSchema;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * jqwik generators for random row data used in write-path property testing.
 *
 * <p>Provides {@link Arbitrary} instances for generating random {@link TypedRow}
 * and {@link ColumnValue} objects matching TPC-H table schemas.
 */
public final class DataGenerators {

    private static final List<String> SAMPLE_DATES =
            List.of("1992-01-01", "1995-06-15", "1998-12-31");

    private DataGenerators() {}

    /**
     * Generate a random {@link TypedRow} matching a given table schema.
     *
     * @param schema the table schema defining columns; must not be null
     * @return arbitrary producing random typed rows
     */
    @Provide
    public static Arbitrary<TypedRow> rowForSchema(TableSchema schema) {
        Objects.requireNonNull(schema, "schema must not be null");
        List<TestDataGenerator.ColumnDef> columns = schema.columns();
        if (columns.isEmpty()) {
            return Arbitraries.just(TypedRow.of(Map.of()));
        }
        return buildRowArbitrary(columns, 0, new LinkedHashMap<>());
    }

    /**
     * Generate a random {@link ColumnValue} for the given column type.
     *
     * @param type the column type to generate a value for; must not be null
     * @return arbitrary producing random column values
     */
    @Provide
    public static Arbitrary<ColumnValue> valueForType(ColumnType type) {
        Objects.requireNonNull(type, "type must not be null");
        return switch (type) {
            case STRING -> Arbitraries.strings().alpha()
                    .ofMinLength(1).ofMaxLength(50)
                    .map(s -> (ColumnValue) new ColumnValue.StringValue(s));
            case LONG -> Arbitraries.longs()
                    .between(1, 1_000_000)
                    .map(l -> (ColumnValue) new ColumnValue.LongValue(l));
            case DOUBLE -> Arbitraries.doubles()
                    .between(0.01, 99999.99)
                    .map(d -> (ColumnValue) new ColumnValue.DoubleValue(d));
            case DATE -> Arbitraries.of(SAMPLE_DATES)
                    .map(s -> (ColumnValue) new ColumnValue.StringValue(s));
            case BOOLEAN -> Arbitraries.of(true, false)
                    .map(b -> (ColumnValue) new ColumnValue.BooleanValue(b));
        };
    }

    /**
     * Recursively build a TypedRow Arbitrary by chaining flatMap for each column.
     */
    private static Arbitrary<TypedRow> buildRowArbitrary(
            List<TestDataGenerator.ColumnDef> columns,
            int index,
            Map<String, ColumnValue> accumulated) {
        if (index >= columns.size()) {
            return Arbitraries.just(TypedRow.of(new LinkedHashMap<>(accumulated)));
        }
        TestDataGenerator.ColumnDef col = columns.get(index);
        return valueForType(col.type()).flatMap(value -> {
            Map<String, ColumnValue> next = new LinkedHashMap<>(accumulated);
            next.put(col.name(), value);
            return buildRowArbitrary(columns, index + 1, next);
        });
    }
}
