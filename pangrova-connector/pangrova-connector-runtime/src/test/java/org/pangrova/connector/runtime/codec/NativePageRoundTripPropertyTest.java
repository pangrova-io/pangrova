package org.pangrova.connector.runtime.codec;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.pangrova.types.value.ColumnValue;
import org.pangrova.types.value.TypedRow;
import org.pangrova.batch.api.page.NativePage;
import org.pangrova.connector.runtime.codec.ConnectorBinaryPageCodec;
import org.pangrova.batch.api.page.Page;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for NativePage serialization round-trip via ConnectorBinaryPageCodec.
 *
 * <p>Verifies that converting Page → NativePage → Page preserves row count,
 * column names, and cell values exactly for all types supported by the
 * CUSTOM_BINARY serialization format.</p>
 *
 * <p><b>Validates: Requirements 1.5, 10.4</b></p>
 */
class NativePageRoundTripPropertyTest {

    /**
     * Property 1: NativePage serialization round-trip.
     *
     * <p>For any valid Page (with arbitrary row count, column names and typed values),
     * round-tripping through {@code ConnectorBinaryPageCodec.fromPage()} →
     * {@code ConnectorBinaryPageCodec.toPage()} preserves row count, column names
     * and values exactly.</p>
     *
     * <p><b>Validates: Requirements 1.5, 10.4</b></p>
     */
    @Property(tries = 100)
    void roundTripPreservesRowCountColumnNamesAndValues(
            @ForAll("pages") Page original) {

        NativePage nativePage = ConnectorBinaryPageCodec.fromPage(original);
        Page restored = ConnectorBinaryPageCodec.toPage(nativePage);

        assertThat(restored.getPositionCount())
                .as("Row count must be preserved")
                .isEqualTo(original.getPositionCount());

        assertThat(restored.columns())
                .as("Column names must be preserved in order")
                .isEqualTo(original.columns());

        for (int r = 0; r < original.rows().size(); r++) {
            TypedRow originalRow = original.rows().get(r);
            TypedRow restoredRow = restored.rows().get(r);
            for (String col : original.columns()) {
                assertThat(restoredRow.get(col))
                        .as("Row %d, column '%s'", r, col)
                        .isEqualTo(originalRow.get(col));
            }
        }
    }

    /**
     * Round-trip preserves NativePage metadata (rowCount, columnCount).
     *
     * <p><b>Validates: Requirements 1.5, 10.4</b></p>
     */
    @Property(tries = 100)
    void roundTripPreservesNativePageMetadata(
            @ForAll("pages") Page original) {

        NativePage nativePage = ConnectorBinaryPageCodec.fromPage(original);

        assertThat(nativePage.getRowCount())
                .as("NativePage rowCount must match original")
                .isEqualTo(original.getPositionCount());

        assertThat(nativePage.getColumnCount())
                .as("NativePage columnCount must match original")
                .isEqualTo(original.columns().size());
    }

    // ---- Arbitrary generators ----

    @Provide
    Arbitrary<Page> pages() {
        Arbitrary<Integer> colCount = Arbitraries.integers().between(0, 6);
        Arbitrary<Integer> rowCount = Arbitraries.integers().between(0, 20);

        return Combinators.combine(colCount, rowCount).flatAs((cols, rows) -> {
            if (cols == 0) {
                return Arbitraries.just(new Page(List.of(), List.of()));
            }

            List<String> columns = IntStream.rangeClosed(1, cols)
                    .mapToObj(i -> "col_" + i)
                    .toList();

            return columnValues()
                    .list().ofSize(rows * cols)
                    .map(flatValues -> buildPage(columns, rows, flatValues));
        });
    }

    private static Page buildPage(List<String> columns, int rowCount,
                                  List<ColumnValue> flatValues) {
        int cols = columns.size();
        List<TypedRow> typedRows = new ArrayList<>(rowCount);
        for (int r = 0; r < rowCount; r++) {
            TypedRow.Builder builder = TypedRow.builder();
            for (int c = 0; c < cols; c++) {
                builder.put(columns.get(c), flatValues.get(r * cols + c));
            }
            typedRows.add(builder.build());
        }
        return new Page(columns, typedRows);
    }

    /**
     * Generates ColumnValue instances for the types natively supported by
     * ConnectorBinaryPageCodec's CUSTOM_BINARY format: null, String, int, long,
     * double, and boolean.
     */
    private Arbitrary<ColumnValue> columnValues() {
        return Arbitraries.oneOf(
                Arbitraries.just(new ColumnValue.NullValue()),
                Arbitraries.strings().ofMinLength(0).ofMaxLength(50)
                        .map(ColumnValue.StringValue::new),
                Arbitraries.integers().map(ColumnValue.IntValue::new),
                Arbitraries.longs().map(ColumnValue.LongValue::new),
                Arbitraries.doubles()
                        .filter(d -> !Double.isNaN(d) && Double.isFinite(d))
                        .map(ColumnValue.DoubleValue::new),
                Arbitraries.of(true, false).map(ColumnValue.BooleanValue::new)
        );
    }
}
