package org.pangrova.spi.property.connector.data;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.page.ColumnarPage;
import org.pangrova.spi.connector.data.page.RowPageConverter;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for columnar Page round-trip via RowPageConverter.
 *
 * <p><b>Validates: Requirements 4.1, 4.2, 4.3, 4.6</b></p>
 */
class ColumnarPageRoundTripPropertyTest {

    /**
     * Property 5: Columnar Page Round-Trip
     *
     * For any row-based Page(columns, rows), converting to ColumnarPage
     * via RowPageConverter.fromLegacy() and back via toLegacy() should
     * produce identical data.
     *
     * <p><b>Validates: Requirements 4.2, 4.6</b></p>
     */
    @Property(tries = 10)
    void columnarPageRoundTripPreservesData(
            @ForAll("rowBasedPages") RowBasedPage input) {

        ColumnarPage columnar = RowPageConverter.fromLegacy(
                input.columns(), input.rows());

        List<TypedRow> restored = RowPageConverter.toLegacy(
                columnar, input.columns());

        assertThat(restored)
                .as("Round-trip should preserve row count")
                .hasSize(input.rows().size());

        for (int i = 0; i < input.rows().size(); i++) {
            TypedRow original = input.rows().get(i);
            TypedRow roundTripped = restored.get(i);
            for (String col : input.columns()) {
                assertThat(roundTripped.get(col))
                        .as("Row %d, column '%s'", i, col)
                        .isEqualTo(original.get(col));
            }
        }
    }

    /**
     * ColumnarPage channel count matches column count.
     */
    @Property(tries = 10)
    void columnarPageChannelCountMatchesColumns(
            @ForAll("rowBasedPages") RowBasedPage input) {

        ColumnarPage columnar = RowPageConverter.fromLegacy(
                input.columns(), input.rows());

        assertThat(columnar.getChannelCount())
                .isEqualTo(input.columns().size());
    }

    /**
     * ColumnarPage position count matches row count.
     */
    @Property(tries = 10)
    void columnarPagePositionCountMatchesRows(
            @ForAll("rowBasedPages") RowBasedPage input) {

        ColumnarPage columnar = RowPageConverter.fromLegacy(
                input.columns(), input.rows());

        assertThat(columnar.getPositionCount())
                .isEqualTo(input.rows().size());
    }

    /**
     * Each block in the ColumnarPage has the correct position count.
     */
    @Property(tries = 10)
    void eachBlockHasCorrectPositionCount(
            @ForAll("rowBasedPages") RowBasedPage input) {

        ColumnarPage columnar = RowPageConverter.fromLegacy(
                input.columns(), input.rows());

        for (int ch = 0; ch < columnar.getChannelCount(); ch++) {
            assertThat(columnar.getBlock(ch).getPositionCount())
                    .as("Block at channel %d", ch)
                    .isEqualTo(input.rows().size());
        }
    }

    // ---- Data holder for generated test input ----

    record RowBasedPage(List<String> columns, List<TypedRow> rows) {}

    // ---- Arbitrary generators ----

    @Provide
    Arbitrary<RowBasedPage> rowBasedPages() {
        Arbitrary<Integer> colCount = Arbitraries.integers().between(1, 5);
        Arbitrary<Integer> rowCount = Arbitraries.integers().between(1, 20);

        return Combinators.combine(colCount, rowCount).flatAs((cols, rows) -> {
            List<String> columns = IntStream.rangeClosed(1, cols)
                    .mapToObj(i -> "col_" + i)
                    .toList();

            return columnValues()
                    .list().ofSize(rows * cols)
                    .map(flatValues -> {
                        List<TypedRow> typedRows = new ArrayList<>();
                        for (int r = 0; r < rows; r++) {
                            TypedRow.Builder builder = TypedRow.builder();
                            for (int c = 0; c < cols; c++) {
                                builder.put(columns.get(c),
                                        flatValues.get(r * cols + c));
                            }
                            typedRows.add(builder.build());
                        }
                        return new RowBasedPage(columns, typedRows);
                    });
        });
    }

    private Arbitrary<ColumnValue> columnValues() {
        return Arbitraries.oneOf(
                Arbitraries.integers().map(ColumnValue.IntValue::new),
                Arbitraries.longs().map(ColumnValue.LongValue::new),
                Arbitraries.doubles()
                        .filter(d -> !Double.isNaN(d) && Double.isFinite(d))
                        .map(ColumnValue.DoubleValue::new),
                Arbitraries.of(true, false).map(ColumnValue.BooleanValue::new),
                Arbitraries.strings().alpha().ofMinLength(0).ofMaxLength(20)
                        .map(ColumnValue.StringValue::new),
                Arbitraries.just(new ColumnValue.NullValue())
        );
    }
}
