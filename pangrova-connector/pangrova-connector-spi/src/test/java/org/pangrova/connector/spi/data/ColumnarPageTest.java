package org.pangrova.connector.spi.data;

import org.pangrova.connector.spi.data.value.ColumnValue;
import org.pangrova.connector.spi.data.value.TypedRow;

import java.util.List;
import java.util.Map;

import org.pangrova.connector.spi.data.block.ArrayBlock;
import org.pangrova.connector.spi.data.block.Block;
import org.pangrova.connector.spi.data.page.ColumnarPage;
import org.pangrova.connector.spi.data.page.RowPageConverter;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for Block, ColumnarPage, and RowPageConverter.
 */
class ColumnarPageTest {

    @Test
    void blockBasicOperations() {
        ColumnValue[] values = {
                new ColumnValue.IntValue(10),
                new ColumnValue.NullValue(),
                new ColumnValue.IntValue(30)
        };
        ArrayBlock block = new ArrayBlock(values);

        assertThat(block.getPositionCount()).isEqualTo(3);
        assertThat(block.isNull(0)).isFalse();
        assertThat(block.isNull(1)).isTrue();
        assertThat(block.isNull(2)).isFalse();
        assertThat(block.getLong(0)).isEqualTo(10);
        assertThat(block.getLong(2)).isEqualTo(30);
    }

    @Test
    void blockGetRegion() {
        ColumnValue[] values = {
                new ColumnValue.LongValue(1L),
                new ColumnValue.LongValue(2L),
                new ColumnValue.LongValue(3L),
                new ColumnValue.LongValue(4L)
        };
        Block block = new ArrayBlock(values);
        Block region = block.getRegion(1, 2);

        assertThat(region.getPositionCount()).isEqualTo(2);
        assertThat(region.getLong(0)).isEqualTo(2L);
        assertThat(region.getLong(1)).isEqualTo(3L);
    }

    @Test
    void blockOutOfBoundsThrows() {
        Block block = new ArrayBlock(new ColumnValue[]{
                new ColumnValue.IntValue(1)
        });

        assertThatThrownBy(() -> block.getLong(1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> block.getLong(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void blockUnsupportedTypeThrows() {
        Block block = new ArrayBlock(new ColumnValue[]{
                new ColumnValue.StringValue("hello")
        });

        assertThatThrownBy(() -> block.getLong(0))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> block.getDouble(0))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void blockSliceForStringValues() {
        Block block = new ArrayBlock(new ColumnValue[]{
                new ColumnValue.StringValue("hello")
        });

        byte[] slice = block.getSlice(0);
        assertThat(new String(slice, java.nio.charset.StandardCharsets.UTF_8))
                .isEqualTo("hello");
    }

    @Test
    void columnarPageConstruction() {
        Block b1 = new ArrayBlock(new ColumnValue[]{
                new ColumnValue.IntValue(1),
                new ColumnValue.IntValue(2)
        });
        Block b2 = new ArrayBlock(new ColumnValue[]{
                new ColumnValue.StringValue("a"),
                new ColumnValue.StringValue("b")
        });

        ColumnarPage page = new ColumnarPage(2, b1, b2);

        assertThat(page.getPositionCount()).isEqualTo(2);
        assertThat(page.getChannelCount()).isEqualTo(2);
        assertThat(page.getBlock(0)).isSameAs(b1);
        assertThat(page.getBlock(1)).isSameAs(b2);
    }

    @Test
    void columnarPageRejectsNegativePositionCount() {
        Block block = new ArrayBlock(new ColumnValue[0]);
        assertThatThrownBy(() -> new ColumnarPage(-1, block))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void columnarPageRejectsEmptyBlocks() {
        assertThatThrownBy(() -> new ColumnarPage(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void columnarPageRejectsMismatchedPositionCount() {
        Block b1 = new ArrayBlock(new ColumnValue[]{
                new ColumnValue.IntValue(1)
        });
        Block b2 = new ArrayBlock(new ColumnValue[]{
                new ColumnValue.IntValue(1),
                new ColumnValue.IntValue(2)
        });

        assertThatThrownBy(() -> new ColumnarPage(1, b1, b2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void legacyAdapterRoundTrip() {
        List<String> columns = List.of("id", "name", "score");
        List<TypedRow> rows = List.of(
                TypedRow.of(Map.of(
                        "id", new ColumnValue.IntValue(1),
                        "name", new ColumnValue.StringValue("Alice"),
                        "score", new ColumnValue.DoubleValue(95.5))),
                TypedRow.of(Map.of(
                        "id", new ColumnValue.IntValue(2),
                        "name", new ColumnValue.NullValue(),
                        "score", new ColumnValue.DoubleValue(87.3)))
        );

        ColumnarPage columnar = RowPageConverter.fromLegacy(columns, rows);
        assertThat(columnar.getChannelCount()).isEqualTo(3);
        assertThat(columnar.getPositionCount()).isEqualTo(2);

        List<TypedRow> restored = RowPageConverter.toLegacy(columnar, columns);
        assertThat(restored).hasSize(2);

        for (int i = 0; i < rows.size(); i++) {
            for (String col : columns) {
                assertThat(restored.get(i).get(col))
                        .as("Row %d, column '%s'", i, col)
                        .isEqualTo(rows.get(i).get(col));
            }
        }
    }

    @Test
    void legacyAdapterRejectsEmptyColumns() {
        assertThatThrownBy(() ->
                RowPageConverter.fromLegacy(List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void legacyAdapterRejectsColumnCountMismatch() {
        Block block = new ArrayBlock(new ColumnValue[]{
                new ColumnValue.IntValue(1)
        });
        ColumnarPage page = new ColumnarPage(1, block);

        assertThatThrownBy(() ->
                RowPageConverter.toLegacy(page, List.of("a", "b")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void legacyAdapterHandlesNullValues() {
        List<String> columns = List.of("x");
        List<TypedRow> rows = List.of(
                TypedRow.of(Map.of("x", new ColumnValue.NullValue())),
                TypedRow.of(Map.of("x", new ColumnValue.NullValue()))
        );

        ColumnarPage columnar = RowPageConverter.fromLegacy(columns, rows);
        Block block = columnar.getBlock(0);
        assertThat(block.isNull(0)).isTrue();
        assertThat(block.isNull(1)).isTrue();

        List<TypedRow> restored = RowPageConverter.toLegacy(columnar, columns);
        assertThat(restored.get(0).get("x")).isInstanceOf(ColumnValue.NullValue.class);
        assertThat(restored.get(1).get("x")).isInstanceOf(ColumnValue.NullValue.class);
    }

    @Test
    void legacyAdapterHandlesBooleanValues() {
        List<String> columns = List.of("flag");
        List<TypedRow> rows = List.of(
                TypedRow.of(Map.of("flag", new ColumnValue.BooleanValue(true))),
                TypedRow.of(Map.of("flag", new ColumnValue.BooleanValue(false)))
        );

        ColumnarPage columnar = RowPageConverter.fromLegacy(columns, rows);
        List<TypedRow> restored = RowPageConverter.toLegacy(columnar, columns);

        assertThat(restored.get(0).get("flag"))
                .isEqualTo(new ColumnValue.BooleanValue(true));
        assertThat(restored.get(1).get("flag"))
                .isEqualTo(new ColumnValue.BooleanValue(false));
    }
}
