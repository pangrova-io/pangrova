package org.pangrova.batch.api.page;

import org.pangrova.common.api.annotation.Experimental;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.pangrova.types.value.ColumnValue;
import org.pangrova.types.value.TypedRow;
import org.pangrova.batch.api.block.ArrayBlock;
import org.pangrova.batch.api.block.Block;
import org.pangrova.batch.api.enums.BlockTypeId;
import org.pangrova.batch.api.block.DoubleBlock;
import org.pangrova.batch.api.block.IntBlock;
import org.pangrova.batch.api.block.LongBlock;
import org.pangrova.batch.api.block.SliceBlock;

/**
 * Converter for converting between the row-based {@link Page} and the
 * columnar {@link ColumnarPage} representations.
 *
 * <p>Round-trip conversion preserves data fidelity:
 * {@code toLegacy(fromLegacy(columns, rows), columns)} produces rows
 * equal to the original input.</p>
 *
 * <p>Utility class — not instantiable.</p>
 */
@Experimental
public final class RowPageConverter {

    private RowPageConverter() {
        throw new AssertionError("Utility class");
    }

    /**
     * Converts a row-based {@link Page} into a columnar page.
     *
     * @param page the row-based page to convert
     * @return a ColumnarPage with one ArrayBlock per column
     */
    public static ColumnarPage fromLegacy(Page page) {
        Objects.requireNonNull(page, "page must not be null");
        return fromLegacy(page.columns(), page.rows());
    }

    /**
     * Converts a row-based page into a columnar page.
     *
     * @param columns ordered column names
     * @param rows    list of typed rows
     * @return a ColumnarPage with typed blocks per column
     * @throws IllegalArgumentException if columns is empty
     */
    public static ColumnarPage fromLegacy(List<String> columns, List<TypedRow> rows) {
        Objects.requireNonNull(columns, "columns must not be null");
        Objects.requireNonNull(rows, "rows must not be null");
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("columns must not be empty");
        }

        int rowCount = rows.size();
        int colCount = columns.size();
        Block[] blocks = new Block[colCount];

        for (int col = 0; col < colCount; col++) {
            String columnName = columns.get(col);
            ColumnValue[] colValues = new ColumnValue[rowCount];
            for (int row = 0; row < rowCount; row++) {
                colValues[row] = rows.get(row).get(columnName);
            }
            blocks[col] = buildTypedBlock(colValues, rowCount);
        }

        return new ColumnarPage(rowCount, blocks);
    }

    private static Block buildTypedBlock(ColumnValue[] colValues, int rowCount) {
        if (rowCount == 0) {
            return new ArrayBlock(colValues);
        }
        BlockTypeId detectedType = detectColumnType(colValues);
        if (detectedType == null) {
            return new ArrayBlock(colValues);
        }
        return switch (detectedType) {
            case INT -> buildIntBlock(colValues, rowCount);
            case LONG -> buildLongBlock(colValues, rowCount);
            case DOUBLE -> buildDoubleBlock(colValues, rowCount);
            case SLICE -> buildSliceBlock(colValues, rowCount);
            default -> new ArrayBlock(colValues);
        };
    }

    private static BlockTypeId detectColumnType(ColumnValue[] values) {
        BlockTypeId detected = null;
        for (ColumnValue cv : values) {
            if (cv instanceof ColumnValue.NullValue) {
                continue;
            }
            BlockTypeId current = VALUE_TYPE_MAP.get(cv.getClass());
            if (current == null) {
                return null;
            }
            if (detected == null) {
                detected = current;
            } else if (detected != current) {
                return null;
            }
        }
        return detected;
    }

    private static final java.util.Map<Class<? extends ColumnValue>, BlockTypeId> VALUE_TYPE_MAP =
            java.util.Map.of(
                    ColumnValue.IntValue.class, BlockTypeId.INT,
                    ColumnValue.LongValue.class, BlockTypeId.LONG,
                    ColumnValue.DoubleValue.class, BlockTypeId.DOUBLE,
                    ColumnValue.StringValue.class, BlockTypeId.SLICE);

    private static IntBlock buildIntBlock(ColumnValue[] values, int count) {
        int[] data = new int[count];
        boolean[] nulls = new boolean[count];
        for (int i = 0; i < count; i++) {
            if (values[i] instanceof ColumnValue.IntValue iv) {
                data[i] = iv.value();
            } else {
                nulls[i] = true;
            }
        }
        return new IntBlock(data, nulls, count);
    }

    private static LongBlock buildLongBlock(ColumnValue[] values, int count) {
        long[] data = new long[count];
        boolean[] nulls = new boolean[count];
        for (int i = 0; i < count; i++) {
            if (values[i] instanceof ColumnValue.LongValue lv) {
                data[i] = lv.value();
            } else {
                nulls[i] = true;
            }
        }
        return new LongBlock(data, nulls, count);
    }

    private static DoubleBlock buildDoubleBlock(ColumnValue[] values, int count) {
        double[] data = new double[count];
        boolean[] nulls = new boolean[count];
        for (int i = 0; i < count; i++) {
            if (values[i] instanceof ColumnValue.DoubleValue dv) {
                data[i] = dv.value();
            } else {
                nulls[i] = true;
            }
        }
        return new DoubleBlock(data, nulls, count);
    }

    private static SliceBlock buildSliceBlock(ColumnValue[] values, int count) {
        byte[][] data = new byte[count][];
        boolean[] nulls = new boolean[count];
        for (int i = 0; i < count; i++) {
            if (values[i] instanceof ColumnValue.StringValue sv) {
                data[i] = sv.value().getBytes(java.nio.charset.StandardCharsets.UTF_8);

            } else {
                nulls[i] = true;
            }
        }
        return new SliceBlock(data, nulls, count);
    }

    /**
     * Converts a columnar page back into a list of typed rows.
     *
     * <p>Numeric and string blocks are decoded by their declared block type.
     * Values without a lossless specialized representation, including bytes,
     * remain in {@link ArrayBlock} so their logical type is preserved.</p>
     *
     * @param page    the columnar page to convert
     * @param columns ordered column names matching the page channels
     * @return list of TypedRow instances
     * @throws IllegalArgumentException if columns size does not match channel count
     */
    public static List<TypedRow> toLegacy(ColumnarPage page, List<String> columns) {
        Objects.requireNonNull(page, "page must not be null");
        Objects.requireNonNull(columns, "columns must not be null");
        if (columns.size() != page.getChannelCount()) {
            throw new IllegalArgumentException(
                    "columns size " + columns.size()
                            + " does not match channel count " + page.getChannelCount());
        }

        int rowCount = page.getPositionCount();
        List<TypedRow> rows = new ArrayList<>(rowCount);

        for (int row = 0; row < rowCount; row++) {
            Map<String, ColumnValue> values = new LinkedHashMap<>();
            for (int col = 0; col < columns.size(); col++) {
                Block block = page.getBlock(col);
                ColumnValue cv = extractColumnValue(block, row);
                values.put(columns.get(col), cv);
            }
            rows.add(TypedRow.of(values));
        }

        return List.copyOf(rows);
    }

    private static ColumnValue extractColumnValue(Block block, int position) {
        if (block instanceof ArrayBlock ab) {
            return ab.getColumnValue(position);
        }
        // Generic fallback for non-ArrayBlock implementations
        if (block.isNull(position)) {
            return new ColumnValue.NullValue();
        }
        return extractFromGenericBlock(block, position);
    }

    private static ColumnValue extractFromGenericBlock(Block block, int position) {
        return switch (block.typeId()) {
            case INT -> new ColumnValue.IntValue(block.getInt(position));
            case LONG -> new ColumnValue.LongValue(block.getLong(position));
            case DOUBLE -> new ColumnValue.DoubleValue(block.getDouble(position));
            case SLICE -> new ColumnValue.StringValue(
                    new String(block.getSlice(position), java.nio.charset.StandardCharsets.UTF_8));
            case ARRAY -> {
                if (block instanceof ArrayBlock ab) {
                    yield ab.getColumnValue(position);
                }
                throw new UnsupportedOperationException(
                        "Cannot extract ColumnValue from block type: "
                                + block.getClass().getSimpleName());
            }
        };
    }
}
