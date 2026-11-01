package org.pangrova.spi.connector.data.block;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;

import org.pangrova.spi.connector.data.value.ColumnValue;

/**
 * A simple array-backed Block implementation that stores {@link ColumnValue}
 * entries for row-to-columnar conversion.
 *
 * <p>Primitive columnar paths may instead use typed blocks such as
 * IntBlock, LongBlock and DoubleBlock.</p>
 */
@Experimental
public final class ArrayBlock implements Block {

    private final ColumnValue[] values;

    public ArrayBlock(ColumnValue[] values) {
        Objects.requireNonNull(values, "values must not be null");
        this.values = values.clone();
    }

    @Override
    public BlockTypeId typeId() {
        return BlockTypeId.ARRAY;
    }

    @Override
    public int getPositionCount() {
        return values.length;
    }

    @Override
    public boolean isNull(int position) {
        checkPosition(position);
        return values[position] instanceof ColumnValue.NullValue;
    }

    @Override
    public int getInt(int position) {
        checkPosition(position);
        ColumnValue cv = values[position];
        if (cv instanceof ColumnValue.IntValue iv) {
            return iv.value();
        }
        throw new UnsupportedOperationException(
                "Cannot read int from " + cv.getClass().getSimpleName());
    }

    @Override
    public long getLong(int position) {
        checkPosition(position);
        ColumnValue cv = values[position];
        if (cv instanceof ColumnValue.LongValue lv) {
            return lv.value();
        }
        if (cv instanceof ColumnValue.IntValue iv) {
            return iv.value();
        }
        throw new UnsupportedOperationException(
                "Cannot read long from " + cv.getClass().getSimpleName());
    }

    @Override
    public double getDouble(int position) {
        checkPosition(position);
        ColumnValue cv = values[position];
        if (cv instanceof ColumnValue.DoubleValue dv) {
            return dv.value();
        }
        if (cv instanceof ColumnValue.FloatValue fv) {
            return fv.value();
        }
        throw new UnsupportedOperationException(
                "Cannot read double from " + cv.getClass().getSimpleName());
    }

    @Override
    public byte[] getSlice(int position) {
        checkPosition(position);
        ColumnValue cv = values[position];
        if (cv instanceof ColumnValue.StringValue sv) {
            return sv.value().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
        if (cv instanceof ColumnValue.BytesValue bv) {
            return bv.value().clone();
        }
        throw new UnsupportedOperationException(
                "Cannot read slice from " + cv.getClass().getSimpleName());
    }

    @Override
    public Block getRegion(int positionOffset, int length) {
        if (positionOffset < 0 || length < 0
                || positionOffset > values.length - length) {
            throw new IndexOutOfBoundsException(
                    "Region [" + positionOffset + ", "
                            + ((long) positionOffset + length) + ") out of bounds for "
                            + values.length + " positions");
        }
        ColumnValue[] region = new ColumnValue[length];
        System.arraycopy(values, positionOffset, region, 0, length);
        return new ArrayBlock(region);
    }

    /**
     * Returns the raw ColumnValue at the given position.
     * Used by adapters converting columnar data back to typed rows.
     */
    public ColumnValue getColumnValue(int position) {
        checkPosition(position);
        return values[position];
    }

    private void checkPosition(int position) {
        if (position < 0 || position >= values.length) {
            throw new IndexOutOfBoundsException(
                    "Position " + position + " out of bounds for "
                            + values.length + " positions");
        }
    }
}
