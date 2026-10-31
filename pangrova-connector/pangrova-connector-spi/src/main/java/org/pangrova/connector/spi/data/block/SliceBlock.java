package org.pangrova.connector.spi.data.block;

import java.util.Arrays;
import java.util.Objects;

/**
 * Byte-array backed {@link Block} implementation for string/binary data.
 * Supports {@link #getSlice}; int/long/double throw.
 *
 * <p>Immutable after construction. Thread-safe for concurrent reads.
 * Returned slices are defensive copies.</p>
 */
public final class SliceBlock implements Block {

    private final byte[][] values;
    private final boolean[] nulls;
    private final int positionCount;

    /** Creates a slice block from byte values and null markers. */
    public SliceBlock(byte[][] values, boolean[] nulls, int positionCount) {
        Objects.requireNonNull(values, "values must not be null");
        Objects.requireNonNull(nulls, "nulls must not be null");
        if (positionCount < 0 || positionCount > values.length || positionCount > nulls.length) {
            throw new IllegalArgumentException("Invalid positionCount: " + positionCount);
        }
        this.positionCount = positionCount;
        this.nulls = Arrays.copyOf(nulls, positionCount);
        this.values = deepCopy(values, positionCount);
    }

    @Override
    public BlockTypeId typeId() {
        return BlockTypeId.SLICE;
    }

    @Override
    public int getPositionCount() {
        return positionCount;
    }

    @Override
    public boolean isNull(int position) {
        checkPosition(position);
        return nulls[position];
    }

    @Override
    public long getLong(int position) {
        throw new UnsupportedOperationException("SliceBlock does not support getLong");
    }

    @Override
    public double getDouble(int position) {
        throw new UnsupportedOperationException("SliceBlock does not support getDouble");
    }

    @Override
    public byte[] getSlice(int position) {
        checkPosition(position);
        return values[position] == null ? null : values[position].clone();
    }

    @Override
    public Block getRegion(int positionOffset, int length) {
        checkRegion(positionOffset, length);
        return new SliceBlock(
                Arrays.copyOfRange(values, positionOffset, positionOffset + length),
                Arrays.copyOfRange(nulls, positionOffset, positionOffset + length),
                length);
    }

    private void checkPosition(int position) {
        if (position < 0 || position >= positionCount) {
            throw new IndexOutOfBoundsException(
                    "Position " + position + " out of bounds for " + positionCount + " positions");
        }
    }

    private void checkRegion(int offset, int length) {
        if (offset < 0 || length < 0 || offset > positionCount - length) {
            throw new IndexOutOfBoundsException(
                    "Region [" + offset + ", " + ((long) offset + length) + ") out of bounds for " + positionCount);
        }
    }

    private static byte[][] deepCopy(byte[][] source, int length) {
        byte[][] copy = new byte[length][];
        for (int i = 0; i < length; i++) {
            copy[i] = source[i] == null ? null : source[i].clone();
        }
        return copy;
    }
}
