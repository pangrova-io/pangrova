package org.pangrova.batch.api.block;

import org.pangrova.batch.api.enums.BlockTypeId;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Arrays;
import java.util.Objects;

/**
 * Primitive long-array backed {@link Block} implementation.
 * Supports {@link #getLong}; {@link #getInt} throws.
 *
 * <p>Immutable after construction. Thread-safe for concurrent reads.</p>
 */
@Experimental
public final class LongBlock implements Block {

    private final long[] values;
    private final boolean[] nulls;
    private final int positionCount;

    /** Creates a long block from values and null markers. */
    public LongBlock(long[] values, boolean[] nulls, int positionCount) {
        Objects.requireNonNull(values, "values must not be null");
        Objects.requireNonNull(nulls, "nulls must not be null");
        if (positionCount < 0 || positionCount > values.length || positionCount > nulls.length) {
            throw new IllegalArgumentException("Invalid positionCount: " + positionCount);
        }
        this.positionCount = positionCount;
        this.values = Arrays.copyOf(values, positionCount);
        this.nulls = Arrays.copyOf(nulls, positionCount);
    }

    @Override
    public BlockTypeId typeId() {
        return BlockTypeId.LONG;
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
        checkPosition(position);
        return values[position];
    }

    @Override
    public double getDouble(int position) {
        throw new UnsupportedOperationException("LongBlock does not support getDouble");
    }

    @Override
    public byte[] getSlice(int position) {
        throw new UnsupportedOperationException("LongBlock does not support getSlice");
    }

    @Override
    public Block getRegion(int positionOffset, int length) {
        checkRegion(positionOffset, length);
        return new LongBlock(
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
}
