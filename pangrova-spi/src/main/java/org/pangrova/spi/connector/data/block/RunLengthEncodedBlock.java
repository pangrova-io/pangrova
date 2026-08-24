package org.pangrova.spi.connector.data.block;

import java.util.Objects;

/**
 * Run-length encoded {@link Block}: a single value repeated for all positions.
 * The wrapped value block must have exactly one position.
 *
 * <p>Efficient for constant columns or fill values. All accessor methods
 * delegate to position 0 of the value block regardless of the requested
 * position.</p>
 *
 * <p>Immutable after construction. Thread-safe for concurrent reads.</p>
 */
public final class RunLengthEncodedBlock implements Block {

    private final Block value;
    private final int positionCount;

    /** Creates a run-length encoded block from a single-position value block. */
    public RunLengthEncodedBlock(Block value, int positionCount) {
        Objects.requireNonNull(value, "value must not be null");
        if (value.getPositionCount() != 1) {
            throw new IllegalArgumentException(
                    "Value block must have exactly 1 position, got " + value.getPositionCount());
        }
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount must be non-negative");
        }
        this.value = value;
        this.positionCount = positionCount;
    }

    @Override
    public BlockTypeId typeId() {
        return value.typeId();
    }

    @Override
    public int getPositionCount() {
        return positionCount;
    }

    @Override
    public boolean isNull(int position) {
        checkPosition(position);
        return value.isNull(0);
    }

    @Override
    public int getInt(int position) {
        checkPosition(position);
        return value.getInt(0);
    }

    @Override
    public long getLong(int position) {
        checkPosition(position);
        return value.getLong(0);
    }

    @Override
    public double getDouble(int position) {
        checkPosition(position);
        return value.getDouble(0);
    }

    @Override
    public byte[] getSlice(int position) {
        checkPosition(position);
        return value.getSlice(0);
    }

    @Override
    public Block getRegion(int positionOffset, int length) {
        checkRegion(positionOffset, length);
        return new RunLengthEncodedBlock(value, length);
    }

    private void checkPosition(int position) {
        if (position < 0 || position >= positionCount) {
            throw new IndexOutOfBoundsException(
                    "Position " + position + " out of bounds for " + positionCount + " positions");
        }
    }

    private void checkRegion(int offset, int length) {
        if (offset < 0 || length < 0 || offset + length > positionCount) {
            throw new IndexOutOfBoundsException(
                    "Region [" + offset + ", " + (offset + length) + ") out of bounds for " + positionCount);
        }
    }
}
