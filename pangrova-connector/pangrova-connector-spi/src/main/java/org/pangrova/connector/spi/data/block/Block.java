package org.pangrova.connector.spi.data.block;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Core interface for columnar data access. A Block represents a single column
 * of data with a fixed number of positions, supporting type-safe value retrieval
 * and null tracking.
 *
 * <p>Block is the fundamental building block of the columnar data model.
 * Each Block stores values for one column across multiple rows (positions).
 * Implementations must be thread-safe for concurrent reads.</p>
 *
 * @see ColumnarPage
 * @see DictionaryBlock
 * @see LazyBlock
 * @see RunLengthEncodedBlock
 */
@Experimental
public interface Block {

    /** Returns the number of positions (rows) in this block. */
    int getPositionCount();

    /**
     * Returns the type identifier for this block, enabling efficient
     * type dispatch without exception-based probing.
     */
    BlockTypeId typeId();

    /**
     * Returns the int value at the given position.
     *
     * @throws UnsupportedOperationException if this block does not hold int values
     * @throws IndexOutOfBoundsException if position is out of range
     */
    default int getInt(int position) {
        throw new UnsupportedOperationException("getInt not supported by " + getClass().getSimpleName());
    }

    /** Returns true if the value at the given position is null. */
    boolean isNull(int position);

    /**
     * Returns the long value at the given position.
     *
     * @throws UnsupportedOperationException if this block does not hold long values
     * @throws IndexOutOfBoundsException if position is out of range
     */
    long getLong(int position);

    /**
     * Returns the double value at the given position.
     *
     * @throws UnsupportedOperationException if this block does not hold double values
     * @throws IndexOutOfBoundsException if position is out of range
     */
    double getDouble(int position);

    /**
     * Returns the byte slice at the given position (for string/binary data).
     *
     * @throws UnsupportedOperationException if this block does not hold slice values
     * @throws IndexOutOfBoundsException if position is out of range
     */
    byte[] getSlice(int position);

    /**
     * Returns a sub-region of this block as a new Block.
     *
     * @param positionOffset starting position (inclusive)
     * @param length number of positions in the region
     * @return a new Block representing the specified region
     * @throws IndexOutOfBoundsException if the region is out of range
     */
    Block getRegion(int positionOffset, int length);
}
