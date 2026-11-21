package org.pangrova.batch.api.block;

import org.pangrova.batch.api.enums.BlockTypeId;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Arrays;
import java.util.Objects;

/**
 * Dictionary-encoded {@link Block} that wraps a dictionary block of unique
 * values and an int[] of ids mapping each position to a dictionary entry.
 *
 * <p>Useful for columns with low cardinality where many positions share
 * the same value. All accessor methods delegate to the dictionary block
 * using the mapped id.</p>
 *
 * <p>Immutable after construction. Thread-safe for concurrent reads.</p>
 */
@Experimental
public final class DictionaryBlock implements Block {

    private final Block dictionary;
    private final int[] ids;

    /** Creates a dictionary block with one dictionary id per position. */
    public DictionaryBlock(Block dictionary, int[] ids) {
        Objects.requireNonNull(dictionary, "dictionary must not be null");
        Objects.requireNonNull(ids, "ids must not be null");
        this.dictionary = dictionary;
        this.ids = ids.clone();
        validateIds();
    }

    @Override
    public BlockTypeId typeId() {
        return dictionary.typeId();
    }

    @Override
    public int getPositionCount() {
        return ids.length;
    }

    @Override
    public boolean isNull(int position) {
        checkPosition(position);
        return dictionary.isNull(ids[position]);
    }

    @Override
    public int getInt(int position) {
        checkPosition(position);
        return dictionary.getInt(ids[position]);
    }

    @Override
    public long getLong(int position) {
        checkPosition(position);
        return dictionary.getLong(ids[position]);
    }

    @Override
    public double getDouble(int position) {
        checkPosition(position);
        return dictionary.getDouble(ids[position]);
    }

    @Override
    public byte[] getSlice(int position) {
        checkPosition(position);
        return dictionary.getSlice(ids[position]);
    }

    @Override
    public Block getRegion(int positionOffset, int length) {
        checkRegion(positionOffset, length);
        return new DictionaryBlock(
                dictionary,
                Arrays.copyOfRange(ids, positionOffset, positionOffset + length));
    }

    private void checkPosition(int position) {
        if (position < 0 || position >= ids.length) {
            throw new IndexOutOfBoundsException(
                    "Position " + position + " out of bounds for " + ids.length + " positions");
        }
    }

    private void checkRegion(int offset, int length) {
        if (offset < 0 || length < 0 || offset > ids.length - length) {
            throw new IndexOutOfBoundsException(
                    "Region [" + offset + ", " + ((long) offset + length) + ") out of bounds for " + ids.length);
        }
    }

    private void validateIds() {
        int dictSize = dictionary.getPositionCount();
        for (int id : ids) {
            if (id < 0 || id >= dictSize) {
                throw new IllegalArgumentException(
                        "Dictionary id " + id + " out of bounds for dictionary size " + dictSize);
            }
        }
    }
}
