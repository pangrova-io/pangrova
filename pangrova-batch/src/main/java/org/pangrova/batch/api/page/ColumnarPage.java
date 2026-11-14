package org.pangrova.batch.api.page;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.batch.api.block.Block;

import java.util.Objects;

/**
 * Columnar page representation: an ordered array of {@link Block}s
 * (one per column) sharing the same position count.
 *
 * <p>This is the columnar counterpart to the row-based {@link Page} record.
 * Conversion algorithms are supplied by adapters outside this contract.</p>
 *
 * <p>Instances are immutable after construction. All blocks must have the
 * same position count as the page.</p>
 */
@Experimental
public final class ColumnarPage {

    private final Block[] blocks;
    private final int positionCount;

    /**
     * Creates a columnar page from the given blocks and position count.
     *
     * @param positionCount number of rows in this page
     * @param blocks        one block per column, each with matching positionCount
     * @throws IllegalArgumentException if positionCount is negative,
     *         blocks is empty, or any block has a mismatched position count
     */
    public ColumnarPage(int positionCount, Block... blocks) {
        if (positionCount < 0) {
            throw new IllegalArgumentException(
                    "positionCount must be non-negative, got " + positionCount);
        }
        Objects.requireNonNull(blocks, "blocks must not be null");
        if (blocks.length == 0) {
            throw new IllegalArgumentException("blocks must not be empty");
        }
        for (int i = 0; i < blocks.length; i++) {
            Objects.requireNonNull(blocks[i], "block at channel " + i + " is null");
            if (blocks[i].getPositionCount() != positionCount) {
                throw new IllegalArgumentException(
                        "Block at channel " + i + " has positionCount "
                                + blocks[i].getPositionCount()
                                + " but page positionCount is " + positionCount);
            }
        }
        this.positionCount = positionCount;
        this.blocks = blocks.clone();
    }

    /** Returns the number of columns (channels) in this page. */
    public int getChannelCount() {
        return blocks.length;
    }

    /**
     * Returns the block for the given channel (column index).
     *
     * @throws IndexOutOfBoundsException if channel is out of range
     */
    public Block getBlock(int channel) {
        return blocks[channel];
    }

    /** Returns the number of positions (rows) in this page. */
    public int getPositionCount() {
        return positionCount;
    }
}
