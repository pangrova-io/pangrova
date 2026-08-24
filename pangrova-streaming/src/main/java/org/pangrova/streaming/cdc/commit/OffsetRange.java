package org.pangrova.streaming.cdc.commit;

/**
 * Represents a contiguous range of offsets for a single partition within a CDC source.
 *
 * <p>The range is inclusive on both ends: [startOffset, endOffset].
 * Used in the two-phase commit protocol to identify the exact offset window
 * being prepared/committed.</p>
 *
 * @param partition  partition number (must be non-negative)
 * @param startOffset first offset in the range (inclusive, must be non-negative)
 * @param endOffset   last offset in the range (inclusive, must be >= startOffset)
 */
public record OffsetRange(int partition, long startOffset, long endOffset) {

    public OffsetRange {
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be non-negative, got: " + partition);
        }
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset must be non-negative, got: " + startOffset);
        }
        if (endOffset < startOffset) {
            throw new IllegalArgumentException(
                "endOffset must be >= startOffset, got startOffset=" + startOffset + " endOffset=" + endOffset);
        }
    }

    /**
     * Returns the number of offsets in this range.
     */
    public long count() {
        return endOffset - startOffset + 1;
    }

    /**
     * Checks whether the given offset falls within this range (inclusive).
     */
    public boolean contains(long offset) {
        return offset >= startOffset && offset <= endOffset;
    }
}
