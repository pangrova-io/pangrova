package org.pangrova.streaming.cdc.commit;

/**
 * Represents the consumed offset window for an exact topic/partition.
 * Source logs may contain offset gaps, for example after compaction.
 *
 * <p>The range is inclusive on both ends: [startOffset, endOffset].
 * Used in the two-phase commit protocol to identify the exact offset window
 * being prepared/committed.</p>
 *
 * @param topic source topic name
 * @param partition  partition number (must be non-negative)
 * @param startOffset first offset in the range (inclusive, must be non-negative)
 * @param endOffset   last offset in the range (inclusive, must be >= startOffset)
 */
public record OffsetRange(String topic, int partition, long startOffset, long endOffset) {

    public OffsetRange {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be null or blank");
        }
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
        if (endOffset == Long.MAX_VALUE) {
            throw new IllegalArgumentException("endOffset must leave room for the next offset");
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
