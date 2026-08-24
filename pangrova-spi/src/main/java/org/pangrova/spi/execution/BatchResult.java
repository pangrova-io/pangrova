package org.pangrova.spi.execution;

/**
 * Discriminated result from native batch retrieval.
 * Distinguishes DATA, EOF, NOT_READY, and ERROR states.
 *
 * <p>This replaces the ambiguous null-for-everything return from
 * {@link TaskExecutor#getNextBatch}, enabling callers to correctly
 * handle transient not-ready states without treating them as EOF.</p>
 *
 * <p>BUG-14 fix: Added {@code dataOffset} and {@code dataLength} to support
 * zero-copy slicing of the raw JNI byte array. The status byte occupies
 * index 0; payload starts at offset 1. Callers should use
 * {@link #dataOffset()} and {@link #dataLength()} instead of assuming
 * the entire {@link #data()} array is payload.</p>
 */
public record BatchResult(Status status, byte[] data, int dataOffset, int dataLength, String errorMessage) {

    public enum Status {
        /** Valid batch data available. */
        DATA,
        /** End of stream — no more batches will be produced. */
        EOF,
        /** Temporary — output queue empty but task still running. Retry. */
        NOT_READY,
        /** Native execution error — task has failed. */
        ERROR
    }

    /** Creates a DATA result owning the full payload array (backward compat). */
    public static BatchResult data(byte[] payload) {
        return new BatchResult(Status.DATA, payload, 0, payload != null ? payload.length : 0, null);
    }

    /**
     * Creates a DATA result referencing a slice of the raw array (zero-copy).
     * The caller must not mutate the backing array while this result is alive.
     */
    public static BatchResult dataSlice(byte[] raw, int offset, int length) {
        return new BatchResult(Status.DATA, raw, offset, length, null);
    }

    public static BatchResult eof() {
        return new BatchResult(Status.EOF, null, 0, 0, null);
    }

    public static BatchResult notReady() {
        return new BatchResult(Status.NOT_READY, null, 0, 0, null);
    }

    public static BatchResult error(String message) {
        return new BatchResult(Status.ERROR, null, 0, 0, message);
    }

    /**
     * Returns a standalone copy of the payload bytes.
     * For sliced results ({@link #dataSlice}), this copies the relevant
     * portion into a new array. For owned results ({@link #data(byte[])}),
     * returns the original array if offset=0 and length=data.length.
     */
    public byte[] copyData() {
        if (data == null || dataLength == 0) {
            return data;
        }
        if (dataOffset == 0 && dataLength == data.length) {
            return data;
        }
        byte[] copy = new byte[dataLength];
        System.arraycopy(data, dataOffset, copy, 0, dataLength);
        return copy;
    }
}
