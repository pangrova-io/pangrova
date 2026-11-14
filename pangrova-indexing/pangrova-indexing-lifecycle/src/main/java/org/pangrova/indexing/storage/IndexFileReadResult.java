package org.pangrova.indexing.storage;

import java.util.Objects;

/**
 * Result of attempting to read an index file from storage.
 *
 * <p>Distinguishes three states so that callers can tell whether a {@code null}-ish
 * outcome is a normal "no index built yet" condition or a real "could not read"
 * failure. Conflating the two has caused circuit breakers to trip during normal
 * operation when newly-written data files have no indexes yet
 * (finding {@code F-INDEX-03}).</p>
 *
 * <ul>
 *   <li>{@link Status#PRESENT} — file existed and bytes were successfully read.</li>
 *   <li>{@link Status#MISSING} — file does not exist. Treat as "no index for this file";
 *       does <strong>not</strong> indicate a failure. Common during the window between
 *       a write commit and the asynchronous index build.</li>
 *   <li>{@link Status#ERROR} — file exists (or its existence cannot be determined) but
 *       reading it failed: I/O error, file exceeded the configured size cap, or
 *       similar. This <strong>is</strong> a failure and should be counted toward the
 *       per-index circuit breaker.</li>
 * </ul>
 */
public final class IndexFileReadResult {

    /** The three possible outcomes of an index file read attempt. */
    public enum Status {
        /** File existed and was successfully read. */
        PRESENT,
        /** File does not exist; treat as "no index", not a failure. */
        MISSING,
        /** File exists but could not be read (I/O error, oversized, etc.); a failure. */
        ERROR
    }

    private static final IndexFileReadResult MISSING_INSTANCE =
            new IndexFileReadResult(Status.MISSING, null);
    private static final IndexFileReadResult ERROR_INSTANCE =
            new IndexFileReadResult(Status.ERROR, null);

    private final Status status;
    private final byte[] bytes;

    private IndexFileReadResult(Status status, byte[] bytes) {
        this.status = status;
        this.bytes = bytes;
    }

    /** Creates a {@link Status#PRESENT} result wrapping the read bytes. */
    public static IndexFileReadResult present(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        return new IndexFileReadResult(Status.PRESENT, bytes.clone());
    }

    /** Returns the canonical {@link Status#MISSING} result. */
    public static IndexFileReadResult missing() {
        return MISSING_INSTANCE;
    }

    /** Returns the canonical {@link Status#ERROR} result. */
    public static IndexFileReadResult error() {
        return ERROR_INSTANCE;
    }

    public Status status() {
        return status;
    }

    /**
     * Returns the raw bytes read from storage.
     *
     * @throws IllegalStateException if {@link #status()} is not {@link Status#PRESENT}
     */
    public byte[] bytes() {
        if (status != Status.PRESENT) {
            throw new IllegalStateException("bytes() requires PRESENT status, got " + status);
        }
        return bytes.clone();
    }

    public boolean isPresent() {
        return status == Status.PRESENT;
    }

    public boolean isMissing() {
        return status == Status.MISSING;
    }

    public boolean isError() {
        return status == Status.ERROR;
    }
}
