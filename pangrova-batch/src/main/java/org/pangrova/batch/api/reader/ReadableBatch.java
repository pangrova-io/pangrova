package org.pangrova.batch.api.reader;

import org.pangrova.common.api.annotation.Experimental;

import java.io.IOException;
import java.util.List;

/**
 * Synchronous access to a decoded batch without exposing its storage library.
 *
 * <p>The creator owns and closes the batch. A callee borrowing it must finish
 * before returning and must not retain or close it. Instances and their views
 * are confined to the calling thread; concurrent reads, projection and close
 * are not supported. Values returned by {@link #value(int, int)} are detached
 * and may outlive the batch. Closing a view never closes its owner, while
 * closing the owner invalidates every view.</p>
 */
@Experimental
public interface ReadableBatch extends AutoCloseable {
    /** Returns ordered column names; duplicate names are preserved. */
    List<String> columns();

    /** Returns the nonnegative row count, including schema-bearing empty batches. */
    int rowCount();

    /**
     * Returns a detached Java scalar, list, map or null. Binary values are copied;
     * nested nulls and map key types are preserved.
     *
     * @throws IndexOutOfBoundsException for an invalid row or column
     * @throws IllegalStateException after this batch or its owner is closed
     */
    Object value(int row, int column);

    /**
     * Creates a borrowed ordinal view. Input lists and arrays are snapshotted.
     * Each source ordinal may be -1 for null; a fallback ordinal is used only
     * when the primary value is null. The returned view must be closed by its
     * creator and remains usable only while this batch and its owners are open.
     *
     * @throws IllegalArgumentException for unequal lengths or invalid ordinals
     * @throws IllegalStateException if this batch or its owner is closed
     */
    ReadableBatch project(List<String> names, int[] sourceColumns, int[] nullFallbackColumns);

    /**
     * Releases owned buffers and invalidates this batch. Idempotent. Does not
     * close a caller-owned allocator or the owner of a borrowed view.
     *
     * @throws IOException if releasing the underlying reader fails
     */
    @Override
    void close() throws IOException;
}
