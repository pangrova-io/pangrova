package org.pangrova.batch.api.page;

import static org.pangrova.common.util.Preconditions.checkArgument;
import static org.pangrova.common.util.Preconditions.checkNotNull;
import static org.pangrova.common.util.Preconditions.checkState;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Columnar data exchange unit wrapping serialized batch data in the format
 * indicated by {@link #format()}.
 *
 * <p>This class transports serialized record batches between components
 * without requiring an Arrow library dependency in the Batch API module.
 * Construction copies the input bytes into an owned snapshot.
 * The actual wire format depends on the producing code path — callers must
 * inspect {@link #format()} before interpreting the byte payload.</p>
 *
 * <p>The payload is an immutable heap snapshot, not a Native/JNI handle. The creator
 * owns the page and closes it when no longer needed. {@link #close()} is idempotent
 * and rejects new payload reads; a read that passed the closed check may finish
 * concurrently with close. Already returned copies and size/format metadata remain
 * valid after close. Heap storage is reclaimed by the JVM when no longer referenced.</p>
 *
 * @see BatchFormat
 */
@Experimental
public final class NativePage implements AutoCloseable {

    private final byte[] serializedBatch;
    private final int rowCount;
    private final int columnCount;
    private final BatchFormat format;
    private volatile boolean closed;

    /**
     * Creates a NativePage from serialized record batch data.
     *
     * @param serializedBatch serialized record batch bytes
     * @param rowCount        number of rows in this batch, or -1 if unknown
     * @param columnCount     number of columns in this batch, or -1 if unknown
     * @param format          the wire format of the serialized data
     */
    public NativePage(byte[] serializedBatch, int rowCount, int columnCount, BatchFormat format) {
        this.serializedBatch = checkNotNull(serializedBatch, "serializedBatch must not be null").clone();
        checkArgument(rowCount >= -1, "rowCount must be >= -1");
        checkArgument(columnCount >= -1, "columnCount must be >= -1");
        this.format = checkNotNull(format, "format must not be null");
        this.rowCount = rowCount;
        this.columnCount = columnCount;
    }

    /**
     * Returns a caller-owned copy of the serialized payload.
     *
     * @throws IllegalStateException if this page has been closed
     */
    public byte[] serializedBatch() {
        checkState(!closed, "NativePage has been closed");
        return serializedBatch.clone();
    }

    /** Returns the encoded payload length without copying it, including after close. */
    public int serializedSizeInBytes() {
        return serializedBatch.length;
    }

    /**
     * Returns the wire format of the serialized batch data.
     */
    public BatchFormat format() {
        return format;
    }

    public int getRowCount() {
        return rowCount;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public boolean isEmpty() {
        return rowCount == 0;
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
    }
}
