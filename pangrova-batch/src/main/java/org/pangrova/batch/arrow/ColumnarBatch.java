package org.pangrova.batch.arrow;

import org.pangrova.common.concurrency.NotThreadSafe;
import org.pangrova.batch.api.reader.ReadableBatch;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 * Scoped, decoded column buffers for one schema + record batch + EOS IPC stream.
 * The allocator is caller-owned. Closing releases the reader and invalidates all
 * projected views. The input buffer is borrowed only while opening/closing this
 * batch; consumers must finish synchronously before the JNI call returns.
 */
@NotThreadSafe
public final class ColumnarBatch implements ReadableBatch {
    private static final int UNSIGNED_BYTE_MASK = 0xff;
    private final ArrowStreamReader reader;
    private final VectorSchemaRoot root;
    private final ColumnarBatch owner;
    private final List<String> columns;
    private final int[] sources;
    private final int[] fallbacks;
    private boolean closed;

    private ColumnarBatch(ArrowStreamReader reader, VectorSchemaRoot root) {
        this.reader = reader;
        this.root = root;
        this.owner = null;
        this.columns = root.getSchema().getFields().stream().map(field -> field.getName()).toList();
        this.sources = null;
        this.fallbacks = null;
    }

    private ColumnarBatch(ColumnarBatch owner, List<String> columns, int[] sources, int[] fallbacks) {
        this.reader = null;
        this.root = null;
        this.owner = owner;
        this.columns = List.copyOf(columns);
        this.sources = sources.clone();
        this.fallbacks = fallbacks.clone();
    }

    /**
     * Opens exactly one plain batch, validating framing and advertised dimensions
     * before a sink sees any values. Empty input is malformed; a zero-row batch
     * must still carry its schema and EOS. Input position and limit are unchanged.
     *
     * @param data borrowed IPC bytes in the buffer's remaining range
     * @param allocator caller-owned allocator for decoded buffers
     * @param rowCount exact nonnegative number of rows
     * @param columnCount exact nonnegative number of source columns, before projection
     * @return closeable batch owning the reader, not the allocator
     * @throws IOException for malformed data or dimension mismatch
     */
    public static ColumnarBatch open(ByteBuffer data, BufferAllocator allocator,
                                     int rowCount, int columnCount) throws IOException {
        validateOpenArguments(data, rowCount, columnCount);
        try {
            BatchStreamValidation.validate(data);
        } catch (RuntimeException e) {
            throw new IOException("Malformed IPC stream metadata", e);
        }
        ArrowStreamReader reader = new ArrowStreamReader(new BufferInput(data.slice()), allocator);
        try {
            return readBatch(reader, rowCount, columnCount);
        } catch (IOException | RuntimeException | Error failure) {
            closeAfterFailure(reader, failure);
            if (failure instanceof RuntimeException) {
                throw new IOException("Failed to decode IPC batch", failure);
            }
            throw failure;
        }
    }

    private static void validateOpenArguments(ByteBuffer data, int rowCount, int columnCount)
            throws IOException {
        if (data == null || rowCount < 0 || columnCount < 0) {
            throw new IOException("Batch data and nonnegative dimensions are required");
        }
    }

    private static ColumnarBatch readBatch(
            ArrowStreamReader reader, int rowCount, int columnCount) throws IOException {
        if (!reader.loadNextBatch()) {
            throw new IOException("Expected one record batch");
        }
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        if (root.getRowCount() != rowCount || root.getFieldVectors().size() != columnCount) {
            throw new IOException("Batch dimensions differ: expected=" + rowCount + "x" + columnCount
                    + " actual=" + root.getRowCount() + "x" + root.getFieldVectors().size());
        }
        return new ColumnarBatch(reader, root);
    }

    private static void closeAfterFailure(ArrowStreamReader reader, Throwable failure) {
        try {
            reader.close();
        } catch (IOException | RuntimeException | Error cleanup) {
            failure.addSuppressed(cleanup);
        }
    }

    /** @return ordered names (duplicates in source streams are preserved) */
    public List<String> columns() {
        ensureOpen();
        return columns;
    }

    /** @return number of rows, including zero for a schema-bearing empty batch */
    public int rowCount() {
        ensureOpen();
        return owner == null ? root.getRowCount() : owner.rowCount();
    }

    /**
     * Reads one detached Java cell for conversion at a connector framework boundary.
     * Nested collections preserve nulls and map key types; binary values are copied.
     *
     * @param row zero-based row index
     * @param column zero-based column index
     * @return scalar, list, map, or null; never a borrowed Arrow value
     * @throws IndexOutOfBoundsException for an invalid index
     * @throws IllegalStateException after this batch or its owner has closed
     */
    public Object value(int row, int column) {
        Objects.checkIndex(row, rowCount());
        Objects.checkIndex(column, columns.size());
        if (owner == null) {
            var vector = root.getVector(column);
            return vector.isNull(row) ? null : BatchValues.detach(vector.getField(), vector.getObject(row));
        }
        Object value = sources[column] < 0 ? null : owner.value(row, sources[column]);
        return value == null && fallbacks[column] >= 0 ? owner.value(row, fallbacks[column]) : value;
    }

    /**
     * Creates an ordinal projection without allocating any rows or copying vectors.
     * Closing a view invalidates only the view; its owner must remain open.
     *
     * @param names ordered projected names
     * @param sourceColumns source ordinals, or -1 for a null column
     * @param nullFallbackColumns source ordinals used only for null primary cells, or -1
     * @return borrowed view, valid only while its owner is open
     * @throws IllegalArgumentException if lengths or ordinals are invalid
     */
    public ColumnarBatch project(List<String> names, int[] sourceColumns, int[] nullFallbackColumns) {
        ensureOpen();
        if (names.size() != sourceColumns.length || names.size() != nullFallbackColumns.length) {
            throw new IllegalArgumentException("Projection lengths differ");
        }
        for (int i = 0; i < names.size(); i++) {
            if (sourceColumns[i] < -1 || sourceColumns[i] >= columns.size()
                    || nullFallbackColumns[i] < -1 || nullFallbackColumns[i] >= columns.size()) {
                throw new IllegalArgumentException("Invalid projection ordinal");
            }
        }
        return new ColumnarBatch(this, names, sourceColumns, nullFallbackColumns);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Batch is closed");
        }
        if (owner != null) {
            owner.ensureOpen();
        }
    }

    /** Releases decoded buffers, even on write failure; does not close the allocator. */
    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            if (reader != null) {
                reader.close();
            }
        }
    }

    private static final class BufferInput extends InputStream {
        private final ByteBuffer data;

        private BufferInput(ByteBuffer data) {
            this.data = data;
        }

        @Override
        public int read() {
            return data.hasRemaining() ? data.get() & UNSIGNED_BYTE_MASK : -1;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) {
            Objects.checkFromIndexSize(offset, length, bytes.length);
            if (length == 0) {
                return 0;
            }
            if (!data.hasRemaining()) {
                return -1;
            }
            int count = Math.min(length, data.remaining());
            data.get(bytes, offset, count);
            return count;
        }
    }
}
