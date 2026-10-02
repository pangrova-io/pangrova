package org.pangrova.worker.jni.write;

/**
 * JNI bridge interface for native-to-Java write operations.
 *
 * <p>Native workers call these methods via JNI to write data through Java
 * connector {@link org.pangrova.spi.connector.data.ConnectorPageSink}
 * implementations. This is the write-path counterpart of the existing
 * native read bridge, maintaining architectural symmetry.
 *
 * <h3>Thread model</h3>
 * The native thread calls JNI; Java executes on the same thread (pull mode).
 * No async callbacks and no new thread pools are introduced.
 *
 * <h3>Data format</h3>
 * The data exchange format crossing the JNI boundary is columnar IPC
 * (serialized stream format bytes). The native side serializes RowVectors
 * to columnar IPC in C++ and passes the raw bytes to Java. This is consistent
 * with the read path which also uses columnar IPC.
 *
 * <h3>Lifecycle contract</h3>
 * Every {@link #beginWrite} must be paired with exactly one
 * {@link #commitWrite} or {@link #abortWrite}. The native caller enforces
 * this via try-finally.
 */
public interface NativeWriter {

    /**
     * Begins a write transaction against the specified target table.
     *
     * @param descriptor write target metadata (table FQN, write mode,
     *                   column schema, connector-specific properties)
     * @return opaque write handle for subsequent operations
     */
    long beginWrite(WriteDescriptor descriptor);

    /**
     * Appends one serialized columnar IPC batch through a borrowed direct buffer.
     *
     * <p>The native side wraps its serialized Arrow IPC buffer as a
     * {@link java.nio.ByteBuffer} via JNI {@code NewDirectByteBuffer()},
     * eliminating a payload-sized byte-array copy in columnar implementations.
     * The ByteBuffer is only valid for the duration of this call — the
     * implementation must not retain references to it.</p>
     *
     * @param writeHandle handle returned by {@link #beginWrite}
     * @param data        direct ByteBuffer wrapping native Arrow IPC bytes
     * @param rowCount    number of rows in the batch
     * @param columnCount number of columns in the batch
     */
    void appendDirectBatch(long writeHandle, java.nio.ByteBuffer data,
                           int rowCount, int columnCount);

    /**
     * Commits the write transaction, making all appended data visible.
     *
     * @param writeHandle handle returned by {@link #beginWrite}
     * @return commit result with summary statistics
     */
    WriteCommitResult commitWrite(long writeHandle);

    /**
     * Aborts the write transaction, rolling back any partial writes
     * and releasing all associated resources.
     *
     * @param writeHandle handle returned by {@link #beginWrite}
     */
    void abortWrite(long writeHandle);
}
