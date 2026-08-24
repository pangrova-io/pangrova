package org.pangrova.spi.execution;

import org.pangrova.spi.annotation.Experimental;

/**
 * Abstracts the JNI boundary for columnar data transfer between the Java
 * control plane and the native (C++) execution engine using the C Stream
 * Interface semantics (per the columnar data interface standard ABI).
 *
 * <p>This interface is the migration target for the current hand-written
 * per-batch JNI pull ABI ({@link InteropMode#LEGACY_JNI_PULL}). Instead of
 * manually aligning two protobuf schemas and transferring serialized byte
 * arrays with a status-byte prefix, implementations of this interface
 * exchange data via the standard columnar data interface structs
 * (native array struct, native schema struct, native stream struct)
 * passed as native memory addresses across JNI.</p>
 *
 * <h3>Design rationale</h3>
 *
 * <p>The C Stream Interface defines a stable C ABI for streaming columnar data
 * between independently compiled modules. By adopting this standard:</p>
 * <ol>
 *   <li>The dual-proto alignment burden disappears — the Substrait plan
 *       carried in {@code TaskRequest.plan_fragment} becomes the single
 *       schema source of truth.</li>
 *   <li>Batch transfer becomes zero-copy: the native engine exposes
 *       native array struct pointers that Java consumes via
 *       {@link java.lang.foreign.MemorySegment} (JDK 22+) or
 *       {@code sun.misc.Unsafe} (JDK 17).</li>
 *   <li>The ABI is engine-agnostic — multiple native engines implement it,
 *       enabling future engine pluggability.</li>
 * </ol>
 *
 * <h3>Lifecycle</h3>
 *
 * <pre>
 *   stream = factory.create(executorHandle);
 *   schema = stream.getSchema();           // native schema struct pointer
 *   while (stream.hasNext()) {
 *       long arrayPtr = stream.nextBatch(); // native array struct pointer
 *       // ... consume the batch ...
 *       stream.releaseBatch(arrayPtr);
 *   }
 *   stream.close();
 * </pre>
 *
 * <h3>Thread safety</h3>
 *
 * <p>Implementations must be safe for single-threaded sequential access.
 * Concurrent access from multiple threads is not required by the contract.</p>
 *
 * @see InteropMode#NATIVE_C_STREAM
 */
@Experimental(reason = "G-INTEROP-07: migration target for legacy JNI pull ABI")
public interface NativeColumnarStream extends AutoCloseable {

    /**
     * Returns the native memory address of the native schema struct
     * describing the columnar layout of batches produced by this stream.
     *
     * <p>The schema is valid for the lifetime of this stream instance.
     * Callers must not free the returned pointer; it is released when
     * {@link #close()} is called.</p>
     *
     * @return native pointer to the native schema struct
     * @throws IllegalStateException if the stream is closed
     */
    long getSchema();

    /**
     * Returns {@code true} if there are more batches available in the stream.
     *
     * <p>This method may block if the native engine has not yet produced
     * the next batch. It returns {@code false} when the stream is exhausted
     * (EOF) or the task has failed.</p>
     *
     * @return {@code true} if {@link #nextBatch()} will return a valid pointer
     * @throws IllegalStateException if the stream is closed
     */
    boolean hasNext();

    /**
     * Returns the native memory address of the next native array struct
     * containing a columnar record batch.
     *
     * <p>The caller must eventually call {@link #releaseBatch(long)} with the
     * returned pointer to free the native memory.</p>
     *
     * @return native pointer to the native array struct
     * @throws java.util.NoSuchElementException if no more batches are available
     * @throws IllegalStateException if the stream is closed
     * @throws NativeExecutionException if the native engine reports an error
     */
    long nextBatch();

    /**
     * Releases the native memory associated with a batch previously returned
     * by {@link #nextBatch()}.
     *
     * <p>After this call the pointer is invalid and must not be dereferenced.
     * Double-release is a no-op.</p>
     *
     * @param batchPtr pointer previously returned by {@link #nextBatch()}
     */
    void releaseBatch(long batchPtr);

    /**
     * Returns the interop mode this stream operates in.
     *
     * @return {@link InteropMode#NATIVE_C_STREAM}
     */
    default InteropMode interopMode() {
        return InteropMode.NATIVE_C_STREAM;
    }

    /**
     * Releases all native resources held by this stream, including the
     * schema and any unreleased batches.
     *
     * <p>After close, all methods throw {@link IllegalStateException}.
     * Idempotent — calling close on an already-closed stream is a no-op.</p>
     */
    @Override
    void close();

    /**
     * Exception thrown when the native execution engine reports an error
     * during columnar stream operations.
     */
    class NativeExecutionException extends RuntimeException {

        public NativeExecutionException(String message) {
            super(message);
        }

        public NativeExecutionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
