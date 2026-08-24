package org.pangrova.spi.execution.interop;

import org.pangrova.spi.annotation.Experimental;

/**
 * Reference-counted handle to a native (C++) memory buffer that crosses
 * the JNI boundary.
 *
 * <h3>Problem this solves (G-INTEROP-01)</h3>
 *
 * <p>When columnar batches cross the JNI boundary via the columnar data interface,
 * the {@code native array struct release} callback is invoked by whichever thread drops
 * the last Java reference. If that callback runs C++ destructor logic on a
 * Java thread, the result is a SIGSEGV (the C++ runtime is not initialized on
 * that thread's stack). The current workaround copies the entire IPC payload
 * on every batch, defeating zero-copy.</p>
 *
 * <h3>Design (Gluten native-handle ownership model)</h3>
 *
 * <p>Instead of passing columnar data interface pointers with release callbacks
 * that run C++ destructors on Java threads, the native side retains ownership
 * of the underlying {@code RowVector}/{@code ColumnarBatch} and exposes a
 * {@code jlong} ticket (the handle) to Java. Java code operates on a
 * non-owning view of the data. When Java is done, it calls
 * {@link #release()} which posts a release request back to the native side
 * (via JNI), ensuring the C++ destructor runs on a C++ thread.</p>
 *
 * <p>This is the same pattern used by Gluten's {@code ColumnarBatchJniWrapper}
 * and the native engine's {@code NativeColumnarBatch}: C++ holds the buffer, Java holds
 * only a ticket, release is an explicit JNI call that queues destruction on
 * a native thread pool.</p>
 *
 * <h3>Reference counting</h3>
 *
 * <p>A handle starts with a reference count of 1 (at creation). Each
 * {@link #retain()} increments the count; each {@link #release()} decrements
 * it. When the count reaches 0 the native release callback is invoked on the
 * native side. Double-release (count already 0) throws
 * {@link IllegalStateException}.</p>
 *
 * <h3>Thread safety</h3>
 *
 * <p>Reference count operations ({@link #retain()}, {@link #release()}) must
 * be thread-safe. All other accessors ({@link #nativeHandle()},
 * {@link #bufferAddress()}, {@link #bufferLength()}) are safe to call from
 * any thread while the handle is alive (refcount &gt; 0).</p>
 *
 * @see NativeBufferOwnership
 * @see org.pangrova.spi.execution.NativeColumnarStream
 */
@Experimental(reason = "G-INTEROP-01: native-handle ownership model for zero-copy JNI transfer")
public interface NativeBufferHandle extends AutoCloseable {

    /**
     * Returns the opaque native handle (typically a {@code jlong} pointer or
     * ticket) identifying the native-side buffer.
     *
     * <p>This value is meaningful only to the native engine and must not be
     * interpreted or dereferenced by Java code.</p>
     *
     * @return the native handle value
     * @throws IllegalStateException if the handle has been fully released
     */
    long nativeHandle();

    /**
     * Returns the native memory address of the buffer data.
     *
     * <p>The returned address points to the raw columnar data (e.g., the
     * columnar record batch payload). Java code may read from this address using
     * {@code sun.misc.Unsafe} or {@code MemorySegment}
     * (JDK 22+) but must not write to it or free it.</p>
     *
     * @return native memory address of the data, or 0 if address is unavailable
     * @throws IllegalStateException if the handle has been fully released
     */
    long bufferAddress();

    /**
     * Returns the length of the buffer in bytes.
     *
     * @return buffer length in bytes
     * @throws IllegalStateException if the handle has been fully released
     */
    long bufferLength();

    /**
     * Returns the current reference count.
     *
     * <p>This is primarily for diagnostics and testing. Do not make
     * control-flow decisions based on this value in production code, as it
     * may change between the call and the decision.</p>
     *
     * @return current reference count, always &ge; 0
     */
    int refCount();

    /**
     * Increments the reference count by 1.
     *
     * <p>Call this when passing the handle to another consumer that will
     * independently call {@link #release()} when done.</p>
     *
     * @throws IllegalStateException if the handle has been fully released
     *                               (refcount is 0)
     */
    void retain();

    /**
     * Decrements the reference count by 1. When the count reaches 0, the
     * native-side release callback is invoked (on a native thread, not the
     * calling Java thread).
     *
     * <p>After the final release, all accessor methods throw
     * {@link IllegalStateException}.</p>
     *
     * @throws IllegalStateException if the reference count is already 0
     */
    void release();

    /**
     * Returns the ownership model governing this handle.
     *
     * @return the ownership model, never null
     */
    NativeBufferOwnership ownership();

    /**
     * Equivalent to {@link #release()}. Provided so that {@code NativeBufferHandle}
     * can be used in try-with-resources blocks.
     */
    @Override
    default void close() {
        release();
    }
}
