package org.pangrova.spi.execution.interop;

import org.pangrova.spi.annotation.Experimental;

/**
 * Defines the ownership model for native buffers crossing the JNI boundary.
 *
 * <h3>Background (G-INTEROP-01)</h3>
 *
 * <p>The columnar data interface includes a {@code release} callback on
 * the native array struct. When Java code holds an imported array and drops
 * its last reference, the JVM invokes this callback. If the callback contains
 * C++ destructor logic, it executes on a Java thread where the C++ runtime
 * may not be initialized, causing SIGSEGV.</p>
 *
 * <p>This enum models the three strategies for handling this ownership
 * boundary, from least safe to safest:</p>
 *
 * <ol>
 *   <li>{@link #COPIED} -- full IPC copy; safe but defeats zero-copy.</li>
 *   <li>{@link #JAVA_RELEASE_CALLBACK} -- zero-copy with release callback
 *       on the Java side; fast but SIGSEGV-prone.</li>
 *   <li>{@link #NATIVE_OWNED_HANDLE} -- C++ retains ownership, Java holds
 *       a non-owning view via {@link NativeBufferHandle}; both safe and
 *       zero-copy. This is the target model.</li>
 * </ol>
 *
 * <h3>Industry precedent</h3>
 *
 * <ul>
 *   <li><b>Gluten:</b> {@code ColumnarBatchJniWrapper} -- C++ holds
 *       {@code ColumnarBatch}, Java holds jlong handle, explicit native
 *       release via {@code nativeReleaseBatch(handle)}.</li>
 *   <li><b>Native engine bridge:</b> {@code NativeColumnarBatch} -- same pattern,
 *       release queued on the native driver thread pool.</li>
 *   <li><b>DataFusion (via C FFI):</b> reference-counted
 *       native array struct with release running on the producing thread's
 *       runtime.</li>
 * </ul>
 *
 * @see NativeBufferHandle
 * @see org.pangrova.spi.execution.NativeColumnarStream
 */
@Experimental(reason = "G-INTEROP-01: ownership model for native buffers crossing JNI")
public enum NativeBufferOwnership {

    /**
     * Full IPC serialization copy across the JNI boundary.
     *
     * <p>The native engine serializes batches into IPC byte arrays,
     * which are copied into Java heap memory. No native pointers cross the
     * boundary, so there is no release-callback risk. This is the current
     * production fallback but defeats zero-copy performance.</p>
     *
     * <p>Corresponds to the status-byte-prefixed {@code byte[]} returned by
     * the legacy {@code getNextBatch} JNI call.</p>
     */
    COPIED,

    /**
     * Zero-copy import via C Data Interface with release callback
     * on the Java side.
     *
     * <p><b>UNSAFE in the Pangrova architecture.</b> The release callback
     * runs C++ destructor logic on a Java thread, causing SIGSEGV when the
     * C++ runtime is not initialized on that thread. This mode exists only
     * to document the anti-pattern; it must not be used in production.</p>
     *
     * <p>This is the mode that task 49 (G-INTEROP-01) was created to replace.</p>
     */
    JAVA_RELEASE_CALLBACK,

    /**
     * Zero-copy with native-owned handle (Gluten model).
     *
     * <p>The native engine retains full ownership of the underlying
     * {@code RowVector}/{@code ColumnarBatch}. Java receives a
     * {@link NativeBufferHandle} containing:</p>
     * <ul>
     *   <li>An opaque {@code jlong} native handle (ticket)</li>
     *   <li>A direct memory address for zero-copy read access</li>
     *   <li>A reference count managed via {@link NativeBufferHandle#retain()}
     *       and {@link NativeBufferHandle#release()}</li>
     * </ul>
     *
     * <p>When Java calls {@link NativeBufferHandle#release()}, the release
     * request is posted to the native side via JNI, ensuring the C++
     * destructor runs on a native thread (typically the native driver thread
     * pool). This is both safe and zero-copy.</p>
     *
     * <p>This is the target ownership model for all data crossing the
     * JNI boundary.</p>
     */
    NATIVE_OWNED_HANDLE;

    /**
     * Returns whether this ownership model provides zero-copy transfer.
     *
     * @return true if data is not copied across the JNI boundary
     */
    public boolean isZeroCopy() {
        return this != COPIED;
    }

    /**
     * Returns whether this ownership model is safe from SIGSEGV caused by
     * running C++ destructors on Java threads.
     *
     * @return true if release callbacks are guaranteed to run on native threads
     */
    public boolean isReleaseSafe() {
        return this != JAVA_RELEASE_CALLBACK;
    }

    /**
     * Returns the recommended ownership model for production use.
     *
     * @return {@link #NATIVE_OWNED_HANDLE}
     */
    public static NativeBufferOwnership recommended() {
        return NATIVE_OWNED_HANDLE;
    }
}
