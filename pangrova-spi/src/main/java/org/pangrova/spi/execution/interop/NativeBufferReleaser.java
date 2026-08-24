package org.pangrova.spi.execution.interop;

import org.pangrova.spi.annotation.Experimental;

/**
 * Callback interface for releasing native buffers on the native (C++) side.
 *
 * <p>Implementations of this interface are provided by the native engine
 * integration layer. When a {@link NativeBufferHandle}'s reference count
 * reaches zero, the handle invokes this releaser to post the destruction
 * request back to the native side via JNI.</p>
 *
 * <p>The key safety invariant is: the {@link #releaseNativeBuffer(long)}
 * method must not run C++ destructors on the calling Java thread. Instead,
 * it must post the release to a native thread (e.g., the native driver thread
 * pool) and return immediately. This decoupling is what prevents the
 * SIGSEGV that occurs when columnar data interface release callbacks run C++ destructors
 * on Java threads.</p>
 *
 * <h3>Implementation guidance</h3>
 *
 * <p>A typical JNI implementation:</p>
 * <pre>
 *   // Java side
 *   public void releaseNativeBuffer(long nativeHandle) {
 *       nativeReleaseBatch(nativeHandle);  // JNI call
 *   }
 *
 *   // C++ side (JNI_OnLoad registered)
 *   void Java_..._nativeReleaseBatch(JNIEnv*, jclass, jlong handle) {
 *       // Post to driver pool -- DO NOT destroy inline
 *       driverPool->enqueue([handle] {
 *           auto* batch = reinterpret_cast&lt;ColumnarBatch*&gt;(handle);
 *           delete batch;
 *       });
 *   }
 * </pre>
 *
 * @see NativeBufferHandle
 */
@Experimental(reason = "G-INTEROP-01: native-side release callback for safe buffer lifecycle")
@FunctionalInterface
public interface NativeBufferReleaser {

    /**
     * Requests release of the native buffer identified by the given handle.
     *
     * <p>This method must be safe to call from any Java thread. The actual
     * C++ destruction must happen on a native thread, not the calling thread.</p>
     *
     * <p>Implementations must be idempotent: calling with an already-released
     * handle is a no-op (not an error).</p>
     *
     * @param nativeHandle the opaque native handle identifying the buffer
     */
    void releaseNativeBuffer(long nativeHandle);
}
