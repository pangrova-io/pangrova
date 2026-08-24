package org.pangrova.worker.lance;

import org.pangrova.common.format.NativeFormatCapabilities;
import org.pangrova.worker.jni.NativeLibraryLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initializes the Lance C FFI bridge and manages the singleton Tokio runtime.
 *
 * <p>Called during Worker bootstrap after {@link NativeLibraryLoader#load()}.
 * The Tokio runtime is shared across all Lance FFI operations (reader, writer,
 * vector index) to avoid per-call runtime creation overhead.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>{@link #initialize()} — creates Tokio runtime (once)</li>
 *   <li>Worker operates — all Lance calls share the runtime</li>
 *   <li>{@link #shutdown()} — drops runtime, blocks until async tasks drain</li>
 * </ol>
 *
 * <p>Thread safety: double-checked locking with volatile flag. Idempotent.
 */
public final class LanceFfiBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(LanceFfiBootstrap.class);

    private static volatile boolean initialized;
    private static volatile long runtimeHandle;

    private LanceFfiBootstrap() {}

    /**
     * Initialize the Lance FFI bridge. Creates a singleton Tokio multi-thread
     * runtime in the native layer. Idempotent — safe to call multiple times.
     *
     * @throws IllegalStateException if native library is not loaded
     */
    public static void initialize() {
        if (initialized) {
            return;
        }
        synchronized (LanceFfiBootstrap.class) {
            if (initialized) {
                return;
            }
            if (!NativeLibraryLoader.isAvailable()) {
                LOG.warn("event=lance_ffi_skip reason=native_library_unavailable");
                return;
            }
            try {
                runtimeHandle = nativeCreateTokioRuntime();
                if (runtimeHandle == 0) {
                    LOG.error("event=lance_ffi_init_failed reason=runtime_handle_null");
                    return;
                }
                initialized = true;
                NativeFormatCapabilities.setAvailable(true);
                LOG.info("event=lance_ffi_initialized runtime_handle={}", runtimeHandle);
            } catch (UnsatisfiedLinkError linkError) {
                LOG.warn("event=lance_ffi_skip reason=lance_symbols_missing msg={}",
                        linkError.getMessage());
            }
        }
    }

    /**
     * Returns true if the Lance FFI bridge is initialized and ready.
     */
    public static boolean isAvailable() {
        return initialized && runtimeHandle != 0;
    }

    /**
     * Returns the opaque runtime handle for use by {@link LanceFfiBridge} methods.
     *
     * @throws IllegalStateException if not initialized
     */
    static long getRuntimeHandle() {
        if (!initialized || runtimeHandle == 0) {
            throw new IllegalStateException(
                    "Lance FFI not initialized. Call LanceFfiBootstrap.initialize() first.");
        }
        return runtimeHandle;
    }

    /**
     * Shutdown the Lance FFI bridge. Drops the Tokio runtime and blocks until
     * all in-flight async tasks complete. Called during worker graceful shutdown.
     */
    public static void shutdown() {
        if (!initialized) {
            return;
        }
        synchronized (LanceFfiBootstrap.class) {
            if (!initialized) {
                return;
            }
            if (runtimeHandle != 0) {
                nativeShutdownTokioRuntime(runtimeHandle);
                runtimeHandle = 0;
            }
            initialized = false;
            NativeFormatCapabilities.setAvailable(false);
            LOG.info("event=lance_ffi_shutdown");
        }
    }

    // -- Native methods (implemented in libpangrova_native via JNI) --

    private static native long nativeCreateTokioRuntime();

    private static native void nativeShutdownTokioRuntime(long handle);
}
