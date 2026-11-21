package org.pangrova.worker.exception;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when a native JNI call returns a negative error code,
 * indicating a C++ execution failure (R7.9).
 *
 * <p>Distinct from {@link NativeRuntimeException} which covers JNI-level
 * failures (e.g. stale handle, library unavailable). This exception
 * specifically represents native engine execution errors signaled
 * via negative return codes from batch-pull operations.</p>
 */
public class NativeExecutionException extends PangrovaException {

    private final int nativeErrorCode;

    public NativeExecutionException(int nativeErrorCode) {
        super(PangrovaErrorCode.NATIVE_JNI_EXCEPTION,
                "Native execution failed with error code: " + nativeErrorCode);
        this.nativeErrorCode = nativeErrorCode;
    }

    public NativeExecutionException(int nativeErrorCode, String message) {
        super(PangrovaErrorCode.NATIVE_JNI_EXCEPTION, message);
        this.nativeErrorCode = nativeErrorCode;
    }

    /** Returns the negative error code from the native engine. */
    public int nativeErrorCode() {
        return nativeErrorCode;
    }
}
