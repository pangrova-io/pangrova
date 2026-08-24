package org.pangrova.worker.jni;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Exception thrown by NativeRuntime when JNI calls encounter errors.
 */
public class NativeRuntimeException extends PangrovaException {

    /**
     * Convenience constructor used by JNI {@code ThrowNew} which only passes a message string.
     */
    public NativeRuntimeException(String message) {
        super(PangrovaErrorCode.NATIVE_JNI_EXCEPTION, message);
    }

    public NativeRuntimeException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public NativeRuntimeException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
