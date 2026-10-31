package org.pangrova.worker.nativebridge.jni;

/**
 * Exception thrown from the native (C++) side across the JNI boundary.
 * The native code locates this class via {@code FindClass} and constructs
 * instances with the single-arg {@code (String)} constructor.
 *
 * <p>Extends {@link NativeRuntimeException} so that callers catching
 * {@code NativeRuntimeException} also catch JNI bridge errors.
 */
public class NativeBridgeException extends NativeRuntimeException {

    public NativeBridgeException(String message) {
        super(message);
    }
}
