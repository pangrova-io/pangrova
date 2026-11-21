package org.pangrova.worker.exception;

/**
 * Thrown when the native quota push fails (JNI error, native side unavailable, etc.).
 */
public final class NativeQuotaSendException extends RuntimeException {

    public NativeQuotaSendException(String tenantId, long bytes, Throwable cause) {
        super("Failed to push quota to native: tenantId=" + tenantId + " bytes=" + bytes, cause);
    }
}
