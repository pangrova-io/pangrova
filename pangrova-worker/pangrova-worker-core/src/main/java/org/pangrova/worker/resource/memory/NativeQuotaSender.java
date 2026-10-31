package org.pangrova.worker.resource.memory;

/**
 * Sends tenant memory quota updates from Java control plane to Native execution domain.
 *
 * <p>Implementations typically invoke a JNI method that serializes a
 * setTenantQuota proto message and delivers it to the native memory manager.
 *
 * <p>This is a single-responsibility contract: push quota bytes for a tenant.
 * The actual JNI call is implemented in the worker module; this interface
 * allows the resource module to remain decoupled from JNI details.
 */
public interface NativeQuotaSender {

    /**
     * Pushes a tenant quota update to the native execution domain.
     *
     * @param tenantId tenant identifier (must not be null or blank)
     * @param bytes    new quota in bytes (must be positive)
     * @throws IllegalArgumentException if tenantId is null/blank or bytes is non-positive
     * @throws NativeQuotaSendException if the native call fails
     */
    void setTenantQuota(String tenantId, long bytes);

    /**
     * Thrown when the native quota push fails (JNI error, native side unavailable, etc.).
     */
    final class NativeQuotaSendException extends RuntimeException {

        public NativeQuotaSendException(String tenantId, long bytes, Throwable cause) {
            super("Failed to push quota to native: tenantId=" + tenantId + " bytes=" + bytes, cause);
        }
    }
}
