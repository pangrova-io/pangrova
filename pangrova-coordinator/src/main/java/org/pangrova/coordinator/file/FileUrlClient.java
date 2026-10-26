package org.pangrova.coordinator.file;

/**
 * Asks the Worker named by a grant to mint a provider-signed URL.
 *
 * <p>Only the Worker holds storage credentials, so the Coordinator cannot sign a URL
 * itself; it forwards the short-lived descriptor instead of the credential.</p>
 */
@FunctionalInterface
public interface FileUrlClient {

    /**
     * @param read       {@code true} for a download URL, {@code false} for an upload URL
     * @param ttlSeconds requested lifetime; the Worker clamps it to its own bounds
     */
    FileUrl presign(FileSpaceAccessService.FileAccessGrant grant, boolean read, long ttlSeconds);

    /** Signed provider URL and the epoch millisecond at which it stops working. */
    record FileUrl(String url, long expiresAtEpochMillis) {
        /** Omits bearer credentials from diagnostics. */
        @Override public String toString() { return "FileUrl[expiresAtEpochMillis=" + expiresAtEpochMillis + "]"; }
    }
}
