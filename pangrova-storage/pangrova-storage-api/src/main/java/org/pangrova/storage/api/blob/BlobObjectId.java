package org.pangrova.storage.api.blob;

/** Tenant-scoped content identity and exact length; contains no path, credentials or authority. */
public record BlobObjectId(String tenantScope, String checksum, long size) {
    public BlobObjectId {
        requireDigest(tenantScope, "tenantScope");
        requireDigest(checksum, "checksum");
        if (size < 0) throw new IllegalArgumentException("Blob size must not be negative");
    }

    private static void requireDigest(String value, String name) {
        if (value == null || !value.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException(name + " must be a lowercase SHA-256 token");
        }
    }
}
