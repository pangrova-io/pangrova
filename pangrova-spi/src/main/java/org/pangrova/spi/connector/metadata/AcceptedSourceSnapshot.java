package org.pangrova.spi.connector.metadata;

import org.pangrova.common.api.annotation.Experimental;

/** Immutable accepted manifest proof plus the current deployment authorized to execute it. */
@Experimental
public record AcceptedSourceSnapshot(ManagedSourceCatalog.Deployment deployment, String tenant,
        String tableId, long generation, String datasetUri, long version, String manifestRelativePath,
        byte[] manifestSha256, byte[] schemaSha256, long manifestSize, String namingScheme, String membershipId,
        long acceptedOriginRevision) {
    private static final int MAX_RELATIVE_PATH_LENGTH = 8192;
    private static final int ASCII_UPPER_BOUND = 127;

    public AcceptedSourceSnapshot {
        java.util.Objects.requireNonNull(deployment);
        if (acceptedOriginRevision <= 0 || acceptedOriginRevision > deployment.revision()) {
            throw new IllegalArgumentException("Accepted origin revision differs from current source deployment");
        }
        requireSourceIdentity(tenant, tableId, generation, datasetUri);
        if (version <= 0 || manifestSize <= 0 || manifestSize > 256L * 1024 * 1024
                || manifestSha256.length != 32 || schemaSha256.length != 32
                || !("V1".equals(namingScheme) || "V2".equals(namingScheme))
                || membershipId == null || !membershipId.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("Invalid accepted source snapshot");
        }
        requireRelativePath(manifestRelativePath);
        if (!manifestRelativePath.equals("_pangrova/prepared/" + membershipId + ".manifest")) {
            throw new IllegalArgumentException("Accepted source manifest path differs from its intent membership");
        }
        manifestSha256 = manifestSha256.clone();
        schemaSha256 = schemaSha256.clone();
    }
    @Override public byte[] manifestSha256() { return manifestSha256.clone(); }
    @Override public byte[] schemaSha256() { return schemaSha256.clone(); }

    private static void requireSourceIdentity(String tenant, String tableId, long generation, String datasetUri) {
        if (tenant == null || tenant.isBlank() || tableId == null || tableId.isBlank() || generation <= 0
                || datasetUri == null || datasetUri.isBlank()) {
            throw new IllegalArgumentException("Invalid accepted source snapshot");
        }
    }

    /** Validates the storage-relative path without decoding or normalizing an untrusted identity. */
    public static void requireRelativePath(String path) {
        if (path == null || path.isBlank() || path.length() > MAX_RELATIVE_PATH_LENGTH || path.startsWith("/")
                || path.contains("\\") || path.contains(":") || path.contains("%") || path.contains("?")
                || path.contains("#") || !path.chars().allMatch(value -> value >= 32 && value < ASCII_UPPER_BOUND)
                || java.util.Arrays.stream(path.split("/", -1))
                        .anyMatch(part -> part.isEmpty() || part.equals(".") || part.equals(".."))) {
            throw new IllegalArgumentException("Invalid source relative object path");
        }
    }
}
