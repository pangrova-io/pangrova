package org.pangrova.connector.api.metadata;

import org.pangrova.common.crypto.Sha256Digests;

import org.pangrova.common.api.annotation.Experimental;
import java.security.MessageDigest;
import java.util.List;

/** Exact immutable object deletion set already marked by the sole catalog authority. */
@Experimental
public record SourceCleanupBatch(String executionId, String fingerprint, String cleanupId,
        String datasetUri, List<SourceObjectPage.ObjectReference> objects, byte[] sha256) {
    public SourceCleanupBatch {
        objects = List.copyOf(objects);
        sha256 = sha256.clone();
        if (executionId == null || executionId.isBlank() || !Sha256Digests.isSha256Hex(fingerprint)
                || cleanupId == null || !cleanupId.matches("[0-9a-f]{32}") || datasetUri == null || datasetUri.isBlank()
                || objects.isEmpty() || objects.size() > SourceObjectPage.MAX_PAGE_OBJECTS
                || !MessageDigest.isEqual(sha256, SourceObjectPage.digest(objects))) {
            throw new IllegalArgumentException("Invalid authoritative object cleanup batch");
        }
        String previous = "";
        for (var object : objects) {
            if (object.relativePath().compareTo(previous) <= 0) {
                throw new IllegalArgumentException("Cleanup objects are not exact ordered identities");
            }
            previous = object.relativePath();
        }
    }
    @Override public byte[] sha256() { return sha256.clone(); }
}
