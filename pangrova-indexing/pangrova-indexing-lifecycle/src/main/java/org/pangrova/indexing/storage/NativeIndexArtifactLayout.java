package org.pangrova.indexing.storage;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.crypto.Sha256Digests;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;

/**
 * Computes durable Native index artifact locations and job keys without storage access.
 *
 * <p>Existing v2 paths and legacy job keys are persisted identities. New attempts must remain
 * outside the legacy generation prefix so retiring one attempt cannot delete another.</p>
 */
@Internal
public final class NativeIndexArtifactLayout {

    public static final String LEGACY_ATTEMPT_ID = "submission";
    private final String baseArtifactUri;

    /** Creates the Native layout rooted at the configured shared artifact URI. */
    public NativeIndexArtifactLayout(String baseArtifactUri) {
        if (baseArtifactUri == null || baseArtifactUri.isBlank()) {
            throw new IllegalArgumentException("baseArtifactUri must not be blank");
        }
        this.baseArtifactUri = stripTrailingSlash(baseArtifactUri);
    }

    /** Derives one immutable identity shared by submission, storage, and authority recovery. */
    public static AttemptIdentity attempt(String tenantId, IndexDescriptor descriptor, String attemptId) {
        return new AttemptIdentity(Sha256Digests.sha256Hex(tenantId).substring(0, 32),
                safeComponent(descriptor.indexId()), descriptor.generation(), safeComponent(attemptId));
    }

    /** Returns the exact v2 staging URI for an admitted attempt, including legacy layout compatibility. */
    public String stagingUri(AttemptIdentity identity) {
        String base = baseArtifactUri + "/v2/tenants/" + identity.tenantScope
                + "/artifacts/" + identity.indexId;
        String generation = "/generation-" + identity.generation;
        return identity.isLegacy() ? base + generation
                : base + "/attempt-" + identity.attemptId + generation;
    }

    /** Durable job and idempotency keys derived from the same immutable attempt coordinates. */
    public static final class AttemptIdentity {
        private final String tenantScope;
        private final String indexId;
        private final int generation;
        private final String attemptId;

        private AttemptIdentity(String tenantScope, String indexId, int generation, String attemptId) {
            this.tenantScope = tenantScope;
            this.indexId = indexId;
            this.generation = generation;
            this.attemptId = attemptId;
        }

        /** Returns the persisted generation job key for this attempt. */
        public String jobId() {
            String base = "index-build:tenant-" + tenantScope + ":" + indexId + ":generation:" + generation;
            return isLegacy() ? base : base + ":attempt:" + attemptId;
        }

        /** Returns the historical or current idempotency key for the exact snapshot. */
        public String idempotencyKey(String sourceSnapshot) {
            return jobId() + ":snapshot:" + (isLegacy() ? sourceSnapshot : Sha256Digests.sha256Hex(sourceSnapshot));
        }

        private boolean isLegacy() {
            return LEGACY_ATTEMPT_ID.equals(attemptId);
        }
    }

    private static String safeComponent(String value) {
        if (value == null || value.isBlank() || value.contains("..")
                || value.contains("/") || value.contains("\\") || value.indexOf('\0') >= 0) {
            throw new PangrovaException(PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR,
                    "Artifact path identity contains unsafe characters");
        }
        return value;
    }

    private static String stripTrailingSlash(String value) {
        String result = value;
        while (result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        if (result.isBlank()) {
            throw new IllegalArgumentException("baseArtifactUri must not be a filesystem root");
        }
        return result;
    }
}
