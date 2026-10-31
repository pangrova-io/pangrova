package org.pangrova.protocol.rest.copy;

import org.pangrova.common.file.CopySourceStagingDigest;
import org.pangrova.common.storage.StorageType;

import java.util.Objects;

/** Private Worker task RPC for one admitted, exact-version COPY source read. */
public final class CopySourceReadHttp {
    private CopySourceReadHttp() {}

    /** Immutable admission scope; the Coordinator compares every field to its durable row. */
    public record Scope(String tenantId, String principal, String profileName,
                        String profileIncarnationId, long profileRevision,
                        String canonicalSourceUrl, String managedStorageRootUri,
                        String managedBackendIdentitySha256,
                        String executionId, String stagingAttemptId,
                        String ownerId, String ownerIncarnation, long controlEpoch,
                        long ownerFencingToken, long deadlineEpochMs) {
        /** Exact nonce-free digest persisted by Coordinator admission. */
        public String commandSha256() {
            return CopySourceStagingDigest.sha256(new CopySourceStagingDigest.Scope(
                    tenantId, principal, profileName, profileIncarnationId, profileRevision,
                    canonicalSourceUrl, managedStorageRootUri, managedBackendIdentitySha256,
                    executionId, stagingAttemptId,
                    ownerId, ownerIncarnation, controlEpoch, ownerFencingToken, deadlineEpochMs));
        }
    }

    /** Bearer ticket must travel only in the task-private payload. */
    public record RedeemRequest(String nonce, Scope scope) {
        public RedeemRequest {
            Objects.requireNonNull(nonce, "nonce");
            Objects.requireNonNull(scope, "scope");
        }

        @Override public String toString() {
            return "CopySourceReadRedeemRequest[REDACTED]";
        }
    }

    /** Source version and managed content identity, with no source credentials. */
    public record StagedSource(String sourceVersionId, String managedObjectUri,
                               String contentSha256, long contentSize, String tenantScope) {}

    /** Minimal source location needed by the Worker; no credentials or arbitrary options. */
    public record Profile(String name, StorageType type, String bucket, String prefix,
                          String endpoint, String region, boolean pathStyleAccess,
                          String incarnationId, long revision) {}

    /** Result of a current, fenced redemption. A null stagedSource means stage once. */
    public record RedeemResponse(Profile profile, StagedSource stagedSource) {}

    /** Records the first verified managed content identity for this exact attempt. */
    public record RecordRequest(String nonce, Scope scope, StagedSource stagedSource) {
        public RecordRequest {
            Objects.requireNonNull(nonce, "nonce");
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(stagedSource, "stagedSource");
        }

        @Override public String toString() {
            return "CopySourceReadRecordRequest[REDACTED]";
        }
    }
}
