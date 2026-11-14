package org.pangrova.persistence.file.api.copy;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.file.CopySourceStagingDigest;
import org.pangrova.file.api.profile.StorageProfileRevision;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.security.api.identity.UserIdentity;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static org.pangrova.persistence.file.api.copy.CopySourceAdmissionValidation.decodeNonce;
import static org.pangrova.persistence.file.api.copy.CopySourceAdmissionValidation.isManagedStorageRootUri;
import static org.pangrova.persistence.file.api.copy.CopySourceAdmissionValidation.isManagedObjectUri;
import static org.pangrova.persistence.file.api.copy.CopySourceAdmissionValidation.isSha256;
import static org.pangrova.persistence.file.api.copy.CopySourceAdmissionValidation.requireText;

/**
 * Atomic authority for one exact COPY source read and its immutable staged result.
 * Implementations must support concurrent callers and recheck current tenant,
 * profile, READ permission and execution ownership within each atomic operation.
 * A prior compiler observation or a valid nonce alone is not current authorization.
 *
 * <p>Calls are synchronous. A failed/timeout response does not prove rollback or
 * cancel an accepted operation; callers must confirm the exact admission before
 * retrying or deleting staged data. Storage failures must not become absence or a
 * Memory fallback. The assembler owns backend resources; callers do not close them.</p>
 */
@Experimental
public interface CopySourceReadAdmissions {
    /** Issues a fresh private capability only after current authorization and fencing checks. */
    Ticket issue(Scope scope, UserIdentity identity, ExecutionAnchor expectedAnchor);

    /**
     * Derives a final attempt from a durable staged root under current authority.
     * An exact retry may produce another nonce for the same immutable input;
     * it must not re-read the source or substitute a newer source/profile version.
     */
    FrozenInput issueFrozenInput(FrozenInputAttempt attempt, ExecutionAnchor expectedAnchor);

    /** Redeems an exact live attempt; repeated redemption returns its established staged proof, if any. */
    Redemption redeem(Ticket ticket, Scope presented, ExecutionAnchor expectedAnchor);

    /** Records an immutable result; an identical repeat is accepted and conflicting evidence is rejected. */
    StagedSource recordStagedSource(Ticket ticket, Scope presented,
            StagedSource staged, ExecutionAnchor expectedAnchor);

    /** The nonce itself is a bearer capability and must only travel in a private task payload. */
    @Experimental
    record Ticket(String nonce) {
        public Ticket {
            decodeNonce(nonce);
        }

        @Override public String toString() {
            return "CopySourceReadTicket[REDACTED]";
        }
    }

    /** Exact staging command, issued before the later Native TaskRequest exists. */
    @Experimental
    record Scope(String tenantId, String principal, String profileName,
                        String profileIncarnationId, long profileRevision,
                        String canonicalSourceUrl, String managedStorageRootUri,
                        String managedBackendIdentitySha256,
                        String executionId, String stagingAttemptId,
                        String ownerId, String ownerIncarnation, long controlEpoch,
                        long ownerFencingToken, long deadlineEpochMs) {
        public Scope {
            requireText(tenantId, "tenant");
            requireText(principal, "principal");
            requireText(profileName, "profile");
            UUID.fromString(profileIncarnationId);
            requireText(canonicalSourceUrl, "source URL");
            if (!isManagedStorageRootUri(managedStorageRootUri)) {
                throw new IllegalArgumentException("COPY managed storage root is not canonical");
            }
            if (!isSha256(managedBackendIdentitySha256)) {
                throw new IllegalArgumentException("COPY managed backend identity is incomplete");
            }
            requireText(executionId, "execution");
            requireText(stagingAttemptId, "staging attempt");
            requireText(ownerId, "owner");
            requireText(ownerIncarnation, "owner incarnation");
            if (profileRevision <= 0 || controlEpoch <= 0 || ownerFencingToken <= 0
                    || deadlineEpochMs <= 0) {
                throw new IllegalArgumentException("COPY source staging authority is incomplete");
            }
        }
    }

    /** Verified exact S3 version and managed content identity; no source credentials. */
    @Experimental
    record StagedSource(String sourceVersionId, String managedObjectUri, String contentSha256,
                               long contentSize, String tenantScope) {
        public StagedSource {
            if (sourceVersionId == null || sourceVersionId.isBlank()
                    || "null".equals(sourceVersionId)
                    || sourceVersionId.getBytes(StandardCharsets.UTF_8).length > 1024
                    || !isSha256(contentSha256) || contentSize < 0 || !isSha256(tenantScope)
                    || !isManagedObjectUri(managedObjectUri, tenantScope, contentSha256)) {
                throw new IllegalArgumentException("COPY staged source proof is incomplete");
            }
        }
    }

    /** A repeated exact attempt can reuse the durable staged proof without re-reading S3. */
    @Experimental
    record Redemption(StorageProfileRevision profile, Optional<StagedSource> stagedSource) {
        public Redemption {
            Objects.requireNonNull(profile, "profile");
            Objects.requireNonNull(stagedSource, "stagedSource");
        }
    }

    /** Final execution identity; source/profile/content are loaded only from the durable root proof. */
    @Experimental
    record FrozenInputAttempt(String tenantId, String principal, String executionId,
                                     String sourceAttemptId, String taskId, long controlEpoch,
                                     long deadlineEpochMs) {
        public FrozenInputAttempt {
            requireText(tenantId, "tenant");
            requireText(principal, "principal");
            requireText(executionId, "execution");
            requireText(sourceAttemptId, "source attempt");
            requireText(taskId, "task");
        }
    }

    /** Fresh private capability and its immutable inherited input. */
    @Experimental
    record FrozenInput(Ticket ticket, Scope scope, StagedSource proof) {}

    /** Length-prefixed, versioned command encoding excludes the bearer nonce. */
    static String commandSha256(Scope scope) {
        Objects.requireNonNull(scope, "scope");
        return CopySourceStagingDigest.sha256(new CopySourceStagingDigest.Scope(
                scope.tenantId(), scope.principal(), scope.profileName(),
                scope.profileIncarnationId(), scope.profileRevision(), scope.canonicalSourceUrl(),
                scope.managedStorageRootUri(), scope.managedBackendIdentitySha256(),
                scope.executionId(), scope.stagingAttemptId(),
                scope.ownerId(), scope.ownerIncarnation(), scope.controlEpoch(),
                scope.ownerFencingToken(), scope.deadlineEpochMs()));
    }

}
