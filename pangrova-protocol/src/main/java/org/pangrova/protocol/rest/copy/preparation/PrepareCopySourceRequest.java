package org.pangrova.protocol.rest.copy.preparation;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.protocol.rest.copy.CopySourceReadHttp;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.SourcePreparationRequest;

import java.security.MessageDigest;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Objects;

/** Private preparation envelope. No relational task or source credentials are carried here. */
public final class PrepareCopySourceRequest {
    private final SourcePreparationRequest preparation;
    private final byte[] canonicalBytes;
    private final CopySourceReadHttp.RedeemRequest sourceRead;

    /** Decodes bounded canonical preparation bytes and binds the private source authority. */
    @JsonCreator
    public PrepareCopySourceRequest(@JsonProperty(value = "canonical_preparation", required = true) String encoded,
                                    @JsonProperty(value = "copy_source_read", required = true)
                                    CopySourceReadHttp.RedeemRequest sourceRead) {
        this.sourceRead = Objects.requireNonNull(sourceRead, "sourceRead");
        if (encoded == null || encoded.length() > 64 * 1024) {
            throw new IllegalArgumentException("Source preparation envelope exceeds its control bound");
        }
        try {
            canonicalBytes = Base64.getDecoder().decode(Objects.requireNonNull(encoded, "canonical preparation"));
            preparation = SourcePreparationRequest.parseFrom(canonicalBytes);
        } catch (InvalidProtocolBufferException | IllegalArgumentException invalid) {
            throw new IllegalArgumentException("Malformed canonical source preparation", invalid);
        }
        CanonicalTaskRequests.validateSourcePreparation(preparation);
        requireScopeBinding();
    }

    public static PrepareCopySourceRequest from(SourcePreparationRequest preparation,
                                               CopySourceReadHttp.RedeemRequest sourceRead) {
        return new PrepareCopySourceRequest(Base64.getEncoder().encodeToString(preparation.toByteArray()), sourceRead);
    }

    @JsonProperty("canonical_preparation")
    public String encodedPreparation() { return Base64.getEncoder().encodeToString(canonicalBytes); }
    @JsonProperty("copy_source_read")
    public CopySourceReadHttp.RedeemRequest sourceRead() { return sourceRead; }
    public SourcePreparationRequest preparation() { return preparation; }
    public byte[] canonicalBytes() { return canonicalBytes.clone(); }

    /** Same exact admission tombstone as relational tasks, over this command's own canonical bytes. */
    public org.pangrova.protocol.v2.TaskAdmissionSealRequest admissionSeal() {
        return org.pangrova.protocol.v2.TaskAdmissionSealRequest.newBuilder().setProtocol(preparation.getProtocol())
                .setAttempt(preparation.getAttempt()).setDeadlineEpochMs(preparation.getDeadlineEpochMs())
                .setCanonicalRequestChecksum(ProtocolChecksums.sha256(canonicalBytes)).build();
    }

    @Override public String toString() { return "PrepareCopySourceRequest[REDACTED]"; }

    private void requireScopeBinding() {
        requireScopeBounds();
        var scope = sourceRead.scope();
        var attempt = preparation.getAttempt();
        var owner = attempt.getExecutionOwner();
        if (!scope.tenantId().equals(preparation.getSecurity().getTenantId())
                || !scope.principal().equals(preparation.getSecurity().getPrincipal())
                || !scope.executionId().equals(attempt.getQueryOrJobId())
                || !scope.stagingAttemptId().equals(attempt.getTaskId())
                || scope.controlEpoch() != attempt.getControlEpoch()
                || !attempt.hasExecutionOwner() || !scope.ownerId().equals(owner.getOwnerId())
                || !scope.ownerIncarnation().equals(owner.getOwnerIncarnation())
                || scope.ownerFencingToken() != owner.getFencingToken()
                || scope.deadlineEpochMs() != preparation.getDeadlineEpochMs()
                || !MessageDigest.isEqual(preparation.getSourceCommandSha256().getDigest().toByteArray(),
                        HexFormat.of().parseHex(scope.commandSha256()))) {
            throw new IllegalArgumentException("Source preparation differs from its private authority scope");
        }
    }

    private void requireScopeBounds() {
        var scope = sourceRead.scope();
        if (!sourceRead.nonce().matches("[A-Za-z0-9_-]{43}")) {
            throw new IllegalArgumentException("Source preparation bearer has an invalid bound");
        }
        String[] fields = {scope.tenantId(), scope.principal(), scope.profileName(), scope.profileIncarnationId(),
                scope.canonicalSourceUrl(), scope.managedStorageRootUri(), scope.managedBackendIdentitySha256(),
                scope.executionId(), scope.stagingAttemptId(), scope.ownerId(), scope.ownerIncarnation()};
        long characters = 0;
        for (String field : fields) {
            if (field == null || field.isBlank()) {
                throw new IllegalArgumentException("Incomplete source preparation scope");
            }
            characters += field.length();
        }
        if (characters > 64 * 1024) {
            throw new IllegalArgumentException("Source preparation scope exceeds its control bound");
        }
    }
}
