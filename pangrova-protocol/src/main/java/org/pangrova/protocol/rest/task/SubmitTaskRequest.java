package org.pangrova.protocol.rest.task;

import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.InvalidProtocolBufferException;

import org.pangrova.protocol.rest.copy.CopySourceReadHttp;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.TaskRequest;

/**
 * Authenticated REST transport envelope for one canonical Native task request.
 *
 * <p>The canonical v2 {@link TaskRequest} remains the sole Native task contract.
 * The optional COPY bearer ticket travels only in this private HTTP envelope;
 * its nonce must never be serialized into the Native-visible protobuf.</p>
 */
public final class SubmitTaskRequest {

    private static final int SHA256_BYTES = 32;

    private final String canonicalTaskRequest;
    private final byte[] canonicalBytes;
    private final TaskRequest taskRequest;
    private final CopySourceReadHttp.RedeemRequest copySourceRead;

    /** Creates a transport envelope from Base64-encoded canonical v2 bytes. */
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public SubmitTaskRequest(
            @JsonProperty(value = "canonical_task_request", required = true)
            String canonicalTaskRequest,
            @JsonProperty("copy_source_read") CopySourceReadHttp.RedeemRequest copySourceRead) {
        if (canonicalTaskRequest == null || canonicalTaskRequest.isBlank()) {
            throw new IllegalArgumentException(
                    "canonicalTaskRequest must not be null or blank");
        }
        try {
            this.canonicalBytes = Base64.getDecoder().decode(canonicalTaskRequest);
            this.taskRequest = TaskRequest.parseFrom(this.canonicalBytes);
        } catch (IllegalArgumentException | InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException(
                    "canonicalTaskRequest is not a canonical v2 TaskRequest", failure);
        }
        if (this.canonicalBytes.length == 0) {
            throw new IllegalArgumentException("canonicalTaskRequest must not be empty");
        }
        this.canonicalTaskRequest = canonicalTaskRequest;
        this.copySourceRead = copySourceRead;
        requireCopySourceBinding();
    }

    /** Existing non-COPY task transport has no private source ticket. */
    public SubmitTaskRequest(String canonicalTaskRequest) {
        this(canonicalTaskRequest, null);
    }

    /** Wraps a fully built canonical task request for REST transport. */
    public static SubmitTaskRequest from(TaskRequest taskRequest) {
        Objects.requireNonNull(taskRequest, "taskRequest must not be null");
        return new SubmitTaskRequest(Base64.getEncoder().encodeToString(
                taskRequest.toByteArray()));
    }

    /** Wraps a fully bound COPY task and its separate private admission ticket. */
    public static SubmitTaskRequest from(TaskRequest taskRequest,
                                         CopySourceReadHttp.RedeemRequest copySourceRead) {
        Objects.requireNonNull(taskRequest, "taskRequest must not be null");
        Objects.requireNonNull(copySourceRead, "copySourceRead must not be null");
        return new SubmitTaskRequest(Base64.getEncoder().encodeToString(
                taskRequest.toByteArray()), copySourceRead);
    }

    /** Preserves a private ticket only if all canonical bytes are already final. */
    public SubmitTaskRequest withBoundTask(TaskRequest bound) {
        Objects.requireNonNull(bound, "bound");
        if (copySourceRead != null && !Arrays.equals(canonicalBytes, bound.toByteArray())) {
            throw new IllegalArgumentException(
                    "COPY ticket must be issued after final task identity and output binding");
        }
        return copySourceRead == null ? from(bound) : from(bound, copySourceRead);
    }

    /** Returns the only JSON transport field. */
    @JsonProperty("canonical_task_request")
    public String canonicalTaskRequest() {
        return canonicalTaskRequest;
    }

    /** Private Worker RPC payload. Never log or propagate to JNI. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("copy_source_read")
    public CopySourceReadHttp.RedeemRequest copySourceRead() {
        return copySourceRead;
    }

    /** Returns the immutable parsed canonical task contract. */
    public TaskRequest taskRequest() {
        return taskRequest;
    }

    /** Returns the exact bytes received from or sent by the transport envelope. */
    public byte[] canonicalBytes() {
        return canonicalBytes.clone();
    }

    @Override
    public boolean equals(Object other) {
        return this == other
                || other instanceof SubmitTaskRequest that
                && Arrays.equals(canonicalBytes, that.canonicalBytes)
                && Objects.equals(copySourceRead, that.copySourceRead);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(canonicalBytes) + Objects.hashCode(copySourceRead);
    }

    @Override
    public String toString() {
        return "SubmitTaskRequest[canonicalBytes=" + canonicalBytes.length
                + ", copySourceRead=" + (copySourceRead == null ? "absent" : "REDACTED") + "]";
    }

    private void requireCopySourceBinding() {
        boolean canonicalCopy = taskRequest.hasCopySourceStagingCommandSha256();
        if (canonicalCopy != (copySourceRead != null)) {
            throw new IllegalArgumentException("COPY staging command and private ticket must be paired");
        }
        if (!canonicalCopy) {
            return;
        }
        requireDigestBinding();
        requireAttemptBinding();
    }

    private void requireDigestBinding() {
        var checksum = taskRequest.getCopySourceStagingCommandSha256();
        if (checksum.getAlgorithm() != Checksum.Algorithm.ALGORITHM_SHA256
                || checksum.getDigest().size() != SHA256_BYTES
                || !Arrays.equals(checksum.getDigest().toByteArray(),
                        HexFormat.of().parseHex(copySourceRead.scope().commandSha256()))) {
            throw new IllegalArgumentException("COPY staging command digest does not match private scope");
        }
    }

    private void requireAttemptBinding() {
        var scope = copySourceRead.scope();
        var attempt = taskRequest.getAttempt();
        var owner = attempt.getExecutionOwner();
        if (!scope.tenantId().equals(taskRequest.getSecurity().getTenantId())
                || !scope.principal().equals(taskRequest.getSecurity().getPrincipal())
                || !scope.executionId().equals(attempt.getQueryOrJobId())
                || !scope.stagingAttemptId().equals(attempt.getTaskId())
                || scope.controlEpoch() != attempt.getControlEpoch()
                || !attempt.hasExecutionOwner()
                || !scope.ownerId().equals(owner.getOwnerId())
                || !scope.ownerIncarnation().equals(owner.getOwnerIncarnation())
                || scope.ownerFencingToken() != owner.getFencingToken()
                || scope.deadlineEpochMs() != taskRequest.getDeadlineEpochMs()) {
            throw new IllegalArgumentException("COPY private ticket is not bound to the canonical task attempt");
        }
    }
}
