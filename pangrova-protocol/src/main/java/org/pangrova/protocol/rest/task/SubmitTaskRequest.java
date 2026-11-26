package org.pangrova.protocol.rest.task;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.pangrova.protocol.http.CopyTaskScope;
import org.pangrova.protocol.http.CopyTaskSubmission;
import org.pangrova.protocol.http.CopyTaskTicket;
import org.pangrova.protocol.http.TaskSubmission;
import org.pangrova.protocol.rest.copy.CopySourceReadHttp;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.TaskRequest;

import java.io.InputStream;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Authenticated REST transport envelope for one canonical Native task request.
 *
 * <p>The canonical v2 {@link TaskRequest} remains the sole Native task contract.
 * The optional COPY bearer ticket travels only in this private HTTP envelope;
 * its nonce must never be serialized into the Native-visible protobuf.</p>
 */
public final class SubmitTaskRequest {

    public static final String MEDIA_TYPE = "application/x-protobuf";

    /** Maximum encoded task body admitted by the Worker HTTP control endpoint. */
    public static final int MAX_HTTP_BODY_BYTES = 128 * 1024 * 1024;

    private static final int SHA256_BYTES = 32;

    private final TaskRequest taskRequest;
    private final CopySourceReadHttp.RedeemRequest copySourceRead;
    private final ByteString copyTaskBytes;

    private SubmitTaskRequest(TaskRequest taskRequest, ByteString copyTaskBytes,
                              CopySourceReadHttp.RedeemRequest copySourceRead) {
        this.taskRequest = Objects.requireNonNull(taskRequest, "taskRequest must not be null");
        if (taskRequest.getSerializedSize() == 0) {
            throw new IllegalArgumentException("Canonical TaskRequest must not be empty");
        }
        this.copyTaskBytes = copyTaskBytes;
        this.copySourceRead = copySourceRead;
        requireCopySourceBinding();
    }

    /** Parses the sole task submission wire format. COPY keeps the exact embedded task bytes. */
    public static SubmitTaskRequest decode(byte[] body) throws InvalidProtocolBufferException {
        return decodeEnvelope(TaskSubmission.parseFrom(body));
    }

    /** Parses the submission directly from a request body with transport limits already applied. */
    public static SubmitTaskRequest decode(InputStream body) throws InvalidProtocolBufferException {
        return decodeEnvelope(TaskSubmission.parser().parseFrom(body));
    }

    private static SubmitTaskRequest decodeEnvelope(TaskSubmission envelope) throws InvalidProtocolBufferException {
        if (!envelope.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("Unexpected fields in task submission");
        }
        return switch (envelope.getPayloadCase()) {
            case TASK -> from(envelope.getTask());
            case COPY -> decodeCopy(envelope.getCopy());
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("Task submission payload is required");
        };
    }

    private static SubmitTaskRequest decodeCopy(CopyTaskSubmission copy) throws InvalidProtocolBufferException {
        var ticket = copy.getTicket();
        var scope = ticket.getScope();
        if (!copy.hasTicket() || !ticket.hasScope() || ticket.getNonce().isBlank()
                || !copy.getUnknownFields().asMap().isEmpty()
                || !ticket.getUnknownFields().asMap().isEmpty()
                || !scope.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("Invalid private COPY task ticket");
        }
        var source = new CopySourceReadHttp.Scope(scope.getTenantId(), scope.getPrincipal(), scope.getProfileName(),
                scope.getProfileIncarnationId(), scope.getProfileRevision(), scope.getCanonicalSourceUrl(),
                scope.getManagedStorageRootUri(), scope.getManagedBackendIdentitySha256(), scope.getExecutionId(),
                scope.getStagingAttemptId(), scope.getOwnerId(), scope.getOwnerIncarnation(), scope.getControlEpoch(),
                scope.getOwnerFencingToken(), scope.getDeadlineEpochMs());
        var bytes = copy.getCanonicalTask();
        return new SubmitTaskRequest(TaskRequest.parseFrom(bytes), bytes,
                new CopySourceReadHttp.RedeemRequest(ticket.getNonce(), source));
    }

    /** Wraps a fully built canonical task request for REST transport. */
    public static SubmitTaskRequest from(TaskRequest taskRequest) {
        return new SubmitTaskRequest(taskRequest, null, null);
    }

    /** Wraps a fully bound COPY task and its separate private admission ticket. */
    public static SubmitTaskRequest from(TaskRequest taskRequest,
                                         CopySourceReadHttp.RedeemRequest copySourceRead) {
        Objects.requireNonNull(copySourceRead, "copySourceRead must not be null");
        return new SubmitTaskRequest(taskRequest, taskRequest.toByteString(), copySourceRead);
    }

    /** Preserves a private ticket only if all canonical bytes are already final. */
    public SubmitTaskRequest withBoundTask(TaskRequest bound) {
        Objects.requireNonNull(bound, "bound");
        if (taskRequest == bound) {
            return this;
        }
        if (copySourceRead == null) {
            return from(bound);
        }
        if (!copyTaskBytes.equals(bound.toByteString())) {
            throw new IllegalArgumentException(
                    "COPY ticket must be issued after final task identity and output binding");
        }
        return this;
    }

    /** Serializes the task once into the HTTP body. */
    public byte[] encode() {
        var envelope = TaskSubmission.newBuilder();
        if (copySourceRead == null) {
            return encodeEnvelope(envelope.setTask(taskRequest).build());
        }
        var scope = copySourceRead.scope();
        var ticket = CopyTaskTicket.newBuilder().setNonce(copySourceRead.nonce())
                .setScope(CopyTaskScope.newBuilder()
                        .setTenantId(scope.tenantId()).setPrincipal(scope.principal())
                        .setProfileName(scope.profileName()).setProfileIncarnationId(scope.profileIncarnationId())
                        .setProfileRevision(scope.profileRevision()).setCanonicalSourceUrl(scope.canonicalSourceUrl())
                        .setManagedStorageRootUri(scope.managedStorageRootUri())
                        .setManagedBackendIdentitySha256(scope.managedBackendIdentitySha256())
                        .setExecutionId(scope.executionId()).setStagingAttemptId(scope.stagingAttemptId())
                        .setOwnerId(scope.ownerId()).setOwnerIncarnation(scope.ownerIncarnation())
                        .setControlEpoch(scope.controlEpoch()).setOwnerFencingToken(scope.ownerFencingToken())
                        .setDeadlineEpochMs(scope.deadlineEpochMs()));
        return encodeEnvelope(envelope.setCopy(CopyTaskSubmission.newBuilder().setCanonicalTask(copyTaskBytes)
                .setTicket(ticket)).build());
    }

    private static byte[] encodeEnvelope(TaskSubmission envelope) {
        if (envelope.getSerializedSize() > MAX_HTTP_BODY_BYTES) {
            throw new IllegalArgumentException("Task submission exceeds the HTTP limit");
        }
        return envelope.toByteArray();
    }

    /** Private Worker RPC payload. Never log or propagate to JNI. */
    public CopySourceReadHttp.RedeemRequest copySourceRead() {
        return copySourceRead;
    }

    /** Returns the immutable parsed canonical task contract. */
    public TaskRequest taskRequest() {
        return taskRequest;
    }

    /** Classifies the admitted task for scheduling and progress reporting. */
    public String taskType() {
        if (taskRequest.hasExecutionOptions()
                && taskRequest.getExecutionOptions().getDirectResultOutput()) {
            return "OUTPUT";
        }
        return switch (taskRequest.getTaskKind()) {
            case TASK_KIND_READ, TASK_KIND_SHUFFLE_READ, TASK_KIND_SHUFFLE_WRITE ->
                    "COMPUTE";
            case TASK_KIND_WRITE -> "WRITE";
            case TASK_KIND_BACKGROUND_BUILD -> "BACKGROUND_BUILD";
            case TASK_KIND_BACKGROUND_VALIDATE -> "BACKGROUND_VALIDATE";
            case TASK_KIND_BACKGROUND_EXPORT -> "BACKGROUND_EXPORT";
            case TASK_KIND_CDC_INGEST -> "CDC_INGEST";
            default -> throw new IllegalArgumentException(
                    "Unsupported canonical task kind: " + taskRequest.getTaskKind());
        };
    }

    /** Returns the canonical task bytes, preserving the original bytes for private COPY. */
    public byte[] canonicalBytes() {
        return copyTaskBytes == null ? taskRequest.toByteArray() : copyTaskBytes.toByteArray();
    }

    @Override
    public boolean equals(Object other) {
        return this == other
                || other instanceof SubmitTaskRequest that
                && taskRequest.equals(that.taskRequest)
                && Objects.equals(copySourceRead, that.copySourceRead)
                && Objects.equals(copyTaskBytes, that.copyTaskBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskRequest, copySourceRead, copyTaskBytes);
    }

    @Override
    public String toString() {
        int size = copyTaskBytes == null ? taskRequest.getSerializedSize() : copyTaskBytes.size();
        return "SubmitTaskRequest[canonicalBytes=" + size
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
                || !checksum.getDigest().equals(ByteString.copyFrom(
                        HexFormat.of().parseHex(copySourceRead.scope().commandSha256())))) {
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
