package org.pangrova.protocol.rest.task;

import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.InvalidProtocolBufferException;

import org.pangrova.protocol.v2.TaskRequest;

/**
 * Authenticated REST transport envelope for one canonical Native task request.
 *
 * <p>The JSON representation deliberately contains only the serialized canonical
 * v2 {@link TaskRequest}. Task identity, plan, splits, resource grants, security,
 * exchange inputs, runtime filters, and execution options have no REST mirror.
 * The Worker parses the bytes for admission checks and passes the exact same byte
 * sequence to Native execution.</p>
 */
public final class SubmitTaskRequest {

    private final String canonicalTaskRequest;
    private final byte[] canonicalBytes;
    private final TaskRequest taskRequest;

    /** Creates a transport envelope from Base64-encoded canonical v2 bytes. */
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public SubmitTaskRequest(
            @JsonProperty(value = "canonical_task_request", required = true)
            String canonicalTaskRequest) {
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
    }

    /** Wraps a fully built canonical task request for REST transport. */
    public static SubmitTaskRequest from(TaskRequest taskRequest) {
        Objects.requireNonNull(taskRequest, "taskRequest must not be null");
        return new SubmitTaskRequest(Base64.getEncoder().encodeToString(
                taskRequest.toByteArray()));
    }

    /** Returns the only JSON transport field. */
    @JsonProperty("canonical_task_request")
    public String canonicalTaskRequest() {
        return canonicalTaskRequest;
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
                && Arrays.equals(canonicalBytes, that.canonicalBytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes);
    }

    @Override
    public String toString() {
        return "SubmitTaskRequest[canonicalBytes=" + canonicalBytes.length + "]";
    }
}
