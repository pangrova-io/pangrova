package org.pangrova.protocol.rest.worker;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

/** Durable Worker ownership domain and one physical execution incarnation. */
public record WorkerExecutionIdentity(
        @JsonProperty("admission_domain_id") String admissionDomainId,
        @JsonProperty("incarnation_id") String incarnationId) {
    public WorkerExecutionIdentity {
        requireUuid(admissionDomainId);
        requireUuid(incarnationId);
    }

    public org.pangrova.protocol.v2.WorkerExecutionIdentity toProto() {
        return org.pangrova.protocol.v2.WorkerExecutionIdentity.newBuilder()
                .setAdmissionDomainId(admissionDomainId).setIncarnationId(incarnationId).build();
    }

    /** Validates and decodes the exact canonical execution target. */
    public static WorkerExecutionIdentity from(org.pangrova.protocol.v2.WorkerExecutionIdentity value) {
        if (!value.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("Worker execution identity contains unknown fields");
        }
        return new WorkerExecutionIdentity(value.getAdmissionDomainId(), value.getIncarnationId());
    }

    private static void requireUuid(String value) {
        if (value == null || !UUID.fromString(value).toString().equals(value)) {
            throw new IllegalArgumentException("Worker execution identity requires canonical UUIDs");
        }
    }
}
