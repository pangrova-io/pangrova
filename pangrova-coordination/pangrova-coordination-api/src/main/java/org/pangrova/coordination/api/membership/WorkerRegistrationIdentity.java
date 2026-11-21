package org.pangrova.coordination.api.membership;

import java.util.UUID;

/** Exact registered process; a reused logical Worker ID alone never authorizes withdrawal. */
public record WorkerRegistrationIdentity(String workerId, String admissionDomainId, String incarnationId) {
    public WorkerRegistrationIdentity {
        if (workerId == null || workerId.isBlank()) {
            throw new IllegalArgumentException("workerId must not be blank");
        }
        requireUuid(admissionDomainId);
        requireUuid(incarnationId);
    }

    private static void requireUuid(String value) {
        if (value == null || !UUID.fromString(value).toString().equals(value)) {
            throw new IllegalArgumentException("Worker identity requires canonical UUIDs");
        }
    }
}
