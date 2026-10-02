package org.pangrova.protocol.rest.task;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Exact fenced identity of the process that owns a query or background job. */
public record ExecutionOwnerIdentity(
        @JsonProperty("owner_id") String ownerId,
        @JsonProperty("owner_incarnation") String ownerIncarnation,
        @JsonProperty("fencing_token") long fencingToken) {

    public ExecutionOwnerIdentity {
        requireNonBlank(ownerId, "ownerId");
        requireNonBlank(ownerIncarnation, "ownerIncarnation");
        if (fencingToken <= 0) {
            throw new IllegalArgumentException("fencingToken must be positive");
        }
    }

    /** Creates the REST control projection of a canonical owner identity. */
    public static ExecutionOwnerIdentity from(
            org.pangrova.protocol.v2.ExecutionOwnerIdentity owner) {
        if (owner == null) {
            throw new IllegalArgumentException("owner must not be null");
        }
        return new ExecutionOwnerIdentity(
                owner.getOwnerId(), owner.getOwnerIncarnation(),
                owner.getFencingToken());
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
