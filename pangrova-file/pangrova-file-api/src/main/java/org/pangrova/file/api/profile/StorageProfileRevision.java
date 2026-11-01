package org.pangrova.file.api.profile;

import java.util.Objects;
import java.util.UUID;

/** Immutable identity of one storage-profile definition, including after DROP/re-CREATE. */
public record StorageProfileRevision(StorageProfile profile, String incarnationId, long revision) {

    public StorageProfileRevision {
        Objects.requireNonNull(profile, "profile must not be null");
        Objects.requireNonNull(incarnationId, "incarnationId must not be null");
        UUID.fromString(incarnationId);
        if (revision <= 0) {
            throw new IllegalArgumentException("storage profile revision must be positive");
        }
    }
}
