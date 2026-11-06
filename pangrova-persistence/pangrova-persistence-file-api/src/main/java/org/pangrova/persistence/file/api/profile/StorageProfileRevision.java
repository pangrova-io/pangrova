package org.pangrova.persistence.file.api.profile;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.filesystem.api.profile.StorageProfile;

import java.util.Objects;
import java.util.UUID;

/** Immutable identity of one storage-profile definition, including after DROP/re-CREATE. */
@Experimental
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
