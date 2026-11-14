package org.pangrova.file.api.directory;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import org.pangrova.common.api.annotation.Experimental;

/** Immutable namespace identity and definition revision for a directory scan. */
@Experimental
public record FileDirectoryReference(UUID spaceId, Instant definitionRevision) {
    public FileDirectoryReference {
        Objects.requireNonNull(spaceId, "spaceId");
        Objects.requireNonNull(definitionRevision, "definitionRevision");
    }
}
