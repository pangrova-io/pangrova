package org.pangrova.spi.storage.file;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import org.pangrova.spi.annotation.Experimental;

/** Immutable namespace identity and definition revision for a directory scan. */
@Experimental
public record FileDirectoryReference(UUID spaceId, Instant definitionRevision) {
    public FileDirectoryReference {
        Objects.requireNonNull(spaceId, "spaceId");
        Objects.requireNonNull(definitionRevision, "definitionRevision");
    }
}
