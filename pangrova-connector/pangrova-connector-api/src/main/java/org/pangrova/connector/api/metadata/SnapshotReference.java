package org.pangrova.connector.api.metadata;

import org.pangrova.connector.api.enums.SnapshotReferenceType;
import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;

/** A named snapshot reference returned by connector catalog metadata. */
@Experimental
public record SnapshotReference(String name, SnapshotReferenceType type, long snapshotId) {


    public SnapshotReference {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("snapshot reference name must not be blank");
        }
        Objects.requireNonNull(type, "snapshot reference type must not be null");
        if (snapshotId <= 0) {
            throw new IllegalArgumentException("snapshot reference ID must be positive");
        }
    }
}
