package org.pangrova.spi.connector.metadata;

import java.util.Objects;

/** A named snapshot reference returned by connector catalog metadata. */
public record SnapshotReference(String name, Type type, long snapshotId) {

    public enum Type {
        BRANCH,
        TAG
    }

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
