package org.pangrova.connector.api.index;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;

/** One exact durable connector pin, including the opaque source snapshot token. */
@Experimental
public record SnapshotPinReference(String snapshotToken, SnapshotPinOwner owner) {
    public SnapshotPinReference {
        if (snapshotToken == null || snapshotToken.isBlank()) {
            throw new IllegalArgumentException("snapshotToken must not be blank");
        }
        owner = Objects.requireNonNull(owner, "owner");
    }
}
