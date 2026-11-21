package org.pangrova.shuffle.api;

import static org.pangrova.common.util.Preconditions.requireText;

/** Fenced identity of the Coordinator process that owns a shuffle lifecycle. */
public record ShuffleExecutionOwner(
        String ownerId,
        String ownerIncarnation,
        long fencingToken) {

    public ShuffleExecutionOwner {
        requireText(ownerId, "ownerId");
        requireText(ownerIncarnation, "ownerIncarnation");
        if (fencingToken <= 0) {
            throw new IllegalArgumentException("fencingToken must be positive");
        }
    }
}
