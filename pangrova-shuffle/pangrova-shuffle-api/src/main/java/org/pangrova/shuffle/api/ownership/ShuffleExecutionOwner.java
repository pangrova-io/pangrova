package org.pangrova.shuffle.api.ownership;

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

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
