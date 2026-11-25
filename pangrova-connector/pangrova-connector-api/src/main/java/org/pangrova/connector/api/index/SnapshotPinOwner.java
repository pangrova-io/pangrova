package org.pangrova.connector.api.index;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;

/** Stable owner of one connector snapshot pin across submission and cleanup retries. */
@Experimental
public record SnapshotPinOwner(String indexId, int generation, String attemptId) {
    public SnapshotPinOwner {
        if (indexId == null || indexId.isBlank()) {
            throw new IllegalArgumentException("indexId must not be blank");
        }
        if (generation < 0) {
            throw new IllegalArgumentException("generation must be non-negative");
        }
        if (attemptId == null || attemptId.isBlank()) {
            throw new IllegalArgumentException("attemptId must not be blank");
        }
    }

    /** Returns the canonical durable identity used by the pin ledger and connector tags. */
    public String id() {
        return indexId + ":generation:" + generation + ":attempt:" + attemptId;
    }

    /** Parses a canonical owner identity without interpreting connector snapshot tokens. */
    public static SnapshotPinOwner fromId(String id) {
        Objects.requireNonNull(id, "id");
        String generationMarker = ":generation:";
        String attemptMarker = ":attempt:";
        int generationAt = id.indexOf(generationMarker);
        int attemptAt = id.indexOf(attemptMarker, generationAt + generationMarker.length());
        if (generationAt <= 0 || attemptAt <= generationAt + generationMarker.length()) {
            throw new IllegalArgumentException("Invalid snapshot pin owner identity");
        }
        try {
            return new SnapshotPinOwner(id.substring(0, generationAt),
                    Integer.parseInt(id.substring(generationAt + generationMarker.length(), attemptAt)),
                    id.substring(attemptAt + attemptMarker.length()));
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException("Invalid snapshot pin owner generation", invalid);
        }
    }
}
