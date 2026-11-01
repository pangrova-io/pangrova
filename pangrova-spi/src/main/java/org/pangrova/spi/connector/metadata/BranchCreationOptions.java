package org.pangrova.spi.connector.metadata;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import org.pangrova.common.api.annotation.Stable;

/** Exact branch source and reference lifetime; omitted values use the connector's current source/default. */
@Stable(since = "1.0")
public record BranchCreationOptions(
        OptionalLong sourceSnapshotId,
        Optional<String> sourceTag,
        OptionalLong maxReferenceAgeMillis) {
    public BranchCreationOptions {
        Objects.requireNonNull(sourceSnapshotId, "sourceSnapshotId");
        Objects.requireNonNull(sourceTag, "sourceTag");
        Objects.requireNonNull(maxReferenceAgeMillis, "maxReferenceAgeMillis");
        if (sourceSnapshotId.isPresent() && sourceSnapshotId.getAsLong() <= 0) {
            throw new IllegalArgumentException("Branch source snapshot must be positive");
        }
        if (sourceTag.isPresent() && sourceTag.orElseThrow().isBlank()) {
            throw new IllegalArgumentException("Branch source tag must not be blank");
        }
        if (sourceSnapshotId.isPresent() && sourceTag.isPresent()) {
            throw new IllegalArgumentException("Branch requires one source, either a snapshot or a tag");
        }
        if (maxReferenceAgeMillis.isPresent() && maxReferenceAgeMillis.getAsLong() <= 0) {
            throw new IllegalArgumentException("Branch reference lifetime must be positive");
        }
    }

    public static BranchCreationOptions current() {
        return new BranchCreationOptions(OptionalLong.empty(), Optional.empty(), OptionalLong.empty());
    }
}
