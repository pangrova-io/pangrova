package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.api.index.SnapshotPinOwner;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Durable construction-only snapshot pin lifecycle for scalar index attempts. */
@Experimental
public interface ScalarSnapshotPins {
    record ScalarPin(String tenantId, String catalog, String schema, String table,
                     String snapshot, SnapshotPinOwner owner, String state) {}

    default boolean reserveScalarSnapshotPin(IndexDescriptor descriptor, String snapshot,
                                              SnapshotPinOwner owner, Instant now) {
        throw new UnsupportedOperationException("Durable scalar pin reservation is required");
    }

    default void markScalarSnapshotPinDrained(SnapshotPinOwner owner, String snapshot, Instant now) {
        throw new UnsupportedOperationException("Durable scalar drain evidence is required");
    }

    default Optional<ScalarPin> findScalarSnapshotPin(SnapshotPinOwner owner) {
        return Optional.empty();
    }

    default List<ScalarPin> listScalarSnapshotPinCleanupCandidates(Instant now, int limit) {
        return List.of();
    }

    default Optional<ScalarPin> claimScalarSnapshotPinCleanup(SnapshotPinOwner owner, Instant now) {
        throw new UnsupportedOperationException("Durable scalar pin cleanup claim is required");
    }

    default void markScalarSnapshotPinReleased(SnapshotPinOwner owner, String snapshot, Instant now) {
        throw new UnsupportedOperationException("Durable scalar pin release is required");
    }
}
