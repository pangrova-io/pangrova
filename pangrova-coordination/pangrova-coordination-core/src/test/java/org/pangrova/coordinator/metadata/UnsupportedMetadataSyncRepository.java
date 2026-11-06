package org.pangrova.coordinator.metadata;

import org.pangrova.persistence.catalog.api.synchronization.MetadataSyncRepository;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.persistence.catalog.api.synchronization.MetadataSyncCompletion;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/** Connector-only test fixture: synchronization is outside its scope and must fail if invoked. */
public final class UnsupportedMetadataSyncRepository implements MetadataSyncRepository {
    @Override public void recoverPending(CatalogDeploymentIdentity scope) {
        throw new UnsupportedOperationException("Connector-only fixture has no synchronization recovery");
    }
    @Override public Optional<Instant> loadCheckpoint(CatalogDeploymentIdentity scope) {
        throw new UnsupportedOperationException("This test fixture does not support metadata synchronization");
    }
    @Override public void complete(MetadataSyncCompletion completion) {
        throw new UnsupportedOperationException("This test fixture does not support metadata synchronization");
    }
    @Override public Optional<MetadataSyncCompletion> findCompletion(UUID id) {
        throw new UnsupportedOperationException("This test fixture does not support metadata synchronization");
    }
}
