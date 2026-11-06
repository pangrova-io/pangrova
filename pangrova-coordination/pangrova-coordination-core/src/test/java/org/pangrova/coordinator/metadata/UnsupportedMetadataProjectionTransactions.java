package org.pangrova.coordinator.metadata;

import org.pangrova.persistence.catalog.api.projection.MetadataProjectionTransactions;
import org.pangrova.persistence.catalog.api.projection.MetadataProjectionMutation;
import org.pangrova.persistence.catalog.api.projection.MetadataProjectionReceipt;
import java.util.Optional;
import java.util.UUID;

/** Connector-only fixture: local projection mutation is not a supplied capability. */
public final class UnsupportedMetadataProjectionTransactions implements MetadataProjectionTransactions {
    @Override public void recoverPending(org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity scope) {
        throw new UnsupportedOperationException("Connector-only fixture has no projection recovery");
    }
    @Override public MetadataProjectionReceipt apply(MetadataProjectionMutation mutation) { throw unsupported(); }
    @Override public Optional<MetadataProjectionReceipt> findReceipt(UUID operationId) { throw unsupported(); }
    private UnsupportedOperationException unsupported() {
        return new UnsupportedOperationException("This connector-only fixture does not implement local projection transactions");
    }
}
