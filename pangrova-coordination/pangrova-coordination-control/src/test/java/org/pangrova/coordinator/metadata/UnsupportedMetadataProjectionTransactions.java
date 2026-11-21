package org.pangrova.coordinator.metadata;

import org.pangrova.persistence.workflow.api.projection.MetadataProjectionTransactions;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionMutation;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionReceipt;
import java.util.Optional;
import java.util.UUID;

/** Connector-only fixture: local projection mutation is not a supplied capability. */
public final class UnsupportedMetadataProjectionTransactions implements MetadataProjectionTransactions {
    @Override public long beginObservation(org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity scope) {
        throw unsupported();
    }
    @Override public void recoverPending(org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity scope) {
        throw new UnsupportedOperationException("Connector-only fixture has no projection recovery");
    }
    @Override public MetadataProjectionReceipt apply(long epoch, MetadataProjectionMutation mutation) { throw unsupported(); }
    @Override public Optional<MetadataProjectionReceipt> findReceipt(UUID operationId) { throw unsupported(); }
    private UnsupportedOperationException unsupported() {
        return new UnsupportedOperationException("This connector-only fixture does not implement local projection transactions");
    }
}
