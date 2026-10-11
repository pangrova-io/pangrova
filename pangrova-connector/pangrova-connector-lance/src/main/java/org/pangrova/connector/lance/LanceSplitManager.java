package org.pangrova.connector.lance;

import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.ConnectorSplitSource;
import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.spi.connector.metadata.DynamicFilter;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import java.util.Optional;
import org.pangrova.spi.connector.metadata.NativeDatasetControlPlane;

/** Enumerates bounded physical manifest partitions from an immutable table handle. */
public final class LanceSplitManager implements ConnectorSplitManager {
    private final Optional<NativeDatasetControlPlane> controlPlane;
    private final LanceSnapshotResolver snapshots;
    private final java.util.function.Supplier<LanceCatalogStore> store;

    LanceSplitManager(Optional<NativeDatasetControlPlane> controlPlane,
            java.util.function.Supplier<LanceCatalogStore> store, String catalog, String warehouse) {
        this.controlPlane = java.util.Objects.requireNonNull(controlPlane);
        this.store = java.util.Objects.requireNonNull(store);
        this.snapshots = new LanceSnapshotResolver(store, controlPlane, catalog, warehouse);
    }
    @Override public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction,
            ConnectorSession session, ConnectorTableHandle table, DynamicFilter dynamicFilter, Constraint constraint) {
        if (!(table instanceof LanceTableHandle lance)) {
            throw new IllegalArgumentException("Expected Lance table handle");
        }
        LanceSessionIdentity.requireTenant(session, lance.tenantId());
        var selected = snapshots.resolve(session, lance);
        return new LancePartitionSplitSource(selected, session, controlPlane,
                selected.version() == 0 ? null : store.get().loadSnapshot(selected).reference());
    }
}
