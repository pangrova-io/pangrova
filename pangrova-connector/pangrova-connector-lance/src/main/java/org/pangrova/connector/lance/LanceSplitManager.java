package org.pangrova.connector.lance;

import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.ConnectorTransactionHandle;
import org.pangrova.connector.spi.metadata.ConnectorSplitManager;
import org.pangrova.connector.spi.metadata.ConnectorSplitSource;
import org.pangrova.connector.spi.metadata.Constraint;
import org.pangrova.connector.spi.metadata.DynamicFilter;
import org.pangrova.connector.spi.metadata.table.ConnectorTableHandle;
import java.util.Optional;
import org.pangrova.connector.spi.metadata.NativeDatasetControlPlane;

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
