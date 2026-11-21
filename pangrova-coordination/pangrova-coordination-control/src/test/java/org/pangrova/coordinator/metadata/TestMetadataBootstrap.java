package org.pangrova.coordinator.metadata;

import org.pangrova.metadata.api.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.testinfra.TestCatalogRegistry;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.persistence.catalog.api.projection.TableProjectionRepository;
import org.pangrova.persistence.catalog.api.synchronization.MetadataSyncRepository;
import org.pangrova.persistence.table.api.provider.TableStores;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionTransactions;

/** Boots the standard metadata runtime with a registry configured by an owning test fixture. */
public final class TestMetadataBootstrap {
    private TestMetadataBootstrap() {}

    public static MetadataBootstrap initialize(TableStores stores, CatalogDeploymentRepository deployments,
            TableProjectionRepository tables, MetadataProjectionTransactions projections,
            MetadataSyncRepository synchronization, ConnectorHandleFactory connectors) {
        var dispatcher = new MetadataEventDispatcher();
        var registry = new TestCatalogRegistry(stores, deployments, connectors, null, dispatcher);
        return MetadataBootstrap.initialize(registry, dispatcher, tables, projections, synchronization, null, null);
    }
}
