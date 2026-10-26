package org.pangrova.coordinator.bootstrap;

import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.control.JdbcTableControlStore;
import org.pangrova.metadata.control.TableControlService;
import org.pangrova.metadata.store.JdbcMetadataStore;

import javax.sql.DataSource;

/** Technical metadata wiring matching production startup for governance lifecycle tests. */
final class GovernanceBootstrapTestAuthority {
    private GovernanceBootstrapTestAuthority() {}

    static DataSource bind(ModuleIntegrationBootstrap modules, DataSource source) {
        modules.setMetadataSyncDeps(null, registry(source));
        return source;
    }

    static DataSource bind(ModuleIntegrationWiring modules, DataSource source) {
        modules.setMetadataSyncDeps(null, registry(source));
        return source;
    }

    private static CatalogRegistry registry(DataSource source) {
        var registry = new CatalogRegistry(new JdbcMetadataStore(source));
        registry.setTableControlService(new TableControlService(
                new JdbcTableControlStore(source), registry::requireCurrentCatalogIdentity));
        return registry;
    }
}
