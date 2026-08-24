package org.pangrova.testing.e2e.setup;

import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Registers ConnectorTestConfig entries as CONNECTOR catalogs in CatalogRegistry.
 * Bridges test configuration to the production catalog lifecycle.
 */
public final class TestCatalogRegistrar {

    private static final Logger LOG = LoggerFactory.getLogger(TestCatalogRegistrar.class);

    private TestCatalogRegistrar() {}

    /** Register each ConnectorTestConfig as a CONNECTOR catalog and create default schema. */
    public static void registerAll(CatalogRegistry registry, List<ConnectorTestConfig> configs) {
        for (ConnectorTestConfig config : configs) {
            register(registry, config);
            ensureDefaultSchema(registry, config);
        }
    }

    private static void register(CatalogRegistry registry, ConnectorTestConfig config) {
        CatalogDescriptor descriptor = new CatalogDescriptor(
                config.catalogName(),
                CatalogType.CONNECTOR,
                config.connectorType(),
                CatalogState.ACTIVE,
                config.properties(),
                "E2E test catalog: " + config.connectorType(),
                Set.of());
        registry.register(descriptor);
        LOG.info("Registered test catalog '{}' (type={})",
                config.catalogName(), config.connectorType());
    }

    private static void ensureDefaultSchema(CatalogRegistry registry, ConnectorTestConfig config) {
        try {
            CatalogHandle handle = registry.getCatalog(config.catalogName());
            ConnectorSession session = new SimpleSession(config.catalogName());
            ConnectorMetadata metadata = handle.getMetadata(session);
            metadata.createSchema(session, config.schemaName());
            LOG.info("Created default schema '{}' for catalog '{}'",
                    config.schemaName(), config.catalogName());
        } catch (UnsupportedOperationException e) {
            LOG.debug("Catalog '{}' does not support createSchema", config.catalogName());
        } catch (Exception e) {
            LOG.warn("Failed to create schema '{}' for catalog '{}': {}",
                    config.schemaName(), config.catalogName(), e.getMessage());
        }
    }

    /** Minimal ConnectorSession for schema initialization. */
    private record SimpleSession(String catalog) implements ConnectorSession {
        @Override public String getUser() { return "system"; }
        @Override public Optional<String> getCatalog() { return Optional.of(catalog); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    }
}
