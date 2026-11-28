package org.pangrova.testing.integration.e2e.fixture.catalog;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Registers ConnectorTestConfig entries as CONNECTOR catalogs in CatalogRegistry.
 * Bridges test configuration to the production catalog lifecycle.
 */
public final class TestCatalogRegistrar {

    private static final Logger LOG = LoggerFactory.getLogger(TestCatalogRegistrar.class);

    private TestCatalogRegistrar() {}

    /** Register each ConnectorTestConfig as a CONNECTOR catalog and create default schema. */
    public static void registerAll(CatalogRegistry registry, List<ConnectorTestConfig> configs,
            Function<String, ConnectorSession> sessions) {
        for (ConnectorTestConfig config : configs) {
            prepareLocalWarehouse(config);
            register(registry, config);
            ensureDefaultSchema(registry, config, sessions.apply(config.catalogName()));
        }
    }

    private static void prepareLocalWarehouse(ConnectorTestConfig config) {
        if (!"iceberg".equals(config.connectorType()) || !"hadoop".equals(config.properties().get("type"))) {
            return;
        }
        String implementation = config.properties().get("catalog-impl");
        if (implementation != null && !"org.apache.iceberg.hadoop.HadoopCatalog".equals(implementation)) return;
        String warehouse = java.util.Objects.requireNonNull(config.properties().get("warehouse"), "warehouse");
        URI location = URI.create(warehouse);
        if (location.getScheme() != null && !"file".equals(location.getScheme())) return;
        Path directory = location.getScheme() == null ? Path.of(warehouse) : Path.of(location);
        try {
            Files.createDirectories(directory);
        } catch (IOException failure) {
            throw new UncheckedIOException("Failed to prepare the local Hadoop fixture warehouse", failure);
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

    private static void ensureDefaultSchema(CatalogRegistry registry, ConnectorTestConfig config,
            ConnectorSession session) {
        CatalogHandle handle = registry.getCatalog(config.catalogName());
        ConnectorMetadata metadata = handle.getMetadata(session);
        if (!metadata.listSchemaNames(session).contains(config.schemaName())) {
            metadata.createSchema(session, config.schemaName());
            LOG.info("Created default schema '{}' for catalog '{}'",
                    config.schemaName(), config.catalogName());
        }
    }
}
