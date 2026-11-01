package org.pangrova.testing.integration.e2e.setup;

import org.pangrova.connector.iceberg.IcebergConnector;
import org.pangrova.spi.connector.plugin.Connector;
import org.apache.iceberg.catalog.Catalog;

import java.nio.file.Path;

/**
 * Creates Connector instances for E2E test data setup.
 * Lives in test scope — allowed to import connector implementation classes.
 */
public final class TestConnectorFactory {

    private TestConnectorFactory() {}

    /**
     * Creates an Iceberg Connector backed by a local filesystem warehouse.
     *
     * @param warehousePath local directory for Iceberg warehouse
     * @return a Connector instance for Iceberg operations
     */
    public static Connector createIcebergConnector(Path warehousePath) {
        Catalog catalog = IcebergCatalogFactory.create(warehousePath);
        return new IcebergConnector("test_warehouse", catalog);
    }
}
