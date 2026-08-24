package org.pangrova.testing.e2e.setup;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;

import java.nio.file.Path;
import java.util.Map;

/**
 * Creates Iceberg HadoopCatalog instances for E2E test data setup.
 * Isolates Iceberg SDK catalog construction from test data logic.
 */
final class IcebergCatalogFactory {

    private static final String CATALOG_NAME = "test_warehouse";

    private IcebergCatalogFactory() {}

    /**
     * Create a HadoopCatalog pointing to the given warehouse directory.
     *
     * @param warehousePath local filesystem directory for Iceberg warehouse
     * @return configured HadoopCatalog instance
     */
    static Catalog create(Path warehousePath) {
        Map<String, String> props = Map.of(
            "type", "hadoop",
            "warehouse", warehousePath.toUri().toString());
        var hadoopConf = new org.apache.hadoop.conf.Configuration();
        return CatalogUtil.buildIcebergCatalog(CATALOG_NAME, props, hadoopConf);
    }
}
