package org.pangrova.connector.paimon;

import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorContext;
import org.pangrova.spi.connector.ConnectorFactory;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

import java.util.Map;

/**
 * Factory for creating Paimon connector instances with real Catalog.
 * Supports filesystem (default) and REST catalog types.
 * Delegates catalog construction to Paimon CatalogFactory.
 */
public class PaimonConnectorFactory implements ConnectorFactory {

    private static final String METASTORE_KEY = "metastore";
    private static final String METASTORE_REST = "rest";
    private static final String REST_URI_KEY = "uri";

    @Override
    public String getName() {
        return "paimon";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        Catalog catalog = buildCatalog(config);
        String tempDir = config.getOrDefault("temp-dir",
                System.getProperty("java.io.tmpdir") + "/paimon-io");
        return new PaimonConnector(catalogName, catalog, tempDir);
    }

    private Catalog buildCatalog(Map<String, String> config) {
        String metastore = config.get(METASTORE_KEY);
        if (METASTORE_REST.equalsIgnoreCase(metastore)) {
            return buildRestCatalog(config);
        }
        return buildFilesystemCatalog(config);
    }

    private Catalog buildRestCatalog(Map<String, String> config) {
        if (!config.containsKey(REST_URI_KEY)) {
            throw new IllegalArgumentException(
                    "REST catalog requires 'uri' property specifying the catalog server endpoint");
        }
        Options options = new Options();
        config.forEach(options::setString);
        options.setString("metastore", "rest");
        CatalogContext context = CatalogContext.create(options);
        return CatalogFactory.createCatalog(context);
    }

    private Catalog buildFilesystemCatalog(Map<String, String> config) {
        Options options = new Options();
        config.forEach(options::setString);
        if (!config.containsKey("warehouse")) {
            options.setString("warehouse", System.getProperty("java.io.tmpdir") + "/paimon-warehouse");
        }
        if (!config.containsKey("temp-dir")) {
            options.setString("temp-dir", System.getProperty("java.io.tmpdir") + "/paimon-temp");
        }
        CatalogContext context = CatalogContext.create(options);
        return CatalogFactory.createCatalog(context);
    }
}
