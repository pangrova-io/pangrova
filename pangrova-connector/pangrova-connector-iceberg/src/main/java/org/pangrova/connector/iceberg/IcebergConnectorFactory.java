package org.pangrova.connector.iceberg;

import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorContext;
import org.pangrova.spi.connector.ConnectorFactory;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;

import java.util.HashMap;
import java.util.Map;
import java.util.Locale;

/**
 * Factory for creating Iceberg connector instances with real Catalog.
 * Resolves the configured SDK catalog type without substituting another backend.
 * Delegates catalog construction to Iceberg CatalogUtil or RESTCatalog directly.
 */
public class IcebergConnectorFactory implements ConnectorFactory {

    private static final String CATALOG_TYPE_KEY = "catalog-type";
    private static final String CATALOG_TYPE_REST = "rest";
    private static final String REST_URI_KEY = "uri";

    /** OAuth2 pass-through property keys forwarded to the RESTCatalog. */
    private static final java.util.List<String> OAUTH2_PASSTHROUGH_KEYS =
            java.util.List.of("credential", "oauth2-server-uri", "scope", "token");

    @Override
    public String getName() {
        return "iceberg";
    }

    private static final String ENCRYPTION_ACCESSIBLE_KEYS = "encryption.accessible-keys";

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        CatalogAndFileIO catalogAndIO = buildCatalog(catalogName, config);
        java.util.Set<String> accessibleKeys = parseAccessibleKeys(config.get(ENCRYPTION_ACCESSIBLE_KEYS));
        return new IcebergConnector(catalogName, catalogAndIO.catalog(),
                catalogAndIO.fileIO(), accessibleKeys);
    }

    private static java.util.Set<String> parseAccessibleKeys(String value) {
        if (value == null || value.isBlank()) {
            return java.util.Set.of();
        }
        java.util.Set<String> keys = new java.util.HashSet<>();
        for (String key : value.split(",")) {
            String trimmed = key.trim();
            if (!trimmed.isEmpty()) {
                keys.add(trimmed);
            }
        }
        return keys;
    }

    private record CatalogAndFileIO(Catalog catalog, org.apache.iceberg.io.FileIO fileIO) {}

    private CatalogAndFileIO buildCatalog(String catalogName, Map<String, String> config) {
        Map<String, String> normalized = normalizeCatalogProperties(config);
        String catalogType = normalized.get("type");
        if (CATALOG_TYPE_REST.equalsIgnoreCase(catalogType)) {
            return new CatalogAndFileIO(buildRestCatalog(catalogName, normalized), null);
        }
        return buildSdkCatalog(catalogName, normalized);
    }

    private static Map<String, String> normalizeCatalogProperties(Map<String, String> config) {
        Map<String, String> properties = new HashMap<>(config);
        String type = catalogType(config.get("type"), "type");
        String alias = catalogType(properties.remove(CATALOG_TYPE_KEY), CATALOG_TYPE_KEY);
        if (type != null && alias != null && !type.equals(alias)) {
            throw new IllegalArgumentException("Conflicting Iceberg catalog properties: type and catalog-type");
        }
        String selected = type != null ? type : alias;
        if (properties.containsKey("catalog-impl")) {
            if (selected != null) {
                throw new IllegalArgumentException("Use either catalog-impl or a catalog type, not both");
            }
        } else {
            selected = selected != null ? selected : "hadoop";
            properties.put("type", selected);
        }
        if ("hadoop".equals(selected) || "jdbc".equals(selected)) {
            requireProperty(properties, "warehouse", selected);
        }
        if ("jdbc".equals(selected) || "rest".equals(selected)) {
            requireProperty(properties, "uri", selected);
        }
        return properties;
    }

    private static String catalogType(String value, String key) {
        if (value == null) return null;
        if (value.isBlank()) throw new IllegalArgumentException("Iceberg " + key + " must not be blank");
        return value.trim().toLowerCase(Locale.ROOT);
    }

    private static void requireProperty(Map<String, String> properties, String key, String type) {
        String value = properties.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Iceberg " + type + " catalog requires '" + key + "'");
        }
    }

    private Catalog buildRestCatalog(String catalogName, Map<String, String> config) {
        RESTCatalog restCatalog = new RESTCatalog();
        Map<String, String> catalogProps = new HashMap<>(config);
        catalogProps.remove(CATALOG_TYPE_KEY);
        if (!catalogProps.containsKey(REST_URI_KEY)) {
            throw new IllegalArgumentException(
                    "REST catalog requires 'uri' property specifying the catalog server endpoint");
        }
        catalogProps.put("type", "rest");

        // OAuth2 token refresh: pass through credential, scope, and token endpoint
        // properties to the RESTCatalog which handles token lifecycle internally.
        copyOAuth2Passthrough(config, catalogProps);

        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        config.forEach(hadoopConf::set);
        restCatalog.setConf(hadoopConf);
        restCatalog.initialize(catalogName, catalogProps);
        return restCatalog;
    }

    private static void copyOAuth2Passthrough(Map<String, String> config, Map<String, String> catalogProps) {
        for (String key : OAUTH2_PASSTHROUGH_KEYS) {
            String value = config.get(key);
            if (value != null && !value.isBlank()) {
                catalogProps.put(key, value);
            }
        }
    }

    private CatalogAndFileIO buildSdkCatalog(String catalogName, Map<String, String> config) {
        Map<String, String> catalogProps = new HashMap<>(config);
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        // Keep local storage isolated from Hadoop's process-wide filesystem cache.
        // Authentication and the current login identity belong to the deployment.
        hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.RawLocalFileSystem.class.getName());
        hadoopConf.setBoolean("fs.file.impl.disable.cache", true);
        config.forEach(hadoopConf::set);
        Catalog catalog = CatalogUtil.buildIcebergCatalog(catalogName, catalogProps, hadoopConf);
        org.apache.iceberg.hadoop.HadoopFileIO fileIO = new org.apache.iceberg.hadoop.HadoopFileIO(hadoopConf);
        return new CatalogAndFileIO(catalog, fileIO);
    }

}
