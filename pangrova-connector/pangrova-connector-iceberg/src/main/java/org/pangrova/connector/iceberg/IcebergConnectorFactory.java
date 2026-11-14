package org.pangrova.connector.iceberg;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.connector.api.constants.ConnectorFactoryContract;
import org.pangrova.connector.api.enums.ConnectorRole;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.plugin.ConnectorContext;
import org.pangrova.connector.api.plugin.ConnectorFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Factory for creating Iceberg connector instances with real Catalog.
 * Resolves the configured SDK catalog type without substituting another backend.
 * Delegates catalog construction to Iceberg CatalogUtil or RESTCatalog directly.
 */
public class IcebergConnectorFactory implements ConnectorFactory {

    private static final String CATALOG_TYPE_KEY = "catalog-type";
    private static final String CATALOG_TYPE_REST = "rest";
    @Override
    public int apiVersion() {
        return ConnectorFactoryContract.CURRENT_API_VERSION;
    }

    @Override
    public Set<ConnectorRole> roles() {
        return Set.of(ConnectorRole.COORDINATOR, ConnectorRole.WORKER);
    }

    @Override
    public void validateConfiguration(Map<String, String> config) {
        normalizeCatalogProperties(config);
    }

    @Override
    public String getName() {
        return "iceberg";
    }

    private static final String ENCRYPTION_ACCESSIBLE_KEYS = "encryption.accessible-keys";

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        Map<String, String> properties = normalizeCatalogProperties(config);
        Set<String> accessibleKeys = parseAccessibleKeys(properties.get(ENCRYPTION_ACCESSIBLE_KEYS));
        var hadoopConf = new org.apache.hadoop.conf.Configuration();
        Catalog catalog;
        HadoopFileIO fileIO = null;
        if (CATALOG_TYPE_REST.equals(properties.get("type"))) {
            properties.forEach(hadoopConf::set);
            var rest = new RESTCatalog();
            try {
                rest.setConf(hadoopConf);
                rest.initialize(catalogName, properties);
            } catch (RuntimeException | Error failure) {
                ResourceCloser.closeOnFailure(failure, rest);
                throw failure;
            }
            catalog = rest;
        } else {
            // Keep local catalogs isolated from Hadoop's process-wide filesystem cache.
            hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.RawLocalFileSystem.class.getName());
            hadoopConf.setBoolean("fs.file.impl.disable.cache", true);
            properties.forEach(hadoopConf::set);
            fileIO = new HadoopFileIO(hadoopConf);
            try {
                catalog = CatalogUtil.buildIcebergCatalog(catalogName, properties, hadoopConf);
            } catch (RuntimeException | Error failure) {
                ResourceCloser.closeOnFailure(failure, fileIO);
                throw failure;
            }
        }
        try {
            return new IcebergConnector(catalogName, catalog, fileIO, accessibleKeys);
        } catch (RuntimeException | Error failure) {
            if (fileIO != null) ResourceCloser.closeOnFailure(failure, fileIO);
            if (catalog instanceof AutoCloseable owned) ResourceCloser.closeOnFailure(failure, owned);
            throw failure;
        }
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

    private static Map<String, String> normalizeCatalogProperties(Map<String, String> config) {
        Map<String, String> properties = new HashMap<>(config);
        String type = catalogType(config.get("type"), "type");
        String alias = catalogType(properties.remove(CATALOG_TYPE_KEY), CATALOG_TYPE_KEY);
        if (type != null && alias != null && !type.equals(alias)) {
            throw new IllegalArgumentException("Conflicting Iceberg catalog properties: type and catalog-type");
        }
        String selected = type != null ? type : alias;
        if (properties.containsKey("catalog-impl")) {
            requireProperty(properties, "catalog-impl", "custom");
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

}
