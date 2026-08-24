package org.pangrova.connector.iceberg;

import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorContext;
import org.pangrova.spi.connector.ConnectorFactory;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory for creating Iceberg connector instances with real Catalog.
 * Supports hadoop (default) and REST catalog types.
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
        String catalogType = config.get(CATALOG_TYPE_KEY);
        if (CATALOG_TYPE_REST.equalsIgnoreCase(catalogType)) {
            return new CatalogAndFileIO(buildRestCatalog(catalogName, config), null);
        }
        return buildHadoopCatalog(catalogName, config);
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

    private CatalogAndFileIO buildHadoopCatalog(String catalogName, Map<String, String> config) {
        Map<String, String> catalogProps = new HashMap<>(config);
        if (!catalogProps.containsKey("type") && !catalogProps.containsKey("catalog-impl")) {
            catalogProps.put("type", "hadoop");
        }
        if (!catalogProps.containsKey("warehouse")) {
            catalogProps.put("warehouse", System.getProperty("java.io.tmpdir") + "/iceberg-" + catalogName);
        }
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("hadoop.security.authentication", "simple");
        // Java 26 removed Subject.getSubject() which Hadoop's FileSystem cache uses
        // via UGI.getCurrentUser(). Using RawLocalFileSystem bypasses the cache key
        // path that triggers getSubject(). Also disable FileSystem cache entirely.
        hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.RawLocalFileSystem.class.getName());
        hadoopConf.setBoolean("fs.file.impl.disable.cache", true);
        config.forEach(hadoopConf::set);
        ensureHadoopUgiInitialized(hadoopConf);
        Catalog catalog = CatalogUtil.buildIcebergCatalog(catalogName, catalogProps, hadoopConf);
        org.apache.iceberg.hadoop.HadoopFileIO fileIO = new org.apache.iceberg.hadoop.HadoopFileIO(hadoopConf);
        return new CatalogAndFileIO(catalog, fileIO);
    }

    private static volatile boolean hadoopUgiInitialized = false;

    private static synchronized void ensureHadoopUgiInitialized(org.apache.hadoop.conf.Configuration conf) {
        if (hadoopUgiInitialized) {
            return;
        }
        // Java 26 removed Subject.getSubject() which Hadoop UGI's getCurrentUser()
        // calls unconditionally. We pre-set the loginUser so that getCurrentUser()
        // finds the cached value, but the bytecode still calls Subject.getSubject()
        // before checking loginUser. The only reliable fix is to also set the system
        // property so that UGI's static initializer uses simple auth.
        System.setProperty("HADOOP_USER_NAME", System.getProperty("user.name", "pangrova"));
        org.apache.hadoop.security.UserGroupInformation.setConfiguration(conf);
        org.apache.hadoop.security.UserGroupInformation ugi =
                org.apache.hadoop.security.UserGroupInformation.createRemoteUser(
                        System.getProperty("user.name", "pangrova"));
        org.apache.hadoop.security.UserGroupInformation.setLoginUser(ugi);
        hadoopUgiInitialized = true;
    }
}
