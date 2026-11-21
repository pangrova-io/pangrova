package org.pangrova.connector.lance;

import org.pangrova.connector.api.constants.ConnectorFactoryContract;
import org.pangrova.connector.api.enums.ConnectorRole;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.plugin.ConnectorContext;
import org.pangrova.connector.api.plugin.ConnectorFactory;

import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/** Creates a persistent Lance catalog with a connector-owned warehouse and Native control port. */
public final class LanceConnectorFactory implements ConnectorFactory {
    private static final int MAX_URI_LENGTH = 8192;

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
        validatedWarehouse(config);
    }

    @Override public String getName() { return "lance"; }

    @Override public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        if (catalogName == null || catalogName.isBlank()) {
            throw new IllegalArgumentException("Lance catalog name is required");
        }
        String warehouse = validatedWarehouse(config);
        return new LanceConnector(catalogName, warehouse, config, context.nativeDatasetControlPlane(),
                context.managedSourceCatalog());
    }

    private static String validatedWarehouse(Map<String, String> config) {
        if (config.containsKey("jdbc.url") || config.containsKey("jdbc.user") || config.containsKey("jdbc.password")) {
            throw new IllegalArgumentException(
                    "Managed Lance uses the shared metadata authority; connector JDBC is forbidden");
        }
        return warehouse(config.get("warehouse"));
    }

    private static String warehouse(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Lance warehouse is required");
        requireCanonicalLocation(value);
        URI uri = URI.create(value);
        if (uri.getScheme() == null) {
            Path path = Path.of(value);
            if (!path.isAbsolute() || !path.normalize().equals(path)) {
                throw new IllegalArgumentException("Lance warehouse must be an absolute normalized path");
            }
        } else {
            validateWarehouseUri(uri);
        }
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    private static void requireCanonicalLocation(String value) {
        if (value.length() > MAX_URI_LENGTH || value.contains("%") || value.contains("\\")
                || value.contains("?") || value.contains("#")) {
            throw new IllegalArgumentException("Lance warehouse must be a bounded unencoded canonical location");
        }
        if (value.startsWith("file:") && !value.startsWith("file:///")) {
            throw new IllegalArgumentException(
                    "Lance file warehouse requires canonical file:/// URI; file:/ is rejected");
        }
    }

    private static void validateWarehouseUri(URI uri) {
        if (!("s3".equals(uri.getScheme()) || "file".equals(uri.getScheme()))
                || uri.getQuery() != null || uri.getFragment() != null || uri.getUserInfo() != null
                || !uri.normalize().equals(uri)) {
            throw new IllegalArgumentException("Lance warehouse must be a normalized local or S3 location");
        }
        validateWarehouseAuthority(uri);
    }

    private static void validateWarehouseAuthority(URI uri) {
        if ("s3".equals(uri.getScheme()) && (uri.getHost() == null || uri.getHost().isBlank())) {
            throw new IllegalArgumentException("Lance S3 warehouse requires a bucket");
        }
        if ("file".equals(uri.getScheme()) && !Path.of(uri).isAbsolute()) {
            throw new IllegalArgumentException("Lance file warehouse must be absolute");
        }
    }
}
