package org.pangrova.connector.lance;

import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorContext;
import org.pangrova.spi.connector.ConnectorFactory;
import java.net.URI;
import java.nio.file.Path;
import java.util.Map;

/** Creates a persistent Lance catalog with a connector-owned warehouse and Native control port. */
public final class LanceConnectorFactory implements ConnectorFactory {
    private static final int MAX_URI_LENGTH = 8192;

    @Override public String getName() { return "lance"; }

    @Override public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        if (catalogName == null || catalogName.isBlank()) {
            throw new IllegalArgumentException("Lance catalog name is required");
        }
        String jdbcUrl = config.get("jdbc.url");
        if (jdbcUrl == null || !jdbcUrl.startsWith("jdbc:postgresql:")) {
            throw new IllegalArgumentException("Lance catalog requires jdbc.url for PostgreSQL");
        }
        String warehouse = warehouse(config.get("warehouse"));
        return new LanceConnector(catalogName, warehouse, config, context.nativeDatasetControlPlane());
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
