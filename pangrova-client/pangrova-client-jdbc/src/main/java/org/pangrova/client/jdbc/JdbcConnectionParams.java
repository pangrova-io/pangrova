package org.pangrova.client.jdbc;

import java.util.Map;

/**
 * Immutable JDBC connection parameters parsed from a JDBC URL.
 *
 * @param host       coordinator hostname or IP
 * @param port       coordinator port (1-65535)
 * @param catalog    optional catalog name (nullable)
 * @param properties additional key-value connection properties
 */
public record JdbcConnectionParams(
        String host,
        int port,
        String catalog,
        Map<String, String> properties
) {

    public JdbcConnectionParams {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be null or blank");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535, got " + port);
        }
        properties = properties == null ? Map.of() : Map.copyOf(properties);
    }

    @Override public String toString() {
        return "JdbcConnectionParams[host=" + host + ", port=" + port
                + ", catalog=" + catalog + ", propertyKeys=" + properties.keySet() + "]";
    }
}
