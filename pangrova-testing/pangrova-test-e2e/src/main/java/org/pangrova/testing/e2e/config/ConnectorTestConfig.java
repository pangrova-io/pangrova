package org.pangrova.testing.e2e.config;

import java.util.Map;
import java.util.Objects;

/**
 * Immutable configuration for connector-specific E2E tests.
 * Captures catalog name, schema, connector type, and connector properties.
 */
public record ConnectorTestConfig(
    String catalogName,
    String schemaName,
    String connectorType,
    Map<String, String> properties
) {

    public ConnectorTestConfig {
        Objects.requireNonNull(catalogName, "catalogName must not be null");
        Objects.requireNonNull(schemaName, "schemaName must not be null");
        Objects.requireNonNull(connectorType, "connectorType must not be null");
        properties = Map.copyOf(properties);
    }

    /** Create an Iceberg connector config backed by S3/MinIO. */
    public static ConnectorTestConfig iceberg(
            String catalogName, String schemaName, String s3Endpoint) {
        return new ConnectorTestConfig(catalogName, schemaName, "iceberg", Map.of(
            "warehouse", "s3a://warehouse/",
            "s3.endpoint", s3Endpoint,
            "s3.access-key", "minioadmin",
            "s3.secret-key", "minioadmin"
        ));
    }

    /** Create an Iceberg connector config backed by local filesystem (Hadoop Catalog). */
    public static ConnectorTestConfig icebergLocal(
            String catalogName, String schemaName, String warehousePath) {
        return new ConnectorTestConfig(catalogName, schemaName, "iceberg", Map.of(
            "type", "hadoop",
            "warehouse", warehousePath
        ));
    }

    /** Create an Iceberg connector config with extra properties merged in. */
    public static ConnectorTestConfig icebergLocal(
            String catalogName, String schemaName, String warehousePath,
            Map<String, String> extraProperties) {
        var props = new java.util.HashMap<String, String>();
        props.put("type", "hadoop");
        props.put("warehouse", warehousePath);
        props.putAll(extraProperties);
        return new ConnectorTestConfig(catalogName, schemaName, "iceberg", props);
    }

    /** Create a Paimon connector config backed by local filesystem. */
    public static ConnectorTestConfig paimon(
            String catalogName, String schemaName, String warehousePath) {
        return new ConnectorTestConfig(catalogName, schemaName, "paimon", Map.of(
            "warehouse", warehousePath
        ));
    }

}
