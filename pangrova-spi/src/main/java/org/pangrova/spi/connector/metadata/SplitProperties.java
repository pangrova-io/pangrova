package org.pangrova.spi.connector.metadata;

import java.util.Map;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Typed split metadata for native engine serialization.
 *
 * <p>Replaces {@code Map<String, Object>} with explicit fields, ensuring
 * field semantics and type information are preserved across serialization
 * boundaries. Connector implementations populate this via
 * {@link ConnectorSplit#typedSplitProperties()}.</p>
 *
 * @param filePath       data file path
 * @param offset         byte offset within the file
 * @param length         byte length of the split
 * @param partitionInfo  partition column name-value pairs
 * @param connectorProperties connector-specific metadata (e.g., iceberg_info, paimon_info)
 */
@Experimental
public record SplitProperties(
        String filePath,
        long offset,
        long length,
        Map<String, String> partitionInfo,
        Map<String, String> connectorProperties
) {

    public SplitProperties {
        if (filePath == null || filePath.isBlank()) {
            throw new IllegalArgumentException("filePath must not be null or blank");
        }
        partitionInfo = partitionInfo == null ? Map.of() : Map.copyOf(partitionInfo);
        connectorProperties = connectorProperties == null ? Map.of() : Map.copyOf(connectorProperties);
    }
}
