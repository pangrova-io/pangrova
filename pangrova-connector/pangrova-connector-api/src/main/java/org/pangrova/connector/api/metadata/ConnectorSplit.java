package org.pangrova.connector.api.metadata;

import org.pangrova.common.api.annotation.Experimental;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Marker interface for connector-specific split.
 *
 * <p>A split represents a portion of a table's data that can be processed
 * independently. Each connector implementation provides its own class
 * implementing this interface to describe how to read a specific chunk of data.</p>
 */
@Experimental
public interface ConnectorSplit {

    /**
     * Returns split metadata as key-value pairs for native engine serialization.
     *
     * <p>Standard keys: file_path, file_format, start_offset, length, connector_id.
     * Connector-specific keys are prefixed (e.g., iceberg.snapshot_id, paimon.bucket_id).
     *
     * <p>Compatibility: additive default method — no migration required for existing implementations.
     */
    default Map<String, String> splitProperties() {
        return Map.of();
    }

    /**
     * Returns strong-typed split properties for native engine serialization.
     *
     * <p>Provides explicit fields (filePath, offset, length, partitionInfo) plus
     * a typed connectorProperties map for connector-specific metadata.</p>
     *
     * <p>Default implementation bridges from {@link #splitProperties()} for
     * backward compatibility. Connector implementations should override this method.</p>
     */
    default SplitProperties typedSplitProperties() {
        Map<String, String> props = splitProperties();

        String filePath = props.getOrDefault("file_path", "unknown");
        long offset = parseLong(props.get("start_offset"), 0L);
        long length = parseLong(props.get("length"), 0L);

        Map<String, String> connectorProps = new LinkedHashMap<>(props);
        connectorProps.remove("file_path");
        connectorProps.remove("start_offset");
        connectorProps.remove("length");
        connectorProps.remove("partition_values");

        return new SplitProperties(filePath, offset, length, Map.of(),
                connectorProps.isEmpty() ? Map.of() : Map.copyOf(connectorProps));
    }

    /**
     * Returns the serialized SplitInfo proto bytes for native engine dispatch.
     *
     * <p>Connectors must override this method to build a {@code SplitInfo} proto
     * message directly for native engine consumption.</p>
     *
     * <p>The default implementation returns an empty byte array. Callers should
     * check {@code length > 0} and reject splits that do not implement proto
     * serialization.</p>
     *
     * @return serialized SplitInfo proto bytes, or empty array if not implemented
     */
    default byte[] toSplitInfoProto() {
        return new byte[0];
    }

    /**
     * Returns constraint predicates carried by this split for native-side split skipping.
     *
     * <p>Constraints include partition values and file-level column statistics (min/max)
     * that describe the data range within this split. The native execution engine uses
     * these predicates to skip irrelevant splits without reading file data.</p>
     *
     * <p>Default implementation returns {@link SplitConstraint#NONE}. Connector
     * implementations should override to provide partition values and file statistics.</p>
     *
     * @return split constraint predicates, never null
     */
    default SplitConstraint splitConstraint() {
        return SplitConstraint.NONE;
    }

    /**
     * Returns all data file paths covered by this split.
     *
     * <p>Most splits map 1:1 to a single data file. However, connectors may
     * merge multiple small files into one split or split a large file into
     * multiple splits (each covering the same file path). Index evaluation
     * uses this list to assess every covered file.</p>
     *
     * <p>Default implementation derives a single-element list from
     * {@link #typedSplitProperties()}.{@code filePath()}. Connectors with
     * multi-file splits must override this method.</p>
     *
     * @return non-null list of data file paths; empty list if unknown
     */
    default List<String> dataFilePaths() {
        try {
            String path = typedSplitProperties().filePath();
            return "unknown".equals(path) ? List.of() : List.of(path);
        } catch (Exception e) {
            return List.of();
        }
    }

    private static long parseLong(String value, long defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }
}
