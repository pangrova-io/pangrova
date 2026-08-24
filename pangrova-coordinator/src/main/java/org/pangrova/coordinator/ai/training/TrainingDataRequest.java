package org.pangrova.coordinator.ai.training;

import java.util.List;
import java.util.Objects;

/**
 * Parsed descriptor for a training data request received via the data streaming endpoint.
 *
 * <p>Clients encode this as JSON in the FlightDescriptor command field. The server
 * parses and validates it before planning the scan.</p>
 *
 * @param table       fully qualified table name (catalog.schema.table)
 * @param version     version reference: "tag:name", "branch:name", or "snapshot:id"
 * @param split       hash-based split expression (e.g., "hash(id) % 10 < 8")
 * @param columns     projected column names (null means all columns)
 * @param samplingRate uniform random sampling rate in (0.0, 1.0]; null means no sampling
 */
public record TrainingDataRequest(
        String table,
        String version,
        String split,
        List<String> columns,
        Double samplingRate) {

    public TrainingDataRequest {
        Objects.requireNonNull(table, "table must not be null");
        if (table.isBlank()) {
            throw new IllegalArgumentException("table must not be blank");
        }
        if (columns != null) {
            columns = List.copyOf(columns);
        }
        if (samplingRate != null && (samplingRate <= 0.0 || samplingRate > 1.0)) {
            throw new IllegalArgumentException(
                    "samplingRate must be in (0.0, 1.0], got: " + samplingRate);
        }
    }

    /**
     * Parses the version reference into its type and value.
     *
     * @return parsed version reference, or null if no version was specified
     */
    public VersionReference parsedVersion() {
        if (version == null || version.isBlank()) {
            return null;
        }
        int colonIndex = version.indexOf(':');
        if (colonIndex <= 0 || colonIndex >= version.length() - 1) {
            throw new IllegalArgumentException(
                    "Invalid version format, expected 'type:value' (tag:name, branch:name, snapshot:id), got: "
                            + version);
        }
        String type = version.substring(0, colonIndex);
        String value = version.substring(colonIndex + 1);
        return new VersionReference(type, value);
    }

    /**
     * Parsed version reference with type and value.
     *
     * @param type  one of "tag", "branch", "snapshot"
     * @param value the reference name or snapshot ID
     */
    public record VersionReference(String type, String value) {
        public VersionReference {
            Objects.requireNonNull(type, "type must not be null");
            Objects.requireNonNull(value, "value must not be null");
            if (!type.equals("tag") && !type.equals("branch") && !type.equals("snapshot")) {
                throw new IllegalArgumentException(
                        "version type must be 'tag', 'branch', or 'snapshot', got: " + type);
            }
        }
    }
}
