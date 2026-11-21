package org.pangrova.connector.api.metadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Immutable configuration for column-level encryption in Parquet files.
 *
 * <p>Parsed from table properties and passed to connectors via write context.
 * The coordinator resolves key IDs to actual key bytes before passing to the connector,
 * so connectors never need direct access to the key management service.
 *
 * <p>Table property format:
 * <ul>
 *   <li>{@code encryption.column.keys} — {@code keyId1:col1,col2;keyId2:col3}</li>
 *   <li>{@code encryption.footer.key} — key ID for footer encryption</li>
 * </ul>
 */
@Experimental
public record ColumnEncryptionConfig(
        List<ColumnKeyMapping> columnKeys,
        Optional<String> footerKeyId
) {

    /**
     * Associates a key identifier with the columns encrypted by that key.
     */
    public record ColumnKeyMapping(String keyId, Set<String> columns) {}

    public static final String PROPERTY_COLUMN_KEYS = "encryption.column.keys";
    public static final String PROPERTY_FOOTER_KEY = "encryption.footer.key";

    /**
     * Parses encryption configuration from table properties.
     *
     * @param properties table properties
     * @return parsed config, or empty if no encryption configured
     */
    public static Optional<ColumnEncryptionConfig> fromProperties(Map<String, String> properties) {
        String columnKeysValue = properties.get(PROPERTY_COLUMN_KEYS);
        if (columnKeysValue == null || columnKeysValue.isBlank()) {
            return Optional.empty();
        }
        List<ColumnKeyMapping> mappings = parseColumnKeys(columnKeysValue);
        Optional<String> footerKey = Optional.ofNullable(properties.get(PROPERTY_FOOTER_KEY))
                .filter(s -> !s.isBlank());
        return Optional.of(new ColumnEncryptionConfig(mappings, footerKey));
    }

    /**
     * Returns the key ID protecting a given column, or empty if unencrypted.
     */
    public Optional<String> keyForColumn(String columnName) {
        for (ColumnKeyMapping mapping : columnKeys) {
            if (mapping.columns().contains(columnName)) {
                return Optional.of(mapping.keyId());
            }
        }
        return Optional.empty();
    }

    /**
     * Returns all column names that are encrypted.
     */
    public Set<String> encryptedColumns() {
        return columnKeys.stream()
                .flatMap(m -> m.columns().stream())
                .collect(Collectors.toUnmodifiableSet());
    }

    private static List<ColumnKeyMapping> parseColumnKeys(String value) {
        String[] segments = value.split(";");
        return java.util.Arrays.stream(segments)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(ColumnEncryptionConfig::parseSegment)
                .toList();
    }

    private static ColumnKeyMapping parseSegment(String segment) {
        int colonIdx = segment.lastIndexOf(':');
        if (colonIdx <= 0 || colonIdx == segment.length() - 1) {
            throw new IllegalArgumentException(
                    "Invalid encryption.column.keys segment: '" + segment
                            + "'. Expected format: keyId:col1,col2");
        }
        String keyId = segment.substring(0, colonIdx).trim();
        String[] cols = segment.substring(colonIdx + 1).split(",");
        Set<String> columns = java.util.Arrays.stream(cols)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
        if (columns.isEmpty()) {
            throw new IllegalArgumentException(
                    "No columns specified for key '" + keyId + "' in encryption.column.keys");
        }
        return new ColumnKeyMapping(keyId, columns);
    }
}
