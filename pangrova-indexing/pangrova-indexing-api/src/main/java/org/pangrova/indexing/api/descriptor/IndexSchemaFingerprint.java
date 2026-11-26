package org.pangrova.indexing.api.descriptor;

import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata.Column;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.util.DigestUtils;

import java.util.List;
import java.util.Locale;

/** Canonical binding for the exact indexed columns and their connector types. */
@Experimental
public final class IndexSchemaFingerprint {
    private IndexSchemaFingerprint() {}

    /** Computes the SHA-256 binding for the ordered indexed columns. */
    public static String of(List<String> indexedColumns, List<Column> availableColumns) {
        if (indexedColumns == null || indexedColumns.isEmpty() || availableColumns == null) {
            throw new IllegalArgumentException("Indexed columns and source schema are required");
        }
        StringBuilder canonical = new StringBuilder();
        for (String indexed : indexedColumns) {
            Column match = availableColumns.stream()
                    .filter(column -> column.name().equalsIgnoreCase(indexed))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Indexed column is absent from the source schema: " + indexed));
            String column = match.toString();
            canonical.append(column.length()).append(':').append(column);
        }
        return DigestUtils.sha256Hex(canonical.toString());
    }

    public static String normalizeColumnName(String name) {
        return name == null ? "" : name.toLowerCase(Locale.ROOT);
    }
}
