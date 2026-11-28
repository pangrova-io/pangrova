package org.pangrova.indexing.api.descriptor;

import org.pangrova.connector.api.metadata.table.ColumnSchemaEncoding;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata.Column;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.crypto.Sha256Digests;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Canonical binding for the exact indexed columns and their connector types. */
@Experimental
public final class IndexSchemaFingerprint {
    private static final String VERSION_PREFIX = "v2:";
    private static final byte[] DOMAIN = "pangrova:index-schema:v2".getBytes(StandardCharsets.UTF_8);

    private IndexSchemaFingerprint() {}

    /** Computes a versioned SHA-256 binding for the ordered indexed column declarations. */
    public static String of(List<String> indexedColumns, List<Column> availableColumns) {
        if (indexedColumns == null || indexedColumns.isEmpty() || availableColumns == null) {
            throw new IllegalArgumentException("Indexed columns and source schema are required");
        }
        MessageDigest digest = Sha256Digests.sha256();
        updateFrame(digest, DOMAIN);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(indexedColumns.size()).array());
        Set<String> resolvedNames = new HashSet<>();
        for (String indexed : indexedColumns) {
            Column match = resolveExactColumn(indexed, availableColumns);
            if (!resolvedNames.add(match.name())) {
                throw new IllegalArgumentException("Duplicate indexed column: " + match.name());
            }
            updateFrame(digest, ColumnSchemaEncoding.encode(match));
        }
        return VERSION_PREFIX + java.util.HexFormat.of().formatHex(digest.digest());
    }

    /** True only for the current version's canonical digest text. */
    public static boolean isCurrent(String fingerprint) {
        return fingerprint != null && fingerprint.startsWith(VERSION_PREFIX)
                && Sha256Digests.isSha256Hex(fingerprint.substring(VERSION_PREFIX.length()));
    }

    /** Resolves only the exact stored spelling of a previously bound connector column. */
    public static Column resolveExactColumn(String requested, List<Column> availableColumns) {
        requireName(requested);
        return uniqueColumn(requested, availableColumns.stream()
                .filter(column -> column.name().equals(requested)).toList());
    }

    /** Resolves an exact name first, otherwise a single unambiguous case-insensitive name. */
    public static Column resolveColumn(String requested, List<Column> availableColumns) {
        requireName(requested);
        List<Column> exact = availableColumns.stream().filter(column -> column.name().equals(requested)).toList();
        if (!exact.isEmpty()) {
            return uniqueColumn(requested, exact);
        }
        List<Column> folded = availableColumns.stream()
                .filter(column -> column.name().equalsIgnoreCase(requested)).toList();
        return uniqueColumn(requested, folded);
    }

    private static void requireName(String requested) {
        if (requested == null || requested.isBlank()) {
            throw new IllegalArgumentException("Indexed column name is required");
        }
    }

    private static Column uniqueColumn(String requested, List<Column> matches) {
        if (matches.isEmpty()) {
            throw new IllegalArgumentException("Indexed column is absent from the source schema: " + requested);
        }
        if (matches.size() != 1) {
            throw new IllegalArgumentException("Ambiguous indexed column in the source schema: " + requested);
        }
        return matches.get(0);
    }

    private static void updateFrame(MessageDigest digest, byte[] bytes) {
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }
}
