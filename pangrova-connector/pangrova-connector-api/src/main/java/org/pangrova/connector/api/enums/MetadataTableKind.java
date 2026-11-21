package org.pangrova.connector.api.enums;

import java.util.Locale;
import java.util.Optional;

import org.pangrova.common.api.annotation.Stable;

/**
 * SQL metadata suffixes with engine Native scan routes. Individual connectors
 * still resolve their supported kinds and bind the exact metadata source.
 * Parsing, schema lookup and base-table authorization share this vocabulary.
 */
@Stable(since = "1.0")
public enum MetadataTableKind {
    SNAPSHOTS, HISTORY, REFS, TAGS, OPTIONS, SCHEMAS, BRANCHES, MANIFESTS, FILES, ENTRIES, PARTITIONS;

    private final String suffix = name().toLowerCase(Locale.ROOT);

    public String suffix() {
        return suffix;
    }

    /** Recognizes only implemented engine suffixes, without admitting an unknown base-table fallback. */
    public static Optional<MetadataTableKind> fromSuffix(String suffix) {
        if (suffix != null) {
            for (var kind : values()) {
                if (kind.suffix.equalsIgnoreCase(suffix)) return Optional.of(kind);
            }
        }
        return Optional.empty();
    }

    /** A metadata relation requires both a nonempty base name and a recognized suffix. */
    public static Optional<MetadataTableKind> fromTableName(String table) {
        if (table == null) return Optional.empty();
        int separator = table.lastIndexOf('$');
        return separator > 0 ? fromSuffix(table.substring(separator + 1)) : Optional.empty();
    }

    /** Preserves the exact base identity; unsupported names remain ordinary names. */
    public static String baseTableName(String table) {
        return fromTableName(table).isPresent() ? table.substring(0, table.lastIndexOf('$')) : table;
    }
}
