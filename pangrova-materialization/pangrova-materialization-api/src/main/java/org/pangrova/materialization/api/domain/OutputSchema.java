package org.pangrova.materialization.api.domain;

import org.pangrova.common.crypto.Sha256Digests;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Immutable output schema of a materialization definition.
 *
 * <p>The canonical fingerprint is part of the definition identity and is
 * compared before publication and before transparent rewrite. Column names are
 * unique case-insensitively, matching SQL identifier semantics.
 *
 * @param columns ordered output columns
 */
public record OutputSchema(List<ColumnDefinition> columns) {

    public OutputSchema {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("output schema must contain at least one column");
        }
        columns = List.copyOf(columns);
        Set<String> names = new HashSet<>();
        for (int i = 0; i < columns.size(); i++) {
            ColumnDefinition column = columns.get(i);
            if (column.ordinal() != i) {
                throw new IllegalArgumentException(
                        "column ordinals must be contiguous from zero; expected " + i
                                + " but found " + column.ordinal());
            }
            if (!names.add(column.name().toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException("duplicate output column: " + column.name());
            }
        }
    }

    /**
     * Returns the canonical SHA-256 fingerprint of this schema.
     *
     * @return lowercase hex fingerprint
     */
    public String fingerprint() {
        StringBuilder canonical = new StringBuilder("schema:");
        for (ColumnDefinition column : columns) {
            canonical.append('|')
                    .append(column.ordinal())
                    .append(':')
                    .append(column.name())
                    .append(':')
                    .append(column.typeName())
                    .append(':')
                    .append(column.nullable());
        }
        return Sha256Digests.sha256Hex(canonical.toString());
    }
}
