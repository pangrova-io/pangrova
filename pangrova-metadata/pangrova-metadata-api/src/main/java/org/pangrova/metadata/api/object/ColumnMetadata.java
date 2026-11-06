package org.pangrova.metadata.api.object;

import java.util.Map;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Immutable column metadata with name, type, nullability, comment, and tags.
 */
public record ColumnMetadata(
        String name,
        ColumnType type,
        boolean nullable,
        String comment,
        Map<String, String> tags) {

    public ColumnMetadata {
        checkNotNull(name, "name must not be null");
        checkNotNull(type, "type must not be null");
        comment = comment == null ? "" : comment;
        tags = tags == null ? Map.of() : Map.copyOf(tags);
    }
}
