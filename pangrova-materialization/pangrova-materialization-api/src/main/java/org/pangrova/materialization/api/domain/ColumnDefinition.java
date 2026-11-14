package org.pangrova.materialization.api.domain;

import static org.pangrova.common.util.Preconditions.requireText;

/**
 * One output column of a materialization definition.
 *
 * @param name column name
 * @param typeName canonical SQL type name
 * @param nullable whether null values are permitted
 * @param ordinal zero-based position in the output schema
 */
public record ColumnDefinition(String name, String typeName, boolean nullable, int ordinal) {

    public ColumnDefinition {
        name = requireText(name, "column name");
        typeName = requireText(typeName, "column type");
        if (ordinal < 0) {
            throw new IllegalArgumentException("column ordinal must not be negative");
        }
    }
}
