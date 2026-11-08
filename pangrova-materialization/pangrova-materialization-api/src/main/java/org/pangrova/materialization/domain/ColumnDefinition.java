package org.pangrova.materialization.domain;

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
        name = Require.text(name, "column name");
        typeName = Require.text(typeName, "column type");
        if (ordinal < 0) {
            throw new IllegalArgumentException("column ordinal must not be negative");
        }
    }
}
