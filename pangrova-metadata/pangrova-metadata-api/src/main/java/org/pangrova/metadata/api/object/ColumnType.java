package org.pangrova.metadata.api.object;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

/**
 * Parameterized column type with precision, scale, and length.
 */
public record ColumnType(String baseType, int precision, int scale, int length) {

    public ColumnType {
        checkNotNull(baseType, "baseType must not be null");
    }

    public static ColumnType of(String baseType) {
        return new ColumnType(baseType, 0, 0, 0);
    }

    public static ColumnType decimal(int precision, int scale) {
        return new ColumnType("DECIMAL", precision, scale, 0);
    }

    public static ColumnType varchar(int length) {
        return new ColumnType("VARCHAR", 0, 0, length);
    }
}
