package org.pangrova.connector.spi.index;

import java.util.List;
import java.util.Objects;

/**
 * Describes a native index capability declared by a connector.
 *
 * @param indexType   the native index type
 * @param columnNames columns covered by this capability (defensively copied)
 * @param description human-readable description of the capability
 */
public record NativeIndexCapability(
        NativeIndexType indexType,
        List<String> columnNames,
        String description
) {

    public NativeIndexCapability {
        Objects.requireNonNull(indexType, "indexType must not be null");
        Objects.requireNonNull(columnNames, "columnNames must not be null");
        columnNames = List.copyOf(columnNames);
        if (description == null) {
            description = "";
        }
    }

    /**
     * Checks whether this capability covers the specified column.
     *
     * @param column column name to check
     * @return true if this capability covers the column
     */
    public boolean coversColumn(String column) {
        return columnNames.contains(column);
    }
}
