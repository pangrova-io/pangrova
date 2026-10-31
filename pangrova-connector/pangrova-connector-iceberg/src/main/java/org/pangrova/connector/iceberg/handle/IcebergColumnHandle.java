package org.pangrova.connector.iceberg.handle;

import org.pangrova.connector.spi.data.type.PangrovaType;
import org.pangrova.connector.spi.metadata.table.ColumnHandle;

/**
 * Handle referencing an Iceberg column by field ID, name, and Pangrova type.
 * The field ID is preserved for Iceberg schema evolution support.
 */
public record IcebergColumnHandle(
        int fieldId,
        String name,
        PangrovaType type) implements ColumnHandle {

    @Override
    public String columnName() {
        return name;
    }
}
