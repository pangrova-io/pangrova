package org.pangrova.connector.paimon.handle;

import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;

/**
 * Handle referencing a Paimon column by field ID, name, and Pangrova type.
 */
public record PaimonColumnHandle(
        int fieldId,
        String name,
        PangrovaType type) implements ColumnHandle {

    @Override
    public String columnName() {
        return name;
    }
}
