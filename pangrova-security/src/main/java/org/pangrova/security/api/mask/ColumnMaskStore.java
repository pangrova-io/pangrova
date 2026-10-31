package org.pangrova.security.api.mask;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;

/**
 * SPI contract for column masking policy storage.
 * Stable contract — implementations reside in pangrova-security.
 */
@Experimental
public interface ColumnMaskStore {

    /**
     * Returns all masks for the given column.
     */
    List<ColumnMaskRecord> getMasks(String catalog, String schema, String table, String column);

    /** Reads only the captured identity for rename/retirement; names remain diagnostic scope. */
    default List<ColumnMaskRecord> getMasks(java.util.UUID tableId,
            String catalog, String schema, String table, String column) {
        return getMasks(catalog, schema, table, column).stream()
                .filter(mask -> java.util.Objects.equals(tableId, mask.tableId())).toList();
    }

    /**
     * Persists a column mask. If a mask with the same name already exists for the
     * same table/column, the implementation should replace it.
     */
    void saveMask(ColumnMaskRecord mask);

    /**
     * Deletes a column mask by its unique identifier.
     */
    void deleteMask(long maskId);
}
