package org.pangrova.spi.security;

import org.pangrova.spi.annotation.Experimental;

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
