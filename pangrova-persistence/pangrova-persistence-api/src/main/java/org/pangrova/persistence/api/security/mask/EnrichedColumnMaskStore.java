package org.pangrova.persistence.api.security.mask;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.spi.security.mask.ColumnMask;
import org.pangrova.spi.security.mask.ColumnMaskRecord;

import java.util.List;

/**
 * Security-module column mask store extending the SPI contract with enriched ColumnMask objects.
 */
@Experimental
public interface EnrichedColumnMaskStore extends org.pangrova.persistence.api.security.mask.ColumnMaskStore {

    /**
     * Returns all masks for the given column as security-enriched ColumnMask objects.
     */
    List<ColumnMask> getColumnMasks(String catalog, String schema, String table, String column);

    /** Returns the table's masks independently of projected or currently declared columns. */
    List<ColumnMask> getTableMasks(String catalog, String schema, String table);

    /**
     * Persists a security-enriched ColumnMask.
     */
    void saveColumnMask(ColumnMask mask);

    @Override
    default List<ColumnMaskRecord> getMasks(String catalog, String schema, String table, String column) {
        return getColumnMasks(catalog, schema, table, column).stream()
                .map(ColumnMask::toRecord)
                .toList();
    }

    @Override
    default void saveMask(ColumnMaskRecord mask) {
        saveColumnMask(ColumnMask.fromRecord(mask));
    }
}
