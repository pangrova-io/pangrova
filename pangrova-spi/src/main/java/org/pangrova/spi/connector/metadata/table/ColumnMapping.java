package org.pangrova.spi.connector.metadata.table;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Maps a source column to a target column for schema evolution.
 *
 * <p>Used when reading data files written under an older schema. The field IDs
 * identify columns across schema versions (names may change, IDs are stable).</p>
 *
 * @param sourceFieldId source column field ID in the data file schema
 * @param targetFieldId target column field ID in the current table schema
 * @param sourceName    source column name in the data file schema
 * @param targetName    target column name in the current table schema
 */
@Experimental
public record ColumnMapping(
        int sourceFieldId,
        int targetFieldId,
        String sourceName,
        String targetName) {

    public ColumnMapping {
        if (sourceName == null || sourceName.isBlank()) {
            throw new IllegalArgumentException("sourceName must not be null or blank");
        }
        if (targetName == null || targetName.isBlank()) {
            throw new IllegalArgumentException("targetName must not be null or blank");
        }
    }
}
