package org.pangrova.connector.spi.metadata.table;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Describes a type promotion for a column during schema evolution.
 *
 * <p>Used when a column type has been widened (e.g., int → long, float → double)
 * and data files written under the old type need casting at read time.</p>
 *
 * @param fieldId    column field ID that was promoted
 * @param sourceType original type in the data file (e.g., "int", "float")
 * @param targetType current type in the table schema (e.g., "long", "double")
 * @param columnName column name in the current table schema for name-based matching;
 *                   empty string when not available (legacy callers)
 */
@Experimental
public record TypePromotion(
        int fieldId,
        String sourceType,
        String targetType,
        String columnName) {

    /**
     * Legacy constructor without columnName — defaults to empty string.
     */
    public TypePromotion(int fieldId, String sourceType, String targetType) {
        this(fieldId, sourceType, targetType, "");
    }

    public TypePromotion {
        if (sourceType == null || sourceType.isBlank()) {
            throw new IllegalArgumentException("sourceType must not be null or blank");
        }
        if (targetType == null || targetType.isBlank()) {
            throw new IllegalArgumentException("targetType must not be null or blank");
        }
        if (columnName == null) {
            columnName = "";
        }
    }
}
