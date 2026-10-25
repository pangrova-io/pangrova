package org.pangrova.spi.security;

import java.util.List;

/**
 * Immutable column masking policy descriptor.
 * Stable SPI contract — governance and security modules interact through this type.
 *
 * @param maskId         unique identifier for the mask
 * @param maskName       human-readable name (e.g. "auto_pii_email")
 * @param catalogName    target catalog
 * @param schemaName     target schema
 * @param tableName      target table
 * @param columnName     target column
 * @param maskExpression mask expression to apply (e.g. "mask_email")
 * @param applyToRoles   roles this mask applies to (empty = all)
 * @param exemptRoles    roles exempt from this mask
 * @param enabled        whether the mask is active
 */
public record ColumnMaskRecord(
        long maskId,
        String maskName,
        String catalogName,
        String schemaName,
        String tableName,
        String columnName,
        String maskExpression,
        List<String> applyToRoles,
        List<String> exemptRoles,
        boolean enabled,
        java.util.UUID tableId
) {

    public ColumnMaskRecord {
        applyToRoles = applyToRoles == null ? List.of() : List.copyOf(applyToRoles);
        exemptRoles = exemptRoles == null ? List.of() : List.copyOf(exemptRoles);
    }
}
