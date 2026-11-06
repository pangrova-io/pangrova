package org.pangrova.security.api.mask;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.Set;

/**
 * Column masking policy that transforms column values based on user identity and role membership.
 * Wraps the SPI {@link ColumnMaskRecord} with security-specific behavior.
 */
@Experimental
public record ColumnMask(
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

    /**
     * Defensive copies of mutable lists to ensure immutability.
     * Rejects mask expressions containing DDL keywords.
     */
    public ColumnMask {
        applyToRoles = applyToRoles != null ? List.copyOf(applyToRoles) : List.of();
        exemptRoles = exemptRoles != null ? List.copyOf(exemptRoles) : List.of();
        org.pangrova.security.api.policy.SqlIdentifierValidator.rejectDdlInExpression(maskExpression);
    }

    /**
     * Creates a ColumnMask from an SPI ColumnMaskRecord.
     */
    public static ColumnMask fromRecord(ColumnMaskRecord record) {
        return new ColumnMask(
                record.maskId(), record.maskName(),
                record.catalogName(), record.schemaName(),
                record.tableName(), record.columnName(),
                record.maskExpression(),
                record.applyToRoles(), record.exemptRoles(),
                record.enabled(), record.tableId());
    }

    /**
     * Converts this mask to an SPI ColumnMaskRecord.
     */
    public ColumnMaskRecord toRecord() {
        return new ColumnMaskRecord(
                maskId, maskName, catalogName, schemaName,
                tableName, columnName, maskExpression,
                applyToRoles, exemptRoles, enabled, tableId);
    }

    /**
     * Returns true if the user is exempt from this mask (sees raw data).
     */
    public boolean isExempt(Set<String> effectiveRoles) {
        if (exemptRoles.isEmpty()) {
            return false;
        }
        return effectiveRoles.stream().anyMatch(exemptRoles::contains);
    }

    /**
     * Returns true if this mask applies to the given user.
     * An empty applyToRoles list means the mask applies to everyone.
     */
    public boolean appliesTo(Set<String> effectiveRoles) {
        if (applyToRoles.isEmpty()) {
            return true;
        }
        return effectiveRoles.stream().anyMatch(applyToRoles::contains);
    }
}
