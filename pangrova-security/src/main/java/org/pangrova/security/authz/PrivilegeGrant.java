package org.pangrova.security.authz;

import java.time.Instant;

/**
 * Represents a privilege grant to a role on a specific object (catalog/schema/table/column).
 * Null or "*" in pattern fields acts as a wildcard matching any value.
 *
 * @param cascade if true, revoking this grant also revokes grants made via withGrantOption
 */
public record PrivilegeGrant(
        String id,
        String roleName,
        Privilege privilege,
        String objectType,
        String catalog,
        String schema,
        String table,
        String column,
        boolean withGrantOption,
        boolean cascade,
        String grantedBy,
        Instant grantedAt
) {

    /** Backward-compatible factory without audit fields. */
    public static PrivilegeGrant of(String roleName, Privilege privilege,
                                     String objectType, String catalog,
                                     String schema, String table, String column) {
        return new PrivilegeGrant(null, roleName, privilege, objectType,
                catalog, schema, table, column, false, false, null, null);
    }

    /**
     * Returns true if this grant covers the given table.
     * F-SEC-03: A column-scoped grant (non-null, non-wildcard column)
     * does NOT satisfy table-level SELECT checks.
     */
    public boolean coversTable(String cat, String sch, String tbl) {
        if (isColumnScoped() || "STORAGE_PROFILE".equalsIgnoreCase(objectType)) {
            return false;
        }
        return matchesOrWild(catalog, cat)
                && matchesOrWild(schema, sch)
                && matchesOrWild(table, tbl);
    }

    /** Storage-profile grants are isolated from table, schema, and catalog privileges. */
    public boolean coversStorageProfile(String profileName) {
        return "STORAGE_PROFILE".equalsIgnoreCase(objectType)
                && catalog != null
                && ("*".equals(catalog) || catalog.equals(profileName))
                && schema == null && table == null && column == null;
    }

    /**
     * Returns true if this grant has a specific column scope
     * (non-null and non-wildcard column field).
     */
    public boolean isColumnScoped() {
        return column != null && !"*".equals(column);
    }

    /**
     * Returns true if this grant covers the given column.
     */
    public boolean coversColumn(String cat, String sch, String tbl, String col) {
        if ("STORAGE_PROFILE".equalsIgnoreCase(objectType)) {
            return false;
        }
        // Column-level check: verify catalog/schema/table match directly
        // (without the isColumnScoped() gate that coversTable() applies).
        // A column-scoped grant must be able to satisfy column-level access checks.
        return matchesOrWild(catalog, cat)
                && matchesOrWild(schema, sch)
                && matchesOrWild(table, tbl)
                && matchesOrWild(column, col);
    }

    private boolean matchesOrWild(String pattern, String value) {
        return pattern == null || "*".equals(pattern) || pattern.equals(value);
    }
}
