package org.pangrova.spi.security.policy;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Instant;

import org.pangrova.common.file.FileSpaceType;

/**
 * Represents a privilege grant to a role on a specific object (catalog/schema/table/column).
 * Null or "*" in pattern fields acts as a wildcard matching any value.
 *
 * @param cascade if true, revoking this grant also revokes grants made via withGrantOption
 */
@Experimental
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
        if (isColumnScoped() || isStorageProfile() || isFileSpace()) {
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
     * Volume and stage grants reuse the catalog/schema/table scope columns, with
     * {@code table} holding the space name. A null space name checks schema scope.
     * Schema and catalog grants cascade to spaces; table grants and grants on the
     * other file-space kind never apply to a volume or stage.
     */
    public boolean coversFileSpace(FileSpaceType type, String cat, String sch, String space) {
        if (isColumnScoped() || isStorageProfile() || isOtherFileSpace(type)) {
            return false;
        }
        if (!isFileSpace() && !isNamespace()) {
            return false;
        }
        return matchesOrWild(catalog, cat)
                && matchesOrWild(schema, sch)
                && matchesOrWild(table, space);
    }

    private boolean isOtherFileSpace(FileSpaceType type) {
        FileSpaceType other = type == FileSpaceType.VOLUME
                ? FileSpaceType.STAGE : FileSpaceType.VOLUME;
        return other.name().equalsIgnoreCase(objectType);
    }

    private boolean isNamespace() {
        return "SCHEMA".equalsIgnoreCase(objectType)
                || "CATALOG".equalsIgnoreCase(objectType);
    }

    private boolean isStorageProfile() {
        return "STORAGE_PROFILE".equalsIgnoreCase(objectType);
    }

    private boolean isFileSpace() {
        return FileSpaceType.VOLUME.name().equalsIgnoreCase(objectType)
                || FileSpaceType.STAGE.name().equalsIgnoreCase(objectType);
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
        if (isStorageProfile() || isFileSpace()) {
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
