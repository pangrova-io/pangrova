package org.pangrova.security.policy.builtin.authorization;

import org.pangrova.file.api.space.FileSpaceType;
import org.pangrova.security.api.policy.PrivilegeGrant;

/** File-specific privilege scope matching for the built-in authorization provider. */
final class FileSpaceGrantMatcher {
    private FileSpaceGrantMatcher() {}

    /** Namespace grants cascade; table, column, storage profile and the other space kind do not. */
    static boolean covers(PrivilegeGrant grant, FileSpaceType type, String catalog, String schema, String space) {
        if (grant.isColumnScoped() || "STORAGE_PROFILE".equalsIgnoreCase(grant.objectType())
                || isOtherFileSpace(grant, type)) {
            return false;
        }
        if (!isFileSpace(grant) && !isNamespace(grant)) {
            return false;
        }
        return matchesOrWild(grant.catalog(), catalog)
                && matchesOrWild(grant.schema(), schema)
                && matchesOrWild(grant.table(), space);
    }

    private static boolean isOtherFileSpace(PrivilegeGrant grant, FileSpaceType type) {
        FileSpaceType other = type == FileSpaceType.VOLUME ? FileSpaceType.STAGE : FileSpaceType.VOLUME;
        return other.name().equalsIgnoreCase(grant.objectType());
    }

    private static boolean isNamespace(PrivilegeGrant grant) {
        return "SCHEMA".equalsIgnoreCase(grant.objectType()) || "CATALOG".equalsIgnoreCase(grant.objectType());
    }

    private static boolean isFileSpace(PrivilegeGrant grant) {
        return FileSpaceType.VOLUME.name().equalsIgnoreCase(grant.objectType())
                || FileSpaceType.STAGE.name().equalsIgnoreCase(grant.objectType());
    }

    private static boolean matchesOrWild(String pattern, String value) {
        return pattern == null || "*".equals(pattern) || pattern.equals(value);
    }
}
