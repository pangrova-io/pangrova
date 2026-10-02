package org.pangrova.security.authz;

/**
 * Enumeration of grantable privileges in the Pangrova access control model.
 *
 * <p>Privilege hierarchy (highest to lowest):
 * <ul>
 *   <li>{@link #OWNER} — implies all privileges on the object</li>
 *   <li>{@link #ALL} — implies all specific privileges on the object</li>
 *   <li>Specific privileges (SELECT, INSERT, etc.)</li>
 * </ul>
 */
public enum Privilege {
    /** Ownership privilege — implies ALL privileges and cannot be revoked without ownership transfer. */
    OWNER,
    /** Meta-privilege that implies all specific privileges on the object. */
    ALL,
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    TRUNCATE,
    DROP,
    ALTER,
    CREATE_TABLE,
    CREATE_VIEW,
    DROP_SCHEMA,
    CREATE_SCHEMA,
    ALTER_SCHEMA,
    CREATE_CATALOG,
    DROP_CATALOG,
    ALTER_CATALOG,
    RENAME_TABLE,
    SHOW_SCHEMAS,
    SHOW_TABLES,
    SHOW_COLUMNS,
    EXECUTE_QUERY,
    KILL_QUERY,
    VIEW_QUERY,
    SET_SESSION;

    /**
     * Returns true if this privilege implies the given required privilege
     * according to the hierarchy: OWNER > ALL > specific.
     */
    public boolean implies(Privilege required) {
        if (this == required) return true;
        if (this == OWNER) return true;
        if (this == ALL) return required != OWNER;
        return false;
    }
}
