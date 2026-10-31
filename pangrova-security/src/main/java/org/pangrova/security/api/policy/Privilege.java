package org.pangrova.security.api.policy;

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
    /** Manage account-scoped storage profile definitions. */
    MANAGE_STORAGE_PROFILE,
    /** View account-scoped storage profile metadata. */
    VIEW_STORAGE_PROFILE,
    /** Read object bytes through one exact storage profile. */
    READ_STORAGE_PROFILE,
    /** Create a managed Databricks-style volume in a schema. */
    CREATE_VOLUME,
    /** Create an external volume that binds an existing object-store prefix. */
    CREATE_EXTERNAL_VOLUME,
    /** Read one volume's metadata and file bytes. */
    READ_VOLUME,
    /** Write or delete files in one volume. */
    WRITE_VOLUME,
    /** Alter, rename, transfer, or drop one volume. */
    MANAGE_VOLUME,
    /** Create a Snowflake-style stage in a schema. */
    CREATE_STAGE,
    /** Discover one stage and its directory projection. */
    USAGE_STAGE,
    /** Read one stage's file bytes. */
    READ_STAGE,
    /** Write or remove files in one stage. */
    WRITE_STAGE,
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
