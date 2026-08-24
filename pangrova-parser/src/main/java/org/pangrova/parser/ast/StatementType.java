package org.pangrova.parser.ast;

/**
 * Enumerates all concrete statement types in the AST hierarchy.
 *
 * <p>Enables exhaustive switch expressions over {@link Statement} subtypes,
 * providing compile-time completeness checking. Adding a new statement type
 * requires adding a corresponding enum value here; any switch expression that
 * does not cover the new value will fail to compile.</p>
 */
public enum StatementType {
    // Same package (org.pangrova.parser.ast)
    QUERY,
    SET_QUERY,
    CTE_QUERY,
    INSERT,
    CREATE_CATALOG,
    CREATE_CONTINUOUS_QUERY,
    CREATE_DYNAMIC_TABLE,
    SHOW_SCHEMAS,

    // DDL statements (org.pangrova.parser.ast.statement)
    ALTER_CATALOG,
    ALTER_DYNAMIC_TABLE,
    ALTER_EMBEDDING_MODEL,
    ALTER_INDEX,
    ALTER_TABLE,
    BACKFILL_COLUMN,
    CANCEL_BACKFILL,
    CREATE_BRANCH,
    CREATE_EMBEDDING_MODEL,
    CREATE_INDEX,
    CREATE_SCHEMA,
    CREATE_TABLE,
    CREATE_TABLE_AS_SELECT,
    CREATE_TAG,
    CREATE_VIEW,
    DESCRIBE_CATALOG,
    DESCRIBE_DYNAMIC_TABLE,
    DESCRIBE_TABLE,
    DROP_BRANCH,
    DROP_CATALOG,
    DROP_DYNAMIC_TABLE,
    DROP_EMBEDDING_MODEL,
    DROP_INDEX,
    DROP_TABLE,
    DROP_TAG,
    DROP_VIEW,
    EXPLAIN,
    GRANT_TENANT,
    INCREMENTAL_READ,
    RECLUSTER_TABLE,
    REVOKE_TENANT,
    SET_CLUSTERING_ORDER,
    SHOW_BACKFILL_STATUS,
    SHOW_BRANCHES,
    SHOW_CATALOGS,
    SHOW_CLUSTERING_STATUS,
    SHOW_DYNAMIC_TABLES,
    SHOW_EMBEDDING_MODELS,
    SHOW_INDEXES,
    SHOW_METADATA,
    SHOW_TABLES,
    SHOW_TAGS,
    SHOW_VIEWS,
    USE_BRANCH,

    // DML statements (org.pangrova.parser.ast.statement.dml)
    DELETE,
    INSERT_OVERWRITE,
    INSERT_SELECT,
    MERGE,
    UPDATE,

    // Maintenance statements (org.pangrova.parser.ast.statement.maintenance)
    CALL,
    COMPACT,
    OPTIMIZE,
    VACUUM,

    // Stream statements (org.pangrova.parser.ast.statement.stream)
    ALTER_STREAM,
    CREATE_STREAM,
    DESCRIBE_STREAM,
    DROP_STREAM,
    SHOW_STREAMS,

    // Session statements
    SET_SESSION
}
