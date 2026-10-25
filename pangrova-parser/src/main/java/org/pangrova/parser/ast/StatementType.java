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
    CREATE_MATERIALIZED_VIEW,
    REFRESH_MATERIALIZED_VIEW,
    SHOW_SCHEMAS,

    // DDL statements (org.pangrova.parser.ast.statement)
    ALTER_CATALOG,
    ALTER_DYNAMIC_TABLE,
    ALTER_MATERIALIZED_VIEW,
    ALTER_EMBEDDING_MODEL,
    ALTER_INDEX,
    ALTER_TABLE,
    ALTER_STAGE,
    ALTER_VOLUME,
    BACKFILL_COLUMN,
    CANCEL_BACKFILL,
    CREATE_BRANCH,
    CREATE_EMBEDDING_MODEL,
    CREATE_INDEX,
    CREATE_SCHEMA,
    CREATE_SEMANTIC_VIEW,
    CREATE_FEATURE_VIEW,
    CREATE_STAGE,
    CREATE_TABLE,
    CREATE_TABLE_AS_SELECT,
    CREATE_TAG,
    CREATE_VIEW,
    CREATE_VOLUME,
    DESCRIBE_CATALOG,
    DESCRIBE_DYNAMIC_TABLE,
    DESCRIBE_MATERIALIZED_VIEW,
    DESCRIBE_STAGE,
    DESCRIBE_TABLE,
    DESCRIBE_VOLUME,
    DROP_BRANCH,
    DROP_CATALOG,
    DROP_DYNAMIC_TABLE,
    DROP_EMBEDDING_MODEL,
    DROP_MATERIALIZED_VIEW,
    DROP_INDEX,
    DROP_STAGE,
    DROP_TABLE,
    DROP_TAG,
    DROP_VIEW,
    DROP_VOLUME,
    EXPLAIN,
    GRANT_TENANT,
    INCREMENTAL_READ,
    RECLUSTER_TABLE,
    PUBLISH_SEMANTIC_VIEW,
    PUBLISH_FEATURE_VIEW,
    MATERIALIZE_FEATURE_VIEW,
    LIST_STAGE,
    REMOVE_STAGE,
    REVOKE_TENANT,
    SET_CLUSTERING_ORDER,
    SHOW_BACKFILL_STATUS,
    SHOW_BRANCHES,
    SHOW_CATALOGS,
    SHOW_CLUSTERING_STATUS,
    SHOW_DYNAMIC_TABLES,
    SHOW_EMBEDDING_MODELS,
    SHOW_MATERIALIZATIONS,
    SHOW_MATERIALIZATION_DEPENDENCIES,
    SHOW_MATERIALIZATION_GC_HISTORY,
    SHOW_MATERIALIZATION_REFRESH_HISTORY,
    SHOW_MATERIALIZED_VIEWS,
    SHOW_INDEXES,
    SHOW_SEMANTIC_VIEWS,
    SHOW_STAGES,
    SHOW_METADATA,
    SHOW_TABLES,
    SHOW_TAGS,
    SHOW_VIEWS,
    SHOW_VOLUMES,
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
    ANALYZE,

    // Stream statements (org.pangrova.parser.ast.statement.stream)
    ALTER_STREAM,
    CREATE_STREAM,
    DESCRIBE_STREAM,
    DROP_STREAM,
    SHOW_STREAMS,

    // Session statements
    SET_SESSION
}
