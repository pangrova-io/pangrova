package org.pangrova.query.statement;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.query.api.enums.StatementCategory;

/**
 * Owns syntax classification, audit and leader-routing policy. Classification is
 * independent of provider availability; execution still requires a registered factory.
 * Every built-in syntax kind must make an explicit classification decision.
 */
public final class StatementClassifier {
    private StatementClassifier() {}

    /** Classifies syntax independently of provider availability; unsupported kinds remain UNKNOWN. */
    public static StatementCategory categorize(Statement statement) {
        return switch (statement.type()) {
            case CREATE_CATALOG, DROP_CATALOG, CREATE_SCHEMA, CREATE_TABLE, CREATE_TABLE_AS_SELECT,
                    DROP_TABLE, ALTER_TABLE, CREATE_BRANCH, DROP_BRANCH, CREATE_TAG, DROP_TAG,
                    CREATE_VIEW, DROP_VIEW, CREATE_EMBEDDING_MODEL, ALTER_EMBEDDING_MODEL,
                    DROP_EMBEDDING_MODEL, CREATE_STORAGE_PROFILE, DROP_STORAGE_PROFILE,
                    CREATE_SEMANTIC_VIEW, PUBLISH_SEMANTIC_VIEW, CREATE_FEATURE_VIEW,
                    PUBLISH_FEATURE_VIEW, MATERIALIZE_FEATURE_VIEW, CREATE_STAGE, ALTER_STAGE,
                    DROP_STAGE, CREATE_VOLUME, ALTER_VOLUME, DROP_VOLUME, SET_SESSION,
                    GRANT_PRIVILEGE, REVOKE_PRIVILEGE -> StatementCategory.DDL;
            case INSERT, INSERT_OVERWRITE, INSERT_SELECT, DELETE, UPDATE, MERGE -> StatementCategory.DML;
            case EXPLAIN -> StatementCategory.EXPLAIN;
            case SHOW_SCHEMAS, SHOW_TABLES, SHOW_TABLE_CONTROLS, DESCRIBE_TABLE, SHOW_CATALOGS,
                    SHOW_VIEWS, SHOW_EMBEDDING_MODELS, SHOW_STORAGE_PROFILES, SHOW_BRANCHES,
                    SHOW_TAGS, SHOW_METADATA, SHOW_BACKFILL_STATUS, SHOW_SEMANTIC_VIEWS,
                    SHOW_MATERIALIZATIONS, SHOW_MATERIALIZATION_DEPENDENCIES,
                    SHOW_MATERIALIZATION_GC_HISTORY, SHOW_MATERIALIZATION_REFRESH_HISTORY,
                    SHOW_VOLUMES, DESCRIBE_VOLUME, SHOW_STAGES, DESCRIBE_STAGE, LIST_STAGE,
                    SHOW_PRIVILEGE_MUTATION -> StatementCategory.SHOW;
            case CALL, COMPACT, OPTIMIZE, VACUUM, ANALYZE, REMOVE_STAGE -> StatementCategory.MAINTENANCE;
            case CREATE_DYNAMIC_TABLE, CREATE_CONTINUOUS_QUERY, ALTER_DYNAMIC_TABLE,
                    DROP_DYNAMIC_TABLE, SHOW_DYNAMIC_TABLES, DESCRIBE_DYNAMIC_TABLE -> StatementCategory.DYNAMIC_TABLE;
            case CREATE_MATERIALIZED_VIEW, ALTER_MATERIALIZED_VIEW, DROP_MATERIALIZED_VIEW,
                    REFRESH_MATERIALIZED_VIEW, SHOW_MATERIALIZED_VIEWS,
                    DESCRIBE_MATERIALIZED_VIEW -> StatementCategory.MATERIALIZED_VIEW;
            case CREATE_INDEX, ALTER_INDEX, DROP_INDEX, SHOW_INDEXES -> StatementCategory.INDEX;
            case SET_CLUSTERING_ORDER, RECLUSTER_TABLE, SHOW_CLUSTERING_STATUS -> StatementCategory.CLUSTERING;
            case GRANT_TENANT, REVOKE_TENANT -> StatementCategory.TENANT;
            case CREATE_STREAM, ALTER_STREAM, DROP_STREAM, SHOW_STREAMS, DESCRIBE_STREAM -> StatementCategory.STREAM;
            case QUERY, SET_QUERY, CTE_QUERY, INCREMENTAL_READ -> StatementCategory.QUERY;
            // These parsed operations have no execution implementation. Never fall through to a query.
            case ALTER_CATALOG, DESCRIBE_CATALOG, BACKFILL_COLUMN, CANCEL_BACKFILL,
                    USE_BRANCH -> StatementCategory.UNKNOWN;
        };
    }

    /** Privilege changes recheck current authority inside their atomic persistence operation. */
    public static boolean isPrivilegeDdl(Statement statement) {
        return switch (statement.type()) {
            case GRANT_PRIVILEGE, REVOKE_PRIVILEGE -> true;
            default -> false;
        };
    }

    /** Namespace mutations only; file deletion authorizes each object through File access grants. */
    public static boolean isFileSpaceDdl(Statement statement) {
        return switch (statement.type()) {
            case CREATE_VOLUME, ALTER_VOLUME, DROP_VOLUME, CREATE_STAGE, ALTER_STAGE, DROP_STAGE -> true;
            default -> false;
        };
    }

    public static boolean isDdl(Statement statement) {
        return categorize(statement) == StatementCategory.DDL;
    }

    public static boolean isDml(Statement statement) {
        return categorize(statement) == StatementCategory.DML;
    }

    public static boolean isShow(Statement statement) {
        return categorize(statement) == StatementCategory.SHOW;
    }

    public static boolean isMaintenance(Statement statement) {
        return categorize(statement) == StatementCategory.MAINTENANCE;
    }

    public static boolean isDynamicTable(Statement statement) {
        return categorize(statement) == StatementCategory.DYNAMIC_TABLE;
    }

    public static boolean isMaterializedView(Statement statement) {
        return categorize(statement) == StatementCategory.MATERIALIZED_VIEW;
    }

    public static boolean isUnsupported(Statement statement) {
        return categorize(statement) == StatementCategory.UNKNOWN;
    }

    public static boolean isIndex(Statement statement) {
        return categorize(statement) == StatementCategory.INDEX;
    }

    public static boolean isClustering(Statement statement) {
        return categorize(statement) == StatementCategory.CLUSTERING;
    }

    public static boolean isTenant(Statement statement) {
        return categorize(statement) == StatementCategory.TENANT;
    }

    public static boolean isStream(Statement statement) {
        return categorize(statement) == StatementCategory.STREAM;
    }

    /** Retains the existing audit policy for metadata operation families. */
    public static boolean requiresDdlAudit(Statement statement) {
        return switch (categorize(statement)) {
            case DDL, INDEX, CLUSTERING, MATERIALIZED_VIEW, TENANT -> true;
            default -> false;
        };
    }

    /** Metadata operation families retain their established leader requirement. */
    public static boolean requiresLeaderRouting(Statement statement) {
        return switch (categorize(statement)) {
            case DDL, INDEX, CLUSTERING, DYNAMIC_TABLE, MATERIALIZED_VIEW, STREAM, TENANT -> true;
            default -> false;
        };
    }
}
