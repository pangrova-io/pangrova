package org.pangrova.query.api.enums;

/**
 * Classifies the type of statement being executed.
 * Used by listeners to determine which side effects to trigger,
 * and by the dispatcher to decide routing and permission checks.
 *
 * Classification and execution are owned by Query Core; this value does not imply support.
 */
@org.pangrova.common.api.annotation.Internal
public enum StatementCategory {
    DDL,
    DML,
    EXPLAIN,
    SHOW,
    MAINTENANCE,
    DYNAMIC_TABLE,
    MATERIALIZED_VIEW,
    INDEX,
    CLUSTERING,
    TENANT,
    STREAM,
    QUERY,
    UNKNOWN;

}
