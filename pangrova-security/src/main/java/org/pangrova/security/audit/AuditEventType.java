package org.pangrova.security.audit;

/**
 * Categories of auditable events across authentication, query, DDL, policy, and tenant operations.
 */
public enum AuditEventType {
    // Authentication
    LOGIN_SUCCESS,
    LOGIN_FAILURE,
    LOGOUT,

    // Query lifecycle
    QUERY_SUBMITTED,
    QUERY_COMPLETED,
    QUERY_FAILED,
    QUERY_CANCELLED,

    // Access control
    ACCESS_GRANTED,
    ACCESS_DENIED,
    TABLE_ACCESSED,
    TABLE_WRITE,
    TABLE_DELETE,

    // DDL - Table
    TABLE_CREATED,
    TABLE_DROPPED,
    TABLE_ALTERED,
    TABLE_ADOPTED,
    TABLE_UNLINKED,
    TABLE_QUARANTINED,

    // Table control plane
    TABLE_MAINTENANCE,
    EXTERNAL_COMMIT_OBSERVED,

    // DDL - Catalog
    CATALOG_CREATED,
    CATALOG_DROPPED,

    // DDL - Schema
    SCHEMA_CREATED,
    SCHEMA_DROPPED,

    // DDL - Volume / Stage
    VOLUME_CREATED,
    VOLUME_ALTERED,
    VOLUME_DROPPED,
    STAGE_CREATED,
    STAGE_ALTERED,
    STAGE_DROPPED,

    // File object operations
    FILE_UPLOADED,
    FILE_DOWNLOADED,
    FILE_DELETED,
    FILE_LISTED,
    FILE_URL_ISSUED,

    // RLS policy
    RLS_POLICY_CREATED,
    RLS_POLICY_UPDATED,
    RLS_POLICY_DELETED,

    // Column mask
    COLUMN_MASK_CREATED,
    COLUMN_MASK_UPDATED,
    COLUMN_MASK_DELETED,

    // Role management
    ROLE_GRANTED,
    ROLE_REVOKED,

    // Tenant management
    TENANT_CREATED,
    TENANT_UPDATED,
    TENANT_DELETED,

    // System
    CONFIG_CHANGED,
    SECRET_ACCESSED
}
