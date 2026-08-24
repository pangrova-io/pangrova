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

    // DDL - Catalog
    CATALOG_CREATED,
    CATALOG_DROPPED,

    // DDL - Schema
    SCHEMA_CREATED,
    SCHEMA_DROPPED,

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
