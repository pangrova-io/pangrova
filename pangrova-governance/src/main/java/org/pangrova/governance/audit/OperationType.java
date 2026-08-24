package org.pangrova.governance.audit;

/**
 * Categories of governance operations recorded in the audit trail.
 */
public enum OperationType {

    /** A governance policy was created or modified. */
    POLICY_CHANGE,

    /** Access was granted to a user or role. */
    ACCESS_GRANT,

    /** Access was revoked from a user or role. */
    ACCESS_REVOKE,

    /** A data quality rule was created, modified, or deleted. */
    QUALITY_RULE_CHANGE,

    /** A column or table tag was assigned or removed. */
    TAG_CHANGE,

    /** A column mask was applied or removed. */
    MASK_CHANGE,

    /** A lineage relationship was registered or modified. */
    LINEAGE_CHANGE,

    /** A scheduled quality check was executed. */
    QUALITY_CHECK_EXECUTION,

    /** A FILE column blob was accessed (read or streamed). */
    FILE_ACCESS,

    /** A multimodal AI inference was invoked on a FILE column. */
    MULTIMODAL_INFERENCE,

    /** A Storage Profile was created, modified, or deleted. */
    STORAGE_PROFILE_CHANGE
}
