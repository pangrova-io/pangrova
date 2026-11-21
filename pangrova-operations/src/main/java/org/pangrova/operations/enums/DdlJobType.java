package org.pangrova.operations.enums;

/** Operational effect of a DDL job. */
public enum DdlJobType {
    METADATA_ONLY,
    DATA_REWRITE,
    TABLE_LOCK
}
