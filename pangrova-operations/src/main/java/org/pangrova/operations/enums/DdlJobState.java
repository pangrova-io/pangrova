package org.pangrova.operations.enums;

/** Execution state of a DDL job. */
public enum DdlJobState {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED
}
