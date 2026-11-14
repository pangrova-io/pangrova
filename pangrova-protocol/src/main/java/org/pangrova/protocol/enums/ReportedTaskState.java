package org.pangrova.protocol.enums;

/**
 * Task states reported over the execution protocol.
 * Worker execution maps its internal lifecycle to these states before reporting it.
 */
public enum ReportedTaskState {
    PENDING, RUNNING, FLUSHING, FINISHED, FAILED, CANCELLED
}
