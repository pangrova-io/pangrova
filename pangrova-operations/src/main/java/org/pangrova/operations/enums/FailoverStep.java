package org.pangrova.operations.enums;

/** Step in the standby failover workflow. */
public enum FailoverStep {
    VERIFY_STANDBY,
    CHECK_LAG,
    STOP_WRITES,
    WAIT_CATCHUP,
    PROMOTE,
    UPDATE_DNS,
    NOTIFY
}
