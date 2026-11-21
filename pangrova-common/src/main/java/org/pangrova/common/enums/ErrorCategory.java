package org.pangrova.common.enums;

/** Operational error category for retry and resilience policy. */
public enum ErrorCategory {
    USER_ERROR,
    RESOURCE_ERROR,
    INTERNAL_ERROR,
    EXTERNAL_ERROR,
    EXCHANGE_ERROR,
    TIMEOUT
}
