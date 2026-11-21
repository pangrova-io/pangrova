package org.pangrova.query.api.error;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Immutable failure observed by query and stage lifecycle owners.
 * Fields preserve the source diagnostics, including absent values. A missing failure
 * is distinct from a present failure with incomplete diagnostics. This value neither
 * decides retryability nor authorizes a state transition; the owning state machine
 * retains the winning failure. Transport adapters own encoding and response policy.
 */
@Experimental
public record QueryFailure(String errorCode, String errorType, String message, String stackTrace) {
    /** Creates a failure without a source stack trace. */
    public QueryFailure(String errorCode, String errorType, String message) {
        this(errorCode, errorType, message, null);
    }
}
