package org.pangrova.parser.api.enums;

/**
 * Refresh strategy for materialized views and dynamic tables.
 */
public enum RefreshStrategy {
    ON_SCHEDULE,
    ON_COMMIT,
    INCREMENTAL,
    CONTINUOUS
}
