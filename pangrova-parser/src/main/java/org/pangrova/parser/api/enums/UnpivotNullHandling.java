package org.pangrova.parser.api.enums;

/**
 * Controls whether NULL values are included or excluded in UNPIVOT output.
 */
public enum UnpivotNullHandling {
    /** Include rows where the unpivoted value is NULL (INCLUDE NULLS). */
    INCLUDE,
    /** Exclude rows where the unpivoted value is NULL (EXCLUDE NULLS, default). */
    EXCLUDE
}
