package org.pangrova.planner.validation;

/**
 * Result status of Substrait plan validation against native engine capabilities.
 */
public enum ValidationStatus {

    /** All operators in the plan are supported by the native engine. */
    FULLY_NATIVE,

    /** Some operators are unsupported; the unsupported node list is available. */
    PARTIAL_NATIVE,

    /** The plan cannot be offloaded to the native engine at all. */
    JAVA_ONLY
}
