package org.pangrova.metadata.api.enums;

/** Identity whose privileges govern view access. */
public enum ViewSecurityMode {
    /** Run with the privileges of the principal that created the view. */
    DEFINER,
    /** Run with the privileges of the principal that invokes the view. */
    INVOKER
}
