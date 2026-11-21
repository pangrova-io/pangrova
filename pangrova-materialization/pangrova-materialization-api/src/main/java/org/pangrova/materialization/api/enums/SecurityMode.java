package org.pangrova.materialization.api.enums;

/**
 * Execution semantics of a materialized object.
 *
 * <p>Only {@link #DEFINER} is supported in the first production milestone.
 * {@link #INVOKER} is represented so that the parser and planner can reject it
 * explicitly instead of silently treating it as definer semantics.
 */
public enum SecurityMode {

    /** The object runs as the persisted definer. */
    DEFINER,

    /** The object would run as the invoking user; not supported. */
    INVOKER;

    /**
     * Returns whether the kernel can execute this mode.
     *
     * @return true for definer mode
     */
    public boolean isSupported() {
        return this == DEFINER;
    }
}
