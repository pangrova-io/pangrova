package org.pangrova.materialization.api.enums;

/**
 * Lifecycle action being authorized on a materialization object.
 */
public enum MaterializationAction {

    /** Create a new object. */
    CREATE,

    /** Trigger a refresh. */
    REFRESH,

    /** Change configuration or definition. */
    ALTER,

    /** Drop the object. */
    DROP,

    /** Inspect object metadata. */
    SHOW,

    /** Read the object as a query source. */
    SELECT
}
