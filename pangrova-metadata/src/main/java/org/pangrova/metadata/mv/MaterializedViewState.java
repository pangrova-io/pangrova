package org.pangrova.metadata.mv;

/**
 * Lifecycle state of a materialized view.
 */
public enum MaterializedViewState {
    CREATED,
    REFRESHING,
    ACTIVE,
    DROPPED
}
