package org.pangrova.databuild.api.build;

/** Durable lifecycle of a generic immutable artifact build. */
public enum DataBuildJobState {
    QUEUED,
    RUNNING,
    READY_TO_PUBLISH,
    PUBLISHED,
    FAILED,
    CANCELLED
}
