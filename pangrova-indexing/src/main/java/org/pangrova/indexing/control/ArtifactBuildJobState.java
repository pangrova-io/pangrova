package org.pangrova.indexing.control;

/** Durable lifecycle of a generic immutable artifact build. */
public enum ArtifactBuildJobState {
    QUEUED,
    RUNNING,
    READY_TO_PUBLISH,
    PUBLISHED,
    FAILED,
    CANCELLED
}
