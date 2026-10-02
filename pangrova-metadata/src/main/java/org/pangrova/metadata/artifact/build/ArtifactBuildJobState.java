package org.pangrova.metadata.artifact.build;

/** Durable lifecycle of a generic immutable artifact build. */
public enum ArtifactBuildJobState {
    QUEUED,
    RUNNING,
    READY_TO_PUBLISH,
    PUBLISHED,
    FAILED,
    CANCELLED
}
