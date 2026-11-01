package org.pangrova.spi.generation.job;

/** Durable lifecycle of a generic immutable artifact build. */
public enum GenerationJobState {
    QUEUED,
    RUNNING,
    READY_TO_PUBLISH,
    PUBLISHED,
    FAILED,
    CANCELLED
}
