package org.pangrova.indexing.enums;

/** Failure class recorded in index build metrics. */
public enum IndexBuildError {
    IO,
    UNKNOWN,
    REJECTED,
    CLAIM_FAILED,
    STALE_GENERATION
}
