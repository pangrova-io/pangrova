package org.pangrova.query.profiling.enums;

/** Outcome of waiting for the build stage before enumerating a probe. */
public enum RuntimeFilterOutcome {
    APPLIED, EMPTY_BUILD, MISSING_SUMMARY, TIMED_OUT, QUERY_TERMINATED
}
