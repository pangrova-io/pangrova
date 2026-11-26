package org.pangrova.common.enums;

/** Shared query status milestone used by callers and the execution owner. */
public enum QueryWaitTarget {
    NEXT_VERSION,
    RESULT_OR_TERMINAL,
    TERMINAL,
    DRAINED
}
