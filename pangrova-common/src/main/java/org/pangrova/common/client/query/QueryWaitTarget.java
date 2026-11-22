package org.pangrova.common.client.query;

/** Owner status event a client needs before continuing. */
public enum QueryWaitTarget {
    NEXT_VERSION,
    RESULT_OR_TERMINAL,
    TERMINAL,
    DRAINED
}
