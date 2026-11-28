package org.pangrova.common.enums;

import org.pangrova.common.client.result.ResultEndpoint;

/** Whether a query has no client result, is waiting to publish one, or owns a result stream. */
public enum ResultDisposition {
    PENDING,
    STREAM,
    NONE,
    /** A retired execution cannot serve its original stream. Only a recovered observation uses this value. */
    UNAVAILABLE;

    /** Enforces the endpoint invariant shared by lifecycle, submission and wire observations. */
    public void validateEndpoint(ResultEndpoint endpoint) {
        if (this == STREAM && endpoint == null) {
            throw new IllegalArgumentException("STREAM requires a result endpoint");
        }
        if (this != STREAM && endpoint != null) {
            throw new IllegalArgumentException(name() + " must not carry a result endpoint");
        }
    }
}
