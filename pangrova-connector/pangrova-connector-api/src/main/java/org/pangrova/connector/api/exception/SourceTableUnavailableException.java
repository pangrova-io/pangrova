package org.pangrova.connector.api.exception;

import org.pangrova.common.api.annotation.Experimental;

/** Authoritative SQL head is absent or has a different table incarnation. This alone is not an abort proof. */
@Experimental
public final class SourceTableUnavailableException extends IllegalStateException {
    public SourceTableUnavailableException(String message) { super(message); }
}
