package org.pangrova.connector.api.exception;

import org.pangrova.common.api.annotation.Experimental;

/** A managed source authority transaction was definitively aborted and may be attempted again. */
@Experimental
public final class SourcePublicationRetryableException extends IllegalStateException {

    public SourcePublicationRetryableException(Throwable cause) {
        super("Managed source authority transaction was aborted by a concurrency conflict", cause);
    }
}
