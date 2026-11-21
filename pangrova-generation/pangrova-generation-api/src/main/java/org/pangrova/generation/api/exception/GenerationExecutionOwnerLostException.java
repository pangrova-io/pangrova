package org.pangrova.generation.api.exception;

import org.pangrova.common.api.annotation.Internal;

/** Stale Artifact execution ownership; storage failure and corrupt routing are different failures. */
@Internal
public final class GenerationExecutionOwnerLostException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    public GenerationExecutionOwnerLostException(String message, Throwable cause) {
        super(message, cause);
    }
}
