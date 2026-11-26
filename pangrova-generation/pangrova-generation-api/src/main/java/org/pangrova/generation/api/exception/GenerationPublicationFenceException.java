package org.pangrova.generation.api.exception;

import org.pangrova.common.api.annotation.Internal;

/** Permanent publication fence loss for one exact artifact generation. */
@Internal
public final class GenerationPublicationFenceException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    public GenerationPublicationFenceException(String message) {
        super(message);
    }
}
