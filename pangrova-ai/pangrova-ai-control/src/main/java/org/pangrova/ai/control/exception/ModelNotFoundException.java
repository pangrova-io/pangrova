package org.pangrova.ai.control.exception;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when a referenced embedding model does not exist in the registry.
 */
public final class ModelNotFoundException extends PangrovaException {

    public ModelNotFoundException(String modelName) {
        super(PangrovaErrorCode.AI_EMBEDDING_MODEL_NOT_FOUND,
                "Embedding model not found: " + modelName);
    }
}
