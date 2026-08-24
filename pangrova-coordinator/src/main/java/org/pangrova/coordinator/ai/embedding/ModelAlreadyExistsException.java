package org.pangrova.coordinator.ai.embedding;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when attempting to create an embedding model that already exists in the registry.
 */
public final class ModelAlreadyExistsException extends PangrovaException {

    public ModelAlreadyExistsException(String modelName) {
        super(PangrovaErrorCode.AI_EMBEDDING_MODEL_ALREADY_EXISTS,
                "Embedding model already exists: " + modelName);
    }
}
