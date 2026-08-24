package org.pangrova.coordinator.ai.embedding;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when attempting to drop an embedding model that is still referenced
 * by dependent auto-embedding column definitions without CASCADE.
 */
public final class ModelInUseException extends PangrovaException {

    public ModelInUseException(String modelName) {
        super(PangrovaErrorCode.AI_EMBEDDING_MODEL_IN_USE,
                "Cannot drop embedding model '%s': referenced by dependent columns. Use CASCADE to force drop."
                        .formatted(modelName));
    }
}
