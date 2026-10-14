package org.pangrova.coordinator.ai.embedding;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/** Fails closed until model dependencies and connector metadata can change atomically. */
public final class ModelDropUnavailableException extends PangrovaException {

    /** Reports a model drop blocked by generated-column dependencies. */
    public ModelDropUnavailableException(String modelName) {
        super(PangrovaErrorCode.AI_EMBEDDING_MODEL_DROP_UNAVAILABLE,
                ("Cannot drop embedding model '%s': generated-column dependencies are "
                        + "not yet tracked by a durable catalog authority, and CASCADE "
                        + "cannot atomically update connector table definitions")
                        .formatted(modelName));
    }
}
