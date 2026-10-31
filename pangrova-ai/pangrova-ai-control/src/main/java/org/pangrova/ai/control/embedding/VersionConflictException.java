package org.pangrova.ai.control.embedding;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when an ALTER operation fails due to optimistic concurrency conflict.
 * The caller's expected version does not match the current stored version.
 */
public final class VersionConflictException extends PangrovaException {

    public VersionConflictException(String modelName, int expectedVersion, int actualVersion) {
        super(PangrovaErrorCode.AI_EMBEDDING_VERSION_CONFLICT,
                "Version conflict on model '%s': expected version %d but current is %d"
                        .formatted(modelName, expectedVersion, actualVersion));
    }
}
