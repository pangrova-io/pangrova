package org.pangrova.ai.control.exception;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when a user lacks the required privilege for an embedding model operation.
 * ADMIN is required for CREATE/ALTER/DROP; USAGE is required for referencing.
 */
public final class EmbeddingRegistryAccessDeniedException extends PangrovaException {

    public EmbeddingRegistryAccessDeniedException(String principal, String operation, String modelName) {
        super(PangrovaErrorCode.AI_EMBEDDING_ACCESS_DENIED,
                "User '%s' lacks privilege for %s on embedding model '%s'"
                        .formatted(principal, operation, modelName));
    }
}
