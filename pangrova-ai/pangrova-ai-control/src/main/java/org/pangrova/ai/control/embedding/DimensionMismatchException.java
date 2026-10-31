package org.pangrova.ai.control.embedding;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when a model's output dimension does not match the VECTOR column dimension
 * at DDL time (CREATE TABLE or ALTER TABLE with auto-embedding column).
 */
public final class DimensionMismatchException extends PangrovaException {

    public DimensionMismatchException(String modelName, int modelDimension, int columnDimension) {
        super(PangrovaErrorCode.AI_EMBEDDING_DIMENSION_MISMATCH,
                "Model '%s' dimension %d does not match VECTOR column dimension %d"
                        .formatted(modelName, modelDimension, columnDimension));
    }
}
