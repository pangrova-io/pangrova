package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents SHOW EMBEDDING MODELS.
 */
public record ShowEmbeddingModelsStatement() implements Statement {

    @Override
    public StatementType type() {
        return StatementType.SHOW_EMBEDDING_MODELS;
    }
}
