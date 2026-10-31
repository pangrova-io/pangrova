package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents SHOW EMBEDDING MODELS.
 */
public record ShowEmbeddingModelsStatement() implements Statement {

    @Override
    public StatementType type() {
        return StatementType.SHOW_EMBEDDING_MODELS;
    }
}
