package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents DROP EMBEDDING MODEL name [CASCADE].
 */
public record DropEmbeddingModelStatement(
        String modelName,
        boolean cascade) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DROP_EMBEDDING_MODEL;
    }
}
