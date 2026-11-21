package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

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
