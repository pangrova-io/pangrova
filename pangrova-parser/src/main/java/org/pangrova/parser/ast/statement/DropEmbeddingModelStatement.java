package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Statement;

/**
 * Represents DROP EMBEDDING MODEL name [CASCADE].
 */
public record DropEmbeddingModelStatement(
        String modelName,
        boolean cascade) implements Statement {
}
