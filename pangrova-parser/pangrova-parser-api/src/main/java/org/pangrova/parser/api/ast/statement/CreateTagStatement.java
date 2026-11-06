package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents CREATE TAG name ON table.
 */
public record CreateTagStatement(
        String tagName,
        Identifier tableName) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.CREATE_TAG;
    }
}
