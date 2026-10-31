package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

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
