package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents DROP TAG name ON table.
 */
public record DropTagStatement(
        String tagName,
        Identifier tableName) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DROP_TAG;
    }
}
