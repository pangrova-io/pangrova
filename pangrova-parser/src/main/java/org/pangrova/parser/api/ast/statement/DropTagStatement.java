package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

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
