package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents SHOW TAGS ON table.
 */
public record ShowTagsStatement(Identifier tableName) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.SHOW_TAGS;
    }
}
