package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents SHOW INDEXES FROM/ON tableName.
 */
public record ShowIndexesStatement(Identifier tableName) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.SHOW_INDEXES;
    }
}
